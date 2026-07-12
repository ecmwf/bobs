// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Linux io_uring ring-pool support.
//!
//! This module starts with hash-routing tests so the stable key-to-shard
//! contract is pinned before the dispatch implementation is introduced.

use super::MAX_IO_URING_SHARDS;
use crate::error::BobsError;
use bytes::Bytes;
use io_uring::{opcode, squeue, types, IoUring};
use siphasher::sip::SipHasher13;
#[cfg(test)]
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ffi::CString;
use std::hash::Hasher;
use std::io::{Error, ErrorKind, Result};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Condvar;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use tokio::sync::{mpsc, oneshot};

const RING_ENTRIES: u32 = 256;
const AT_FDCWD: RawFd = -100;
const METADATA_COMMIT_CHAIN_LEN: usize = 4;
const METADATA_COMMIT_PHASE_LEN: usize = 2;
const MAX_SQES_PER_REQUEST: usize = METADATA_COMMIT_PHASE_LEN;
const METADATA_USER_DATA_SHIFT: u64 = 56;
const METADATA_USER_DATA_MASK: u64 = (1u64 << METADATA_USER_DATA_SHIFT) - 1;

type OpenSender = oneshot::Sender<Result<Arc<OwnedFd>>>;
type WriteSender = oneshot::Sender<Result<usize>>;
type ReadSender = oneshot::Sender<Result<Bytes>>;
type UnitSender = oneshot::Sender<Result<()>>;

pub(crate) enum Request {
    Open {
        path: CString,
        flags: i32,
        mode: u32,
        tx: OpenSender,
    },
    Write {
        fd: Arc<OwnedFd>,
        offset: u64,
        data: Bytes,
        tx: WriteSender,
    },
    Read {
        fd: Arc<OwnedFd>,
        offset: u64,
        len: usize,
        tx: ReadSender,
    },
    SyncData {
        fd: Arc<OwnedFd>,
        tx: UnitSender,
    },
    SyncDirectory {
        fd: Arc<OwnedFd>,
        tx: UnitSender,
    },
    Remove {
        path: CString,
        tx: UnitSender,
    },
    MetadataCommit {
        key: String,
        tmp_fd: OwnedFd,
        parent_fd: OwnedFd,
        tmp_name: CString,
        final_name: CString,
        payload: Bytes,
        tx: UnitSender,
    },
}

impl Request {
    fn reserved_sqe_work(&self) -> usize {
        match self {
            Self::MetadataCommit { .. } => METADATA_COMMIT_PHASE_LEN,
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingPoolOptions {
    pub shard_count: usize,
    pub queue_capacity: usize,
    pub driver_name_prefix: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RingPoolOperationKind {
    DataCreate,
    DataOpen,
    DataWrite,
    DataRead,
    DataSync,
    MetadataCommit,
    DirectorySync,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RingPoolRoutingEvent {
    pub operation_kind: RingPoolOperationKind,
    pub routed_key: String,
    pub ring_index: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RingPoolSubmissionEvent {
    pub shard_index: usize,
    pub driver_name: String,
    pub pushing_thread_name: Option<String>,
    pub push_sequence: u64,
    pub user_data: u64,
    pub operation_id: u64,
    pub chain_index: u16,
    pub chain_len: u16,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RingPoolSafetyEvent {
    SubmitFailed,
    RingDropped,
    InFlightReleased,
    ErrorReported,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
struct RingPoolSubmissionInstrumentation {
    events: Arc<Mutex<Vec<RingPoolSubmissionEvent>>>,
    sequence: Arc<AtomicU64>,
    submit_failures_remaining: Arc<AtomicUsize>,
    safety_events: Arc<Mutex<Vec<RingPoolSafetyEvent>>>,
    submission_pause: Arc<(Mutex<bool>, Condvar)>,
    receive_pause: Arc<(Mutex<bool>, Condvar)>,
    successful_submissions: Arc<AtomicUsize>,
    max_driver_sqe_work: Arc<AtomicUsize>,
}

#[cfg(test)]
struct RingPoolDriverPause {
    gate: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
impl Drop for RingPoolDriverPause {
    fn drop(&mut self) {
        let (paused, wake) = &*self.gate;
        *paused.lock().expect("ring-pool driver pause poisoned") = false;
        wake.notify_all();
    }
}

#[cfg(not(test))]
#[derive(Debug, Clone, Default)]
struct RingPoolSubmissionInstrumentation;

#[derive(Debug, Default)]
pub struct RingPoolInstrumentation {
    in_flight_operations: AtomicUsize,
    submission: RingPoolSubmissionInstrumentation,
    #[cfg(test)]
    routing_events: Mutex<Vec<RingPoolRoutingEvent>>,
}

#[derive(Debug, Default)]
pub struct RingShardCounters {
    in_flight_operations: Arc<AtomicUsize>,
    driver_stopped: Arc<AtomicBool>,
}

impl RingShardCounters {
    fn in_flight_operations(&self) -> usize {
        self.in_flight_operations.load(Ordering::SeqCst)
    }

    fn driver_stopped(&self) -> bool {
        self.driver_stopped.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct RingShard {
    #[allow(dead_code)]
    index: usize,
    driver_name: String,
    sender: Option<mpsc::Sender<Request>>,
    driver: Option<JoinHandle<()>>,
    counters: RingShardCounters,
}

impl Drop for RingShard {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(driver) = self.driver.take() {
            if let Err(panic) = driver.join() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

pub struct RingPool {
    shards: Vec<RingShard>,
    config: RingPoolOptions,
    instrumentation: RingPoolInstrumentation,
}

impl std::fmt::Debug for RingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingPool")
            .field("shards", &self.shards)
            .field("config", &self.config)
            .field("instrumentation", &self.instrumentation)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingPoolShutdown {
    pub joined_driver_handles: usize,
    pub in_flight_operations_remaining: usize,
    pub driver_threads_all_stopped: bool,
}

impl RingPoolOptions {
    pub fn production(configured_shards: Option<usize>, queue_capacity: usize) -> Result<Self> {
        validate_queue_capacity(queue_capacity, RING_ENTRIES as usize)?;
        Ok(Self {
            shard_count: resolve_shard_count(configured_shards)?,
            queue_capacity,
            driver_name_prefix: "bobs-io-uring-shard".to_owned(),
        })
    }
}

impl RingPool {
    pub fn new(configured_shards: Option<usize>) -> Result<Self> {
        Self::from_options(RingPoolOptions::production(configured_shards, 1024)?)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(options: RingPoolOptions) -> Result<Self> {
        Self::from_options(options)
    }

    fn from_options(options: RingPoolOptions) -> Result<Self> {
        if options.shard_count == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "ring pool shard_count must be greater than 0",
            ));
        }
        // RingPoolOptions is public and test/custom callers can bypass
        // RingPoolOptions::production, so validate again at construction.
        validate_queue_capacity(options.queue_capacity, RING_ENTRIES as usize)?;

        let instrumentation = RingPoolInstrumentation::default();
        let shards = Self::start_shards(&options, instrumentation.submission.clone())?;
        Ok(Self {
            shards,
            config: options,
            instrumentation,
        })
    }

    fn start_shards(
        options: &RingPoolOptions,
        submission_instrumentation: RingPoolSubmissionInstrumentation,
    ) -> Result<Vec<RingShard>> {
        validate_shard_count(options.shard_count)?;

        let mut shards = Vec::new();
        shards
            .try_reserve_exact(options.shard_count)
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "could not reserve storage for {} ring-pool shards: {error}",
                        options.shard_count
                    ),
                )
            })?;
        for index in 0..options.shard_count {
            let ring = IoUring::new(RING_ENTRIES)?;

            shards.push(start_shard(
                index,
                &options.driver_name_prefix,
                options.queue_capacity,
                ring,
                submission_instrumentation.clone(),
            )?);
        }
        Ok(shards)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn driver_names(&self) -> Vec<String> {
        self.shards
            .iter()
            .map(|shard| shard.driver_name.clone())
            .collect()
    }

    pub fn driver_join_handle_count(&self) -> usize {
        self.shards
            .iter()
            .filter(|shard| shard.driver.is_some())
            .count()
    }

    fn options(&self) -> &RingPoolOptions {
        &self.config
    }

    pub fn in_flight_operations(&self) -> usize {
        self.instrumentation
            .in_flight_operations
            .load(Ordering::SeqCst)
            + self
                .shards
                .iter()
                .map(|shard| shard.counters.in_flight_operations())
                .sum::<usize>()
    }

    pub(crate) async fn submit_metadata_commit(
        &self,
        key: String,
        tmp_fd: OwnedFd,
        parent_fd: OwnedFd,
        tmp_name: CString,
        final_name: CString,
        payload: Bytes,
    ) -> Result<()> {
        let ring_index = ring_index_for_key(&key, self.shard_count());
        #[cfg(test)]
        self.record_routing(
            RingPoolOperationKind::MetadataCommit,
            key.clone(),
            ring_index,
        );

        let (tx, rx) = oneshot::channel();
        self.submit_to_ring(
            ring_index,
            Request::MetadataCommit {
                key,
                tmp_fd,
                parent_fd,
                tmp_name,
                final_name,
                payload,
                tx,
            },
        )
        .await?;
        rx.await.map_err(|_| {
            Error::new(
                ErrorKind::BrokenPipe,
                "io_uring metadata commit driver dropped request",
            )
        })?
    }

    pub(crate) async fn submit_to_ring(&self, ring_index: usize, request: Request) -> Result<()> {
        let shard = self.shards.get(ring_index).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "ring index {ring_index} out of range for {} shards",
                    self.shard_count()
                ),
            )
        })?;
        shard
            .sender
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    "io_uring shard driver stopped before accepting request",
                )
            })?
            .send(request)
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    "io_uring shard driver stopped before accepting request",
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn metadata_commit_routing_event_for_key_for_test(
        key: &str,
        shard_count: usize,
    ) -> RingPoolRoutingEvent {
        RingPoolRoutingEvent {
            operation_kind: RingPoolOperationKind::MetadataCommit,
            routed_key: key.to_owned(),
            ring_index: ring_index_for_key(key, shard_count),
        }
    }

    #[cfg(test)]
    pub(crate) fn record_routing(
        &self,
        operation_kind: RingPoolOperationKind,
        routed_key: impl Into<String>,
        ring_index: usize,
    ) {
        self.instrumentation
            .routing_events
            .lock()
            .expect("ring-pool routing instrumentation poisoned")
            .push(RingPoolRoutingEvent {
                operation_kind,
                routed_key: routed_key.into(),
                ring_index,
            });
    }

    #[cfg(test)]
    pub(crate) fn routing_events(&self) -> Vec<RingPoolRoutingEvent> {
        self.instrumentation
            .routing_events
            .lock()
            .expect("ring-pool routing instrumentation poisoned")
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn clear_routing_events(&self) {
        self.instrumentation
            .routing_events
            .lock()
            .expect("ring-pool routing instrumentation poisoned")
            .clear();
    }

    #[cfg(test)]
    pub(crate) fn submission_events(&self) -> Vec<RingPoolSubmissionEvent> {
        self.instrumentation
            .submission
            .events
            .lock()
            .expect("ring-pool submission instrumentation poisoned")
            .clone()
    }

    #[cfg(test)]
    fn pause_submissions_for_test(&self) -> RingPoolDriverPause {
        Self::pause_driver_gate(&self.instrumentation.submission.submission_pause)
    }

    #[cfg(test)]
    fn pause_receives_for_test(&self) -> RingPoolDriverPause {
        Self::pause_driver_gate(&self.instrumentation.submission.receive_pause)
    }

    #[cfg(test)]
    fn pause_driver_gate(gate: &Arc<(Mutex<bool>, Condvar)>) -> RingPoolDriverPause {
        let gate = Arc::clone(gate);
        let (paused, _) = &*gate;
        *paused.lock().expect("ring-pool driver pause poisoned") = true;
        RingPoolDriverPause { gate }
    }

    #[cfg(test)]
    fn successful_submissions_for_test(&self) -> usize {
        self.instrumentation
            .submission
            .successful_submissions
            .load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn max_driver_sqe_work_for_test(&self) -> usize {
        self.instrumentation
            .submission
            .max_driver_sqe_work
            .load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn inject_submit_failure_for_test(&self) {
        self.instrumentation
            .submission
            .submit_failures_remaining
            .fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn safety_events(&self) -> Vec<RingPoolSafetyEvent> {
        self.instrumentation
            .submission
            .safety_events
            .lock()
            .expect("ring-pool safety instrumentation poisoned")
            .clone()
    }

    pub fn shutdown(mut self) -> Result<RingPoolShutdown> {
        Ok(self.shutdown_inner())
    }

    fn shutdown_inner(&mut self) -> RingPoolShutdown {
        for shard in &mut self.shards {
            shard.sender.take();
        }

        let mut joined_driver_handles = 0;
        for shard in &mut self.shards {
            if let Some(driver) = shard.driver.take() {
                if let Err(panic) = driver.join() {
                    std::panic::resume_unwind(panic);
                }
                joined_driver_handles += 1;
            }
        }

        let in_flight_operations_remaining = self.in_flight_operations();
        let driver_threads_all_stopped = self
            .shards
            .iter()
            .all(|shard| shard.counters.driver_stopped());

        RingPoolShutdown {
            joined_driver_handles,
            in_flight_operations_remaining,
            driver_threads_all_stopped,
        }
    }
}

impl Drop for RingPool {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

fn start_shard(
    index: usize,
    prefix: &str,
    queue_capacity: usize,
    mut ring: IoUring,
    submission_instrumentation: RingPoolSubmissionInstrumentation,
) -> Result<RingShard> {
    let driver_sqe_capacity = ring.submission().capacity();
    validate_queue_capacity(queue_capacity, driver_sqe_capacity)?;
    let (sender, receiver) = mpsc::channel(queue_capacity);
    let driver_name = format!("{prefix}-{index}");
    let counters = RingShardCounters::default();
    let driver_stopped = Arc::clone(&counters.driver_stopped);
    let in_flight_operations = Arc::clone(&counters.in_flight_operations);
    let thread_name = driver_name.clone();
    let driver_thread_name = driver_name.clone();
    let driver = thread::Builder::new().name(thread_name).spawn(move || {
        run_driver(
            index,
            driver_thread_name,
            ring,
            receiver,
            in_flight_operations,
            driver_stopped,
            submission_instrumentation,
        );
    })?;

    Ok(RingShard {
        index,
        driver_name,
        sender: Some(sender),
        driver: Some(driver),
        counters,
    })
}

fn run_driver(
    shard_index: usize,
    driver_name: String,
    mut ring: IoUring,
    receiver: mpsc::Receiver<Request>,
    in_flight_operations: Arc<AtomicUsize>,
    driver_stopped: Arc<AtomicBool>,
    submission_instrumentation: RingPoolSubmissionInstrumentation,
) {
    let driver_sqe_capacity = ring.submission().capacity();
    RingDriver::new(
        shard_index,
        driver_name,
        ring,
        receiver,
        driver_sqe_capacity,
        Arc::clone(&in_flight_operations),
        submission_instrumentation,
    )
    .run();
    debug_assert_eq!(in_flight_operations.load(Ordering::SeqCst), 0);
    driver_stopped.store(true, Ordering::SeqCst);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataCommitPhase {
    WriteAndSync,
    RenameAndSync,
}

impl MetadataCommitPhase {
    fn first_chain_index(self) -> usize {
        match self {
            Self::WriteAndSync => 0,
            Self::RenameAndSync => METADATA_COMMIT_PHASE_LEN,
        }
    }
}

enum InFlightKind {
    Open {
        path: CString,
        flags: i32,
        mode: u32,
        tx: Option<OpenSender>,
    },
    Write {
        fd: Arc<OwnedFd>,
        offset: u64,
        data: Bytes,
        written: usize,
        tx: Option<WriteSender>,
    },
    Read {
        fd: Arc<OwnedFd>,
        offset: u64,
        len: usize,
        buf: Vec<u8>,
        tx: Option<ReadSender>,
    },
    SyncData {
        fd: Arc<OwnedFd>,
        tx: Option<UnitSender>,
    },
    SyncDirectory {
        fd: Arc<OwnedFd>,
        tx: Option<UnitSender>,
    },
    Remove {
        path: CString,
        tx: Option<UnitSender>,
    },
    MetadataCommit {
        key: String,
        tmp_fd: OwnedFd,
        parent_fd: OwnedFd,
        tmp_name: CString,
        final_name: CString,
        payload: Bytes,
        phase: MetadataCommitPhase,
        completed: [bool; METADATA_COMMIT_PHASE_LEN],
        failure: Option<Error>,
        tx: Option<UnitSender>,
    },
}

struct InFlight {
    kind: InFlightKind,
}

impl InFlight {
    fn reserved_sqe_work(&self) -> usize {
        match self.kind {
            InFlightKind::MetadataCommit { .. } => METADATA_COMMIT_PHASE_LEN,
            _ => 1,
        }
    }
}

struct RingDriver {
    #[allow(dead_code)]
    shard_index: usize,
    #[allow(dead_code)]
    driver_name: String,
    ring: Option<IoUring>,
    rx: mpsc::Receiver<Request>,
    in_flight: HashMap<u64, InFlight>,
    pending: VecDeque<Request>,
    pending_sqe_work: usize,
    in_flight_sqe_work: usize,
    driver_sqe_capacity: usize,
    next_id: u64,
    in_flight_operations: Arc<AtomicUsize>,
    #[allow(dead_code)]
    submission_instrumentation: RingPoolSubmissionInstrumentation,
}

impl RingDriver {
    fn new(
        shard_index: usize,
        driver_name: String,
        ring: IoUring,
        rx: mpsc::Receiver<Request>,
        driver_sqe_capacity: usize,
        in_flight_operations: Arc<AtomicUsize>,
        submission_instrumentation: RingPoolSubmissionInstrumentation,
    ) -> Self {
        Self {
            shard_index,
            driver_name,
            ring: Some(ring),
            rx,
            in_flight: HashMap::new(),
            pending: VecDeque::new(),
            pending_sqe_work: 0,
            in_flight_sqe_work: 0,
            driver_sqe_capacity,
            next_id: 1,
            in_flight_operations,
            submission_instrumentation,
        }
    }

    fn run(mut self) {
        let mut receiver_open = true;

        while receiver_open || !self.pending.is_empty() || !self.in_flight.is_empty() {
            if receiver_open && self.pending.is_empty() && self.in_flight.is_empty() {
                match self.rx.blocking_recv() {
                    Some(req) => {
                        #[cfg(test)]
                        self.wait_for_test_gate(&self.submission_instrumentation.receive_pause);
                        self.queue_pending(req);
                    }
                    None => {
                        receiver_open = false;
                        continue;
                    }
                }
            }

            if receiver_open {
                receiver_open = self.drain_available_requests();
            }

            let (pushed, stalled_on_full_submission_queue) = self.submit_pending_batch();
            if pushed > 0 {
                if let Err(error) = self.submit_ring(false) {
                    self.abort_after_submit_failure(error);
                    return;
                }
            }

            if let Err(error) = self.process_completions() {
                self.abort_after_submit_failure(error);
                return;
            }

            if receiver_open {
                receiver_open = self.drain_available_requests();
            }

            if (self.pending.is_empty() || stalled_on_full_submission_queue)
                && !self.in_flight.is_empty()
            {
                if let Err(error) = self.submit_ring(true) {
                    self.abort_after_submit_failure(error);
                    return;
                }
                if let Err(error) = self.process_completions() {
                    self.abort_after_submit_failure(error);
                    return;
                }
            }
        }
    }

    #[cfg(test)]
    fn wait_for_test_gate(&self, gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (paused, wake) = &**gate;
        let mut paused = paused.lock().expect("ring-pool driver pause poisoned");
        while *paused {
            paused = wake.wait(paused).expect("ring-pool driver pause poisoned");
        }
    }

    /// Move work out of the bounded channel only while the driver has room for
    /// the largest request. `pending_sqe_work + in_flight_sqe_work` therefore
    /// never exceeds the ring's SQE capacity. At most one SQE is deliberately
    /// left unused when the next channel request is unknown; this keeps metadata
    /// pairs atomic without reducing batching for ordinary one-SQE requests.
    ///
    /// The channel separately owns `queue_capacity` permits. Consequently the
    /// strict per-shard admitted-work bound is ring capacity plus
    /// `queue_capacity * MAX_SQES_PER_REQUEST`; capacity arithmetic is checked
    /// before the channel is created.
    fn drain_available_requests(&mut self) -> bool {
        while self.available_driver_sqe_work() >= MAX_SQES_PER_REQUEST {
            match self.rx.try_recv() {
                Ok(req) => self.queue_pending(req),
                Err(mpsc::error::TryRecvError::Empty) => return true,
                Err(mpsc::error::TryRecvError::Disconnected) => return false,
            }
        }
        true
    }

    /// Push one throughput batch, bounded by the ring's SQE capacity. A batch
    /// may contain up to 255 ordinary requests or 128 two-SQE metadata phases.
    fn submit_pending_batch(&mut self) -> (usize, bool) {
        let mut pushed = 0;
        let mut batch_sqe_work = 0usize;
        let mut stalled_on_full_submission_queue = false;
        while let Some(req) = self.pending.pop_front() {
            let request_sqe_work = req.reserved_sqe_work();
            self.pending_sqe_work -= request_sqe_work;
            if batch_sqe_work
                .checked_add(request_sqe_work)
                .is_none_or(|work| work > self.driver_sqe_capacity)
            {
                self.queue_pending_front(req);
                stalled_on_full_submission_queue = true;
                break;
            }
            if let Err(req) = self.submit_request(req) {
                self.queue_pending_front(req);
                stalled_on_full_submission_queue = true;
                break;
            }
            pushed += 1;
            batch_sqe_work += request_sqe_work;
        }
        (pushed, stalled_on_full_submission_queue)
    }

    fn available_driver_sqe_work(&self) -> usize {
        self.driver_sqe_capacity
            .checked_sub(self.pending_sqe_work + self.in_flight_sqe_work)
            .expect("ring driver SQE work exceeded its configured capacity")
    }

    fn queue_pending(&mut self, req: Request) {
        let request_sqe_work = req.reserved_sqe_work();
        debug_assert!(request_sqe_work <= self.available_driver_sqe_work());
        self.pending_sqe_work += request_sqe_work;
        self.pending.push_back(req);
        self.observe_driver_sqe_work();
    }

    fn queue_pending_front(&mut self, req: Request) {
        self.pending_sqe_work += req.reserved_sqe_work();
        self.pending.push_front(req);
        self.observe_driver_sqe_work();
    }

    fn observe_driver_sqe_work(&self) {
        let work = self.pending_sqe_work + self.in_flight_sqe_work;
        debug_assert!(work <= self.driver_sqe_capacity);
        #[cfg(test)]
        self.submission_instrumentation
            .max_driver_sqe_work
            .fetch_max(work, Ordering::SeqCst);
    }

    fn submit_request(&mut self, req: Request) -> std::result::Result<(), Request> {
        let id = self.alloc_id();
        let in_flight = match req {
            Request::Open {
                path,
                flags,
                mode,
                tx,
            } => InFlight {
                kind: InFlightKind::Open {
                    path,
                    flags,
                    mode,
                    tx: Some(tx),
                },
            },
            Request::Write {
                fd,
                offset,
                data,
                tx,
            } => InFlight {
                kind: InFlightKind::Write {
                    fd,
                    offset,
                    data,
                    written: 0,
                    tx: Some(tx),
                },
            },
            Request::Read {
                fd,
                offset,
                len,
                tx,
            } => InFlight {
                kind: InFlightKind::Read {
                    fd,
                    offset,
                    len,
                    // The kernel initializes at most `len` bytes before the CQE is
                    // published. Length stays zero until that CQE is observed, so Rust
                    // never exposes uninitialized memory and large reads avoid a memset.
                    buf: allocate_read_buffer(len),
                    tx: Some(tx),
                },
            },
            Request::SyncData { fd, tx } => InFlight {
                kind: InFlightKind::SyncData { fd, tx: Some(tx) },
            },
            Request::SyncDirectory { fd, tx } => InFlight {
                kind: InFlightKind::SyncDirectory { fd, tx: Some(tx) },
            },
            Request::Remove { path, tx } => InFlight {
                kind: InFlightKind::Remove { path, tx: Some(tx) },
            },
            Request::MetadataCommit {
                key,
                tmp_fd,
                parent_fd,
                tmp_name,
                final_name,
                payload,
                tx,
            } => InFlight {
                kind: InFlightKind::MetadataCommit {
                    key,
                    tmp_fd,
                    parent_fd,
                    tmp_name,
                    final_name,
                    payload,
                    phase: MetadataCommitPhase::WriteAndSync,
                    completed: [false; METADATA_COMMIT_PHASE_LEN],
                    failure: None,
                    tx: Some(tx),
                },
            },
        };

        let is_metadata_commit = matches!(in_flight.kind, InFlightKind::MetadataCommit { .. });
        self.in_flight.insert(id, in_flight);
        self.in_flight_operations.fetch_add(1, Ordering::SeqCst);
        self.in_flight_sqe_work += self
            .in_flight
            .get(&id)
            .expect("inserted request missing")
            .reserved_sqe_work();
        self.observe_driver_sqe_work();
        let push_result = if is_metadata_commit {
            self.push_metadata_commit_phase(id)
        } else {
            self.push_entry(id)
        };
        if push_result.is_err() {
            let in_flight = self
                .in_flight
                .remove(&id)
                .expect("inserted request missing");
            self.in_flight_operations.fetch_sub(1, Ordering::SeqCst);
            self.in_flight_sqe_work -= in_flight.reserved_sqe_work();
            return Err(in_flight.into_request());
        }
        Ok(())
    }

    fn resubmit_existing(&mut self, id: u64) -> Result<()> {
        loop {
            if self.push_entry(id).is_ok() {
                self.submit_ring(false)?;
                return Ok(());
            }
            self.submit_ring(true)?;
            self.process_completions()?;
            if !self.in_flight.contains_key(&id) {
                return Ok(());
            }
        }
    }

    fn resubmit_metadata_existing(&mut self, id: u64) -> Result<()> {
        loop {
            if self.push_metadata_commit_phase(id).is_ok() {
                self.submit_ring(false)?;
                return Ok(());
            }
            self.submit_ring(true)?;
            self.process_completions()?;
            if !self.in_flight.contains_key(&id) {
                return Ok(());
            }
        }
    }

    fn push_entry(&mut self, id: u64) -> std::result::Result<(), ()> {
        let entry = build_entry(
            id,
            self.in_flight.get_mut(&id).expect("in-flight id missing"),
        );
        let ring = self
            .ring
            .as_mut()
            .expect("ring missing before driver abort");
        unsafe { ring.submission().push(&entry).map_err(|_| ())? };
        self.record_submission_push(id, id, 0, 1);
        Ok(())
    }

    fn push_metadata_commit_phase(&mut self, id: u64) -> std::result::Result<(), ()> {
        {
            let sq = self
                .ring
                .as_mut()
                .expect("ring missing before driver abort")
                .submission();
            if sq.capacity().saturating_sub(sq.len()) < METADATA_COMMIT_PHASE_LEN {
                return Err(());
            }
        }

        let (entries, first_chain_index) = build_metadata_commit_entries(
            id,
            self.in_flight.get(&id).expect("in-flight id missing"),
        );
        let mut sq = self
            .ring
            .as_mut()
            .expect("ring missing before driver abort")
            .submission();
        for entry in &entries {
            unsafe { sq.push(entry).map_err(|_| ())? };
        }
        drop(sq);

        for phase_index in 0..METADATA_COMMIT_PHASE_LEN {
            let chain_index = first_chain_index + phase_index;
            self.record_submission_push(
                metadata_user_data(id, chain_index),
                id,
                chain_index as u16,
                METADATA_COMMIT_CHAIN_LEN as u16,
            );
        }
        Ok(())
    }

    fn record_submission_push(
        &self,
        user_data: u64,
        operation_id: u64,
        chain_index: u16,
        chain_len: u16,
    ) {
        #[cfg(test)]
        {
            let push_sequence = self
                .submission_instrumentation
                .sequence
                .fetch_add(1, Ordering::SeqCst);
            self.submission_instrumentation
                .events
                .lock()
                .expect("ring-pool submission instrumentation poisoned")
                .push(RingPoolSubmissionEvent {
                    shard_index: self.shard_index,
                    driver_name: self.driver_name.clone(),
                    pushing_thread_name: thread::current().name().map(str::to_owned),
                    push_sequence,
                    user_data,
                    operation_id,
                    chain_index,
                    chain_len,
                });
        }

        #[cfg(not(test))]
        {
            let _ = (user_data, operation_id, chain_index, chain_len);
        }
    }

    fn process_completions(&mut self) -> Result<()> {
        let completions: Vec<_> = self
            .ring
            .as_mut()
            .expect("ring missing before driver abort")
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();
        for (id, res) in completions {
            self.handle_completion(id, res)?;
        }
        Ok(())
    }

    fn handle_completion(&mut self, user_data: u64, res: i32) -> Result<()> {
        if let Some((id, chain_index)) = decode_metadata_user_data(user_data) {
            return self.handle_metadata_commit_completion(id, chain_index, res);
        }

        let id = user_data;
        if res < 0 {
            self.complete_error(id, Error::from_raw_os_error(-res));
            return Ok(());
        }

        let mut in_flight = match self.in_flight.remove(&id) {
            Some(in_flight) => in_flight,
            None => return Ok(()),
        };
        self.in_flight_operations.fetch_sub(1, Ordering::SeqCst);
        self.in_flight_sqe_work -= in_flight.reserved_sqe_work();

        match &mut in_flight.kind {
            InFlightKind::Open { tx, .. } => {
                let fd = unsafe { OwnedFd::from_raw_fd(res) };
                send_open(tx, Ok(Arc::new(fd)));
            }
            InFlightKind::Write {
                data, written, tx, ..
            } => {
                let n = res as usize;
                if n == 0 && *written < data.len() {
                    send_write(
                        tx,
                        Err(Error::new(
                            ErrorKind::WriteZero,
                            format!("short write: wrote {} of {} bytes", *written, data.len()),
                        )),
                    );
                } else {
                    *written += n;
                    if *written < data.len() {
                        self.in_flight.insert(id, in_flight);
                        self.in_flight_operations.fetch_add(1, Ordering::SeqCst);
                        self.in_flight_sqe_work += self
                            .in_flight
                            .get(&id)
                            .expect("reinserted write request missing")
                            .reserved_sqe_work();
                        self.observe_driver_sqe_work();
                        self.resubmit_existing(id)?;
                    } else {
                        send_write(tx, Ok(*written));
                    }
                }
            }
            InFlightKind::Read { len, buf, tx, .. } => {
                let n = res as usize;
                if n > *len || n > buf.capacity() {
                    send_read(
                        tx,
                        Err(Error::new(
                            ErrorKind::InvalidData,
                            "io_uring read completion exceeded requested buffer length",
                        )),
                    );
                } else {
                    // SAFETY: the read SQE points at this allocation for `len`
                    // bytes, and a successful CQE of `n` means exactly those first `n`
                    // bytes were initialized by the kernel. The allocation remains in
                    // `in_flight` until this CQE is consumed.
                    unsafe { buf.set_len(n) };
                    send_read(tx, Ok(Bytes::from(std::mem::take(buf))));
                }
            }
            InFlightKind::SyncData { tx, .. }
            | InFlightKind::SyncDirectory { tx, .. }
            | InFlightKind::Remove { tx, .. } => {
                send_unit(tx, Ok(()));
            }
            InFlightKind::MetadataCommit { .. } => {
                unreachable!("metadata commit completions use encoded user_data")
            }
        }
        Ok(())
    }

    fn handle_metadata_commit_completion(
        &mut self,
        id: u64,
        chain_index: usize,
        res: i32,
    ) -> Result<()> {
        let mut should_complete = false;
        let mut should_submit_rename = false;
        if let Some(in_flight) = self.in_flight.get_mut(&id) {
            if let InFlightKind::MetadataCommit {
                payload,
                phase,
                completed,
                failure,
                ..
            } = &mut in_flight.kind
            {
                let phase_index = chain_index
                    .checked_sub(phase.first_chain_index())
                    .filter(|index| *index < METADATA_COMMIT_PHASE_LEN)
                    .expect("metadata completion did not match active commit phase");
                completed[phase_index] = true;
                if res < 0 {
                    if failure.is_none() {
                        *failure = Some(Error::from_raw_os_error(-res));
                    }
                } else if *phase == MetadataCommitPhase::WriteAndSync
                    && phase_index == 0
                    && res as usize != payload.len()
                    && failure.is_none()
                {
                    *failure = Some(Error::new(
                        ErrorKind::WriteZero,
                        format!(
                            "metadata io_uring short write: wrote {} of {} bytes",
                            res,
                            payload.len()
                        ),
                    ));
                }

                if completed.iter().all(|done| *done) {
                    if failure.is_some() || *phase == MetadataCommitPhase::RenameAndSync {
                        should_complete = true;
                    } else {
                        // Durability invariant: rename is not even submitted until the
                        // complete temporary payload and its fdatasync have both completed.
                        *phase = MetadataCommitPhase::RenameAndSync;
                        *completed = [false; METADATA_COMMIT_PHASE_LEN];
                        should_submit_rename = true;
                    }
                }
            }
        }

        if should_complete {
            let mut in_flight = self
                .in_flight
                .remove(&id)
                .expect("completed metadata commit missing");
            self.in_flight_operations.fetch_sub(1, Ordering::SeqCst);
            self.in_flight_sqe_work -= in_flight.reserved_sqe_work();
            if let InFlightKind::MetadataCommit { failure, tx, .. } = &mut in_flight.kind {
                match failure.take() {
                    Some(error) => send_unit(tx, Err(error)),
                    None => send_unit(tx, Ok(())),
                }
            }
        } else if should_submit_rename {
            self.resubmit_metadata_existing(id)?;
        }
        Ok(())
    }

    fn complete_error(&mut self, id: u64, error: Error) {
        if let Some(mut in_flight) = self.in_flight.remove(&id) {
            self.in_flight_operations.fetch_sub(1, Ordering::SeqCst);
            self.in_flight_sqe_work -= in_flight.reserved_sqe_work();
            #[cfg(test)]
            {
                self.record_safety_event(RingPoolSafetyEvent::InFlightReleased);
                // Record the reporting boundary before waking the receiver, so the
                // regression observes the same happens-before ordering as callers.
                self.record_safety_event(RingPoolSafetyEvent::ErrorReported);
            }
            match &mut in_flight.kind {
                InFlightKind::Open { tx, .. } => send_open(tx, Err(error)),
                InFlightKind::Write { tx, .. } => send_write(tx, Err(error)),
                InFlightKind::Read { tx, .. } => send_read(tx, Err(error)),
                InFlightKind::SyncData { tx, .. }
                | InFlightKind::SyncDirectory { tx, .. }
                | InFlightKind::Remove { tx, .. }
                | InFlightKind::MetadataCommit { tx, .. } => send_unit(tx, Err(error)),
            }
        }
    }

    fn fail_all(&mut self, error: &Error) {
        let ids: Vec<_> = self.in_flight.keys().copied().collect();
        for id in ids {
            self.complete_error(id, clone_error(error));
        }
    }

    fn submit_ring(&mut self, wait_for_completion: bool) -> Result<usize> {
        #[cfg(test)]
        {
            let (paused, wake) = &*self.submission_instrumentation.submission_pause;
            let mut paused = paused.lock().expect("ring-pool submission pause poisoned");
            while *paused {
                paused = wake
                    .wait(paused)
                    .expect("ring-pool submission pause poisoned");
            }
        }
        #[cfg(test)]
        if self
            .submission_instrumentation
            .submit_failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            self.record_safety_event(RingPoolSafetyEvent::SubmitFailed);
            return Err(Error::other("injected io_uring submit failure"));
        }

        let ring = self
            .ring
            .as_ref()
            .expect("ring missing before driver abort");
        let result = if wait_for_completion {
            ring.submit_and_wait(1)
        } else {
            ring.submit()
        };
        #[cfg(test)]
        match &result {
            Ok(_) => {
                self.submission_instrumentation
                    .successful_submissions
                    .fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => self.record_safety_event(RingPoolSafetyEvent::SubmitFailed),
        }
        result
    }

    fn abort_after_submit_failure(&mut self, error: Error) {
        self.rx.close();

        // Fail-stop invariant: a submit error permanently terminates this shard;
        // no queued SQE is ever retried on another ring. SQEs may have been consumed
        // by the kernel even when io_uring_enter returned an error. Destroying the
        // ring synchronously cancels and quiesces that kernel context; only after
        // it returns may SQE-backed paths, FDs, and buffers in `in_flight` be
        // released or any accepted request be notified.
        drop(self.ring.take());
        #[cfg(test)]
        self.record_safety_event(RingPoolSafetyEvent::RingDropped);

        self.fail_all(&error);
        while let Some(request) = self.pending.pop_front() {
            self.pending_sqe_work -= request.reserved_sqe_work();
            #[cfg(test)]
            self.record_safety_event(RingPoolSafetyEvent::ErrorReported);
            fail_request(request, clone_error(&error));
        }
        // Do not defeat the normal pending bound while failing. Closing first
        // freezes channel admission; drain directly until all already-issued
        // channel permits have either sent or observed closure.
        while let Some(request) = self.rx.blocking_recv() {
            #[cfg(test)]
            self.record_safety_event(RingPoolSafetyEvent::ErrorReported);
            fail_request(request, clone_error(&error));
        }
    }

    #[cfg(test)]
    fn record_safety_event(&self, event: RingPoolSafetyEvent) {
        self.submission_instrumentation
            .safety_events
            .lock()
            .expect("ring-pool safety instrumentation poisoned")
            .push(event);
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = (self.next_id + 1) & METADATA_USER_DATA_MASK;
        if self.next_id == 0 {
            self.next_id = 1;
        }
        id
    }
}

fn allocate_read_buffer(len: usize) -> Vec<u8> {
    Vec::with_capacity(len)
}

fn build_entry(id: u64, in_flight: &mut InFlight) -> squeue::Entry {
    let entry = match &mut in_flight.kind {
        InFlightKind::Open {
            path, flags, mode, ..
        } => opcode::OpenAt::new(types::Fd(AT_FDCWD), path.as_ptr())
            .flags(*flags)
            .mode(*mode)
            .build(),
        InFlightKind::Write {
            fd,
            offset,
            data,
            written,
            ..
        } => opcode::Write::new(
            types::Fd(fd.as_raw_fd()),
            data[*written..].as_ptr(),
            (data.len() - *written) as u32,
        )
        .offset(*offset + *written as u64)
        .build(),
        InFlightKind::Read {
            fd,
            offset,
            len,
            buf,
            ..
        } => {
            debug_assert!(buf.capacity() >= *len);
            opcode::Read::new(types::Fd(fd.as_raw_fd()), buf.as_mut_ptr(), *len as u32)
                .offset(*offset)
                .build()
        }
        InFlightKind::SyncData { fd, .. } => opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build(),
        InFlightKind::SyncDirectory { fd, .. } => {
            opcode::Fsync::new(types::Fd(fd.as_raw_fd())).build()
        }
        InFlightKind::Remove { path, .. } => {
            opcode::UnlinkAt::new(types::Fd(AT_FDCWD), path.as_ptr()).build()
        }
        InFlightKind::MetadataCommit { .. } => {
            unreachable!("metadata commits are submitted as two linked durability phases")
        }
    };
    entry.user_data(id)
}

fn metadata_user_data(id: u64, chain_index: usize) -> u64 {
    debug_assert!(chain_index < METADATA_COMMIT_CHAIN_LEN);
    (((chain_index as u64) + 1) << METADATA_USER_DATA_SHIFT) | (id & METADATA_USER_DATA_MASK)
}

fn decode_metadata_user_data(user_data: u64) -> Option<(u64, usize)> {
    let tag = user_data >> METADATA_USER_DATA_SHIFT;
    if (1..=METADATA_COMMIT_CHAIN_LEN as u64).contains(&tag) {
        Some((user_data & METADATA_USER_DATA_MASK, (tag - 1) as usize))
    } else {
        None
    }
}

fn build_metadata_commit_entries(
    id: u64,
    in_flight: &InFlight,
) -> ([squeue::Entry; METADATA_COMMIT_PHASE_LEN], usize) {
    let InFlightKind::MetadataCommit {
        tmp_fd,
        parent_fd,
        tmp_name,
        final_name,
        payload,
        phase,
        ..
    } = &in_flight.kind
    else {
        unreachable!("metadata chain requested for non-metadata request")
    };

    let first_chain_index = phase.first_chain_index();
    let entries = match phase {
        MetadataCommitPhase::WriteAndSync => [
            opcode::Write::new(
                types::Fd(tmp_fd.as_raw_fd()),
                payload.as_ptr(),
                payload.len() as u32,
            )
            .offset(0)
            .build()
            .user_data(metadata_user_data(id, first_chain_index))
            .flags(squeue::Flags::IO_LINK),
            opcode::Fsync::new(types::Fd(tmp_fd.as_raw_fd()))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(metadata_user_data(id, first_chain_index + 1)),
        ],
        MetadataCommitPhase::RenameAndSync => [
            opcode::RenameAt::new(
                types::Fd(parent_fd.as_raw_fd()),
                tmp_name.as_ptr(),
                types::Fd(parent_fd.as_raw_fd()),
                final_name.as_ptr(),
            )
            .build()
            .user_data(metadata_user_data(id, first_chain_index))
            .flags(squeue::Flags::IO_LINK),
            opcode::Fsync::new(types::Fd(parent_fd.as_raw_fd()))
                .build()
                .user_data(metadata_user_data(id, first_chain_index + 1)),
        ],
    };
    (entries, first_chain_index)
}

impl InFlight {
    fn into_request(self) -> Request {
        match self.kind {
            InFlightKind::Open {
                path,
                flags,
                mode,
                mut tx,
            } => Request::Open {
                path,
                flags,
                mode,
                tx: tx.take().expect("open sender missing"),
            },
            InFlightKind::Write {
                fd,
                offset,
                data,
                mut tx,
                ..
            } => Request::Write {
                fd,
                offset,
                data,
                tx: tx.take().expect("write sender missing"),
            },
            InFlightKind::Read {
                fd,
                offset,
                len,
                mut tx,
                ..
            } => Request::Read {
                fd,
                offset,
                len,
                tx: tx.take().expect("read sender missing"),
            },
            InFlightKind::SyncData { fd, mut tx } => Request::SyncData {
                fd,
                tx: tx.take().expect("sync sender missing"),
            },
            InFlightKind::SyncDirectory { fd, mut tx } => Request::SyncDirectory {
                fd,
                tx: tx.take().expect("directory sync sender missing"),
            },
            InFlightKind::Remove { path, mut tx } => Request::Remove {
                path,
                tx: tx.take().expect("remove sender missing"),
            },
            InFlightKind::MetadataCommit {
                key,
                tmp_fd,
                parent_fd,
                tmp_name,
                final_name,
                payload,
                mut tx,
                ..
            } => Request::MetadataCommit {
                key,
                tmp_fd,
                parent_fd,
                tmp_name,
                final_name,
                payload,
                tx: tx.take().expect("metadata commit sender missing"),
            },
        }
    }
}

fn clone_error(error: &Error) -> Error {
    Error::new(error.kind(), error.to_string())
}

fn fail_request(request: Request, error: Error) {
    match request {
        Request::Open { tx, .. } => {
            let _ = tx.send(Err(error));
        }
        Request::Write { tx, .. } => {
            let _ = tx.send(Err(error));
        }
        Request::Read { tx, .. } => {
            let _ = tx.send(Err(error));
        }
        Request::SyncData { tx, .. }
        | Request::SyncDirectory { tx, .. }
        | Request::Remove { tx, .. }
        | Request::MetadataCommit { tx, .. } => {
            let _ = tx.send(Err(error));
        }
    }
}

fn send_open(tx: &mut Option<OpenSender>, result: Result<Arc<OwnedFd>>) {
    if let Some(tx) = tx.take() {
        let _ = tx.send(result);
    }
}

fn send_write(tx: &mut Option<WriteSender>, result: Result<usize>) {
    if let Some(tx) = tx.take() {
        let _ = tx.send(result);
    }
}

fn send_read(tx: &mut Option<ReadSender>, result: Result<Bytes>) {
    if let Some(tx) = tx.take() {
        let _ = tx.send(result);
    }
}

fn send_unit(tx: &mut Option<UnitSender>, result: Result<()>) {
    if let Some(tx) = tx.take() {
        let _ = tx.send(result);
    }
}

/// Resolved startup settings for the production Linux io_uring ring pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingPoolStartup {
    pub configured_shards: Option<usize>,
    pub resolved_shards: usize,
    pub cpu_pinning_enabled: bool,
}

static GLOBAL_RING_POOL: OnceLock<Mutex<Option<Arc<RingPool>>>> = OnceLock::new();

fn global_ring_pool_slot() -> &'static Mutex<Option<Arc<RingPool>>> {
    GLOBAL_RING_POOL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
thread_local! {
    static TEST_RING_POOL_OVERRIDES: RefCell<Vec<Arc<RingPool>>> = const { RefCell::new(Vec::new()) };
    static TEST_DEFAULT_RING_POOL: RefCell<Option<Arc<RingPool>>> = const { RefCell::new(None) };
}

#[cfg(test)]
fn active_test_ring_pool_override() -> Option<Arc<RingPool>> {
    TEST_RING_POOL_OVERRIDES.with(|overrides| overrides.borrow().last().cloned())
}

#[cfg(test)]
pub(crate) struct RingPoolTestOverrideGuard {
    expected: Arc<RingPool>,
}

#[cfg(test)]
impl Drop for RingPoolTestOverrideGuard {
    fn drop(&mut self) {
        TEST_RING_POOL_OVERRIDES.with(|overrides| {
            let popped = overrides
                .borrow_mut()
                .pop()
                .expect("ring-pool test override stack underflow");
            assert!(
                Arc::ptr_eq(&popped, &self.expected),
                "ring-pool test overrides must be dropped in LIFO order"
            );
        });
    }
}

#[cfg(test)]
pub(crate) fn scoped_test_ring_pool_override(pool: Arc<RingPool>) -> RingPoolTestOverrideGuard {
    TEST_RING_POOL_OVERRIDES.with(|overrides| overrides.borrow_mut().push(Arc::clone(&pool)));
    RingPoolTestOverrideGuard { expected: pool }
}

/// Validate a resolved shard count before allocating rings or spawning drivers.
fn validate_shard_count(shard_count: usize) -> Result<()> {
    if !(1..=MAX_IO_URING_SHARDS).contains(&shard_count) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "ring pool shard_count must be in 1..={MAX_IO_URING_SHARDS}, got {shard_count}"
            ),
        ));
    }

    Ok(())
}

/// Resolve the configured shard count, using host CPU count for `None`.
pub fn resolve_shard_count(configured_shards: Option<usize>) -> Result<usize> {
    match configured_shards {
        Some(shards) => {
            validate_shard_count(shards)?;
            Ok(shards)
        }
        None => Ok((num_cpus::get() / 4).max(1)),
    }
}

/// Initialize the production ring-pool settings for the Linux default backend.
pub fn initialize_production_ring_pool(
    configured_shards: Option<usize>,
    queue_capacity: usize,
) -> Result<RingPoolStartup> {
    init_global_ring_pool(configured_shards, queue_capacity)
}

/// Install the production global ring pool, or validate that an existing one
/// was initialized with the same production configuration.
pub fn init_global_ring_pool(
    configured_shards: Option<usize>,
    queue_capacity: usize,
) -> Result<RingPoolStartup> {
    let requested_options = RingPoolOptions::production(configured_shards, queue_capacity)?;
    let slot = global_ring_pool_slot();
    let mut guard = slot.lock().expect("global ring-pool slot poisoned");

    if let Some(existing) = guard.as_ref() {
        if existing.options() == &requested_options {
            return Ok(startup_from_pool(configured_shards, existing.as_ref()));
        }

        return Err(Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "global ring pool already initialized with conflicting config: existing={:?}, requested={:?}",
                existing.options(), requested_options
            ),
        ));
    }

    let pool = Arc::new(RingPool::from_options(requested_options)?);
    let startup = startup_from_pool(configured_shards, pool.as_ref());
    *guard = Some(pool);
    Ok(startup)
}

/// Clone the currently active ring pool.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn global_ring_pool() -> Result<Arc<RingPool>> {
    #[cfg(test)]
    {
        if let Some(pool) = active_test_ring_pool_override() {
            return Ok(pool);
        }
    }

    global_ring_pool_slot()
        .lock()
        .expect("global ring-pool slot poisoned")
        .as_ref()
        .cloned()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotConnected,
                "global ring pool is not initialized",
            )
        })
}

/// Clone the currently active ring pool, installing the compatibility default
/// pool if production has not initialized one yet.
pub(crate) fn global_or_default_ring_pool() -> Result<Arc<RingPool>> {
    #[cfg(test)]
    {
        if let Some(pool) = active_test_ring_pool_override() {
            return Ok(pool);
        }

        TEST_DEFAULT_RING_POOL.with(|default_pool| {
            let mut default_pool = default_pool.borrow_mut();
            if let Some(pool) = default_pool.as_ref() {
                return Ok(Arc::clone(pool));
            }

            let pool = Arc::new(RingPool::from_options(RingPoolOptions::production(
                None, 1024,
            )?)?);
            *default_pool = Some(Arc::clone(&pool));
            Ok(pool)
        })
    }

    #[cfg(not(test))]
    {
        let slot = global_ring_pool_slot();
        let mut guard = slot.lock().expect("global ring-pool slot poisoned");
        if let Some(pool) = guard.as_ref() {
            return Ok(Arc::clone(pool));
        }

        let pool = Arc::new(RingPool::from_options(RingPoolOptions::production(
            None, 1024,
        )?)?);
        *guard = Some(Arc::clone(&pool));
        Ok(pool)
    }
}

/// Drop the production global ring pool during orderly shutdown.
pub fn shutdown_global_ring_pool_for_exit() -> Result<Option<RingPoolShutdown>> {
    let pool = global_ring_pool_slot()
        .lock()
        .expect("global ring-pool slot poisoned")
        .take();

    match pool {
        Some(pool) => shutdown_owned_pool_when_unshared(pool),
        None => Ok(None),
    }
}

fn shutdown_owned_pool_when_unshared(pool: Arc<RingPool>) -> Result<Option<RingPoolShutdown>> {
    #[cfg(test)]
    {
        let mut pool = pool;
        for _ in 0..50 {
            match Arc::try_unwrap(pool) {
                Ok(pool) => return pool.shutdown().map(Some),
                Err(shared) if Arc::strong_count(&shared) == 1 => {
                    pool = shared;
                }
                Err(shared) => {
                    pool = shared;
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        }
        match Arc::try_unwrap(pool) {
            Ok(pool) => pool.shutdown().map(Some),
            Err(pool) => {
                drop(pool);
                Ok(None)
            }
        }
    }

    #[cfg(not(test))]
    {
        match Arc::try_unwrap(pool) {
            Ok(pool) => pool.shutdown().map(Some),
            Err(pool) => {
                drop(pool);
                Ok(None)
            }
        }
    }
}

fn startup_from_pool(configured_shards: Option<usize>, pool: &RingPool) -> RingPoolStartup {
    RingPoolStartup {
        configured_shards,
        resolved_shards: pool.shard_count(),
        cpu_pinning_enabled: false,
    }
}

/// Return a stable 64-bit hash for a spool or path key.
///
/// The key bytes are fed directly into SipHash-1-3 with fixed keys, avoiding
/// Rust's `Hash` trait and any hasher state that can vary between runs.
#[allow(dead_code)]
pub(crate) fn stable_key_hash(key: &str) -> u64 {
    stable_key_hash_bytes(key.as_bytes())
}

/// Return a stable 64-bit hash for opaque routing-key bytes.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn stable_key_hash_bytes(key: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(0, 0);
    hasher.write(key);
    hasher.finish()
}

/// Return the ring shard index for a spool or path key.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn ring_index_for_key(key: &str, num_shards: usize) -> usize {
    ring_index_for_key_bytes(key.as_bytes(), num_shards)
}

fn validate_queue_capacity(queue_capacity: usize, driver_sqe_capacity: usize) -> Result<usize> {
    let configuration_error = |message| {
        Error::new(
            ErrorKind::InvalidInput,
            BobsError::ConfigurationError(message),
        )
    };

    if queue_capacity == 0 {
        return Err(configuration_error(
            "ring pool queue_capacity must be greater than 0".to_string(),
        ));
    }
    if queue_capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(configuration_error(format!(
            "ring pool queue_capacity must not exceed {}",
            tokio::sync::Semaphore::MAX_PERMITS
        )));
    }

    queue_capacity
        .checked_mul(MAX_SQES_PER_REQUEST)
        .and_then(|queued_work| queued_work.checked_add(driver_sqe_capacity))
        .ok_or_else(|| {
            configuration_error(
                "ring pool queue_capacity overflows the admitted SQE work bound".to_string(),
            )
        })
}

/// Return the ring shard index for opaque routing-key bytes.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn ring_index_for_key_bytes(key: &[u8], num_shards: usize) -> usize {
    assert!(num_shards > 0, "num_shards must be greater than zero");
    (stable_key_hash_bytes(key) % num_shards as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::{
        global_ring_pool, init_global_ring_pool, ring_index_for_key,
        scoped_test_ring_pool_override, shutdown_global_ring_pool_for_exit, Request, RingPool,
        RingPoolOptions, RingPoolSafetyEvent, MAX_SQES_PER_REQUEST, RING_ENTRIES,
    };
    use crate::io::MAX_IO_URING_SHARDS;
    use std::collections::{HashMap, HashSet};
    use std::env;
    use std::fs::OpenOptions;
    use std::io;
    use std::os::fd::OwnedFd;
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout, Duration};

    const PROBE_ENV: &str = "BOBS_RING_POOL_HASH_PROBE";
    const PROBE_PREFIX: &str = "BOBS_RING_POOL_HASH_PROBE_RESULT";

    fn pinned_vectors() -> &'static [(&'static str, usize, usize)] {
        &[
            ("", 2, 0),
            ("", 4, 0),
            ("", 8, 4),
            ("alpha", 2, 0),
            ("alpha", 4, 2),
            ("alpha", 8, 2),
            ("spool-a", 2, 0),
            ("spool-a", 4, 2),
            ("spool-a", 8, 6),
            ("spool-b", 2, 0),
            ("spool-b", 4, 0),
            ("spool-b", 8, 0),
            ("key-5", 2, 1),
            ("key-5", 4, 3),
            ("key-5", 8, 7),
            ("key-6", 2, 1),
            ("key-6", 4, 1),
            ("key-6", 8, 1),
            ("key-12", 2, 1),
            ("key-12", 4, 3),
            ("key-12", 8, 3),
            ("00000000-0000-4000-8000-000000000000", 2, 0),
            ("00000000-0000-4000-8000-000000000000", 4, 2),
            ("00000000-0000-4000-8000-000000000000", 8, 6),
            ("ffffffff-ffff-4fff-bfff-ffffffffffff", 2, 0),
            ("ffffffff-ffff-4fff-bfff-ffffffffffff", 4, 2),
            ("ffffffff-ffff-4fff-bfff-ffffffffffff", 8, 2),
            ("tenant/ns/spool-0001", 2, 0),
            ("tenant/ns/spool-0001", 4, 2),
            ("tenant/ns/spool-0001", 8, 2),
        ]
    }

    fn restart_probe_cases() -> &'static [(&'static str, usize)] {
        &[
            ("spool-a", 2),
            ("spool-a", 4),
            ("spool-a", 8),
            ("spool-b", 2),
            ("spool-b", 4),
            ("spool-b", 8),
            ("00000000-0000-4000-8000-000000000000", 2),
            ("00000000-0000-4000-8000-000000000000", 4),
            ("00000000-0000-4000-8000-000000000000", 8),
            ("ffffffff-ffff-4fff-bfff-ffffffffffff", 2),
            ("ffffffff-ffff-4fff-bfff-ffffffffffff", 4),
            ("ffffffff-ffff-4fff-bfff-ffffffffffff", 8),
            ("tenant/ns/spool-0001", 2),
            ("tenant/ns/spool-0001", 4),
            ("tenant/ns/spool-0001", 8),
        ]
    }

    fn uuid_like_key(n: u64) -> String {
        let a = n.wrapping_mul(0x9e37_79b1);
        let b = n.wrapping_mul(0x85eb_ca6b) as u16;
        let c = n.wrapping_mul(0xc2b2_ae35) & 0x0fff;
        let d = 0x8000 | (n.wrapping_mul(0x27d4_eb2d) & 0x3fff);
        let e = n.wrapping_mul(0x1656_67b1_9e37_79f9) & 0x0000_ffff_ffff_ffff;
        format!("{a:08x}-{b:04x}-4{c:03x}-{d:04x}-{e:012x}")
    }

    #[test]
    fn ring_pool_hash_stability() {
        let expected_after_restart: Vec<_> = restart_probe_cases()
            .iter()
            .map(|(key, shards)| ((*key).to_owned(), *shards, ring_index_for_key(key, *shards)))
            .collect();

        let output = Command::new(env::current_exe().expect("current test binary path"))
            .arg("--exact")
            .arg("io::ring_pool::tests::ring_pool_hash_subprocess_probe")
            .arg("--nocapture")
            .env(PROBE_ENV, "1")
            .output()
            .expect("failed to run ring-pool hash probe in a fresh process");
        assert!(
            output.status.success(),
            "hash probe failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).expect("probe stdout must be UTF-8");
        let mut actual_after_restart = Vec::new();
        for line in stdout.lines() {
            let Some(rest) = line.strip_prefix(PROBE_PREFIX) else {
                continue;
            };
            let mut parts = rest.trim_start().splitn(3, ' ');
            let shards: usize = parts
                .next()
                .expect("probe line has shard count")
                .parse()
                .expect("probe shard count is numeric");
            let index: usize = parts
                .next()
                .expect("probe line has ring index")
                .parse()
                .expect("probe ring index is numeric");
            let key = parts.next().expect("probe line has key").to_owned();
            actual_after_restart.push((key, shards, index));
        }
        assert_eq!(actual_after_restart, expected_after_restart);

        for &(key, num_shards, expected_index) in pinned_vectors() {
            assert_eq!(
                ring_index_for_key(key, num_shards),
                expected_index,
                "pinned ring index changed for key={key:?}, num_shards={num_shards}"
            );
        }
    }

    #[test]
    fn ring_pool_hash_subprocess_probe() {
        if env::var_os(PROBE_ENV).is_none() {
            return;
        }

        for &(key, num_shards) in restart_probe_cases() {
            let index = ring_index_for_key(key, num_shards);
            println!("{PROBE_PREFIX} {num_shards} {index} {key}");
        }
    }

    #[test]
    fn ring_pool_resolve_shard_count_defaults_to_cpu_quarter() {
        assert_eq!(
            super::resolve_shard_count(None).expect("auto shard count should resolve"),
            (num_cpus::get() / 4).max(1)
        );
        assert_eq!(
            super::resolve_shard_count(Some(7)).expect("explicit shard count should resolve"),
            7
        );
        assert_eq!(
            super::resolve_shard_count(Some(0))
                .expect_err("zero shard count should be rejected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn ring_pool_resolve_shard_count_enforces_documented_maximum() {
        assert_eq!(
            super::resolve_shard_count(Some(MAX_IO_URING_SHARDS))
                .expect("maximum shard count should resolve"),
            MAX_IO_URING_SHARDS
        );

        for shard_count in [MAX_IO_URING_SHARDS + 1, usize::MAX] {
            let err = super::resolve_shard_count(Some(shard_count))
                .expect_err("oversized shard count should be rejected");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn ring_pool_direct_construction_rejects_oversized_shard_count() {
        let public_err = RingPool::new(Some(usize::MAX))
            .expect_err("public construction must reject oversized shard counts");
        assert_eq!(public_err.kind(), io::ErrorKind::InvalidInput);

        let options_err = RingPool::new_for_test(RingPoolOptions {
            shard_count: usize::MAX,
            queue_capacity: 1024,
            driver_name_prefix: "bobs-uring-oversized-test".to_owned(),
        })
        .expect_err("direct options must reject oversized shard counts");

        assert_eq!(options_err.kind(), io::ErrorKind::InvalidInput);
        assert!(options_err.to_string().contains("shard_count"));
    }

    #[test]
    fn ring_pool_dispatch_distribution() {
        const KEY_COUNT: u64 = 12_000;

        for num_shards in [2usize, 4, 8] {
            let mut counts = vec![0usize; num_shards];
            let mut seen_keys = HashSet::with_capacity(KEY_COUNT as usize);

            for n in 0..KEY_COUNT {
                let key = uuid_like_key(n);
                assert!(
                    seen_keys.insert(key.clone()),
                    "UUID-like key repeated: {key}"
                );
                let index = ring_index_for_key(&key, num_shards);
                assert!(
                    index < num_shards,
                    "ring index {index} out of range for {num_shards} shards"
                );
                counts[index] += 1;
            }

            assert!(
                counts.iter().all(|&count| count > 0),
                "all shards should receive at least one key for {num_shards} shards: {counts:?}"
            );

            let expected = KEY_COUNT as f64 / num_shards as f64;
            let max_allowed_delta = expected * 0.20;
            for (index, &count) in counts.iter().enumerate() {
                let delta = (count as f64 - expected).abs();
                assert!(
                    delta <= max_allowed_delta,
                    "shard {index} received {count} keys for {num_shards} shards; expected about {expected:.1}, counts={counts:?}"
                );
            }
        }
    }

    fn explicit_test_options(shard_count: usize) -> RingPoolOptions {
        RingPoolOptions {
            shard_count,
            queue_capacity: 1024,
            driver_name_prefix: "bobs-uring-test".to_owned(),
        }
    }

    fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn ring_pool_global_initialization() {
        let _lock = global_test_lock()
            .lock()
            .expect("ring-pool global test lock poisoned");
        let _ = shutdown_global_ring_pool_for_exit();

        let startup = init_global_ring_pool(Some(2), 1024)
            .expect("production global ring pool should initialize");
        assert_eq!(startup.configured_shards, Some(2));
        assert_eq!(startup.resolved_shards, 2);
        let first = global_ring_pool().expect("initialized global ring pool should be available");
        assert_eq!(first.shard_count(), 2);

        let matching = init_global_ring_pool(Some(2), 1024)
            .expect("matching global ring-pool initialization should be idempotent");
        let second = global_ring_pool().expect("global ring pool should remain available");
        assert_eq!(matching, startup);
        assert!(
            Arc::ptr_eq(&first, &second),
            "idempotent initialization must keep the installed pool"
        );

        let conflict = init_global_ring_pool(Some(3), 1024)
            .expect_err("conflicting global ring-pool initialization should be rejected");
        assert_eq!(conflict.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            conflict.to_string().contains("conflicting config"),
            "conflict error should identify the cause clearly: {conflict}"
        );
        assert!(
            conflict.to_string().contains("requested"),
            "conflict error should include the requested config: {conflict}"
        );

        drop(first);
        drop(second);
        let shutdown = shutdown_global_ring_pool_for_exit()
            .expect("global ring-pool shutdown should succeed")
            .expect("initialized global ring pool should be dropped on shutdown");
        assert_eq!(shutdown.joined_driver_handles, 2);
        assert_eq!(shutdown.in_flight_operations_remaining, 0);
        assert!(shutdown.driver_threads_all_stopped);
    }

    #[test]
    fn ring_pool_test_override_isolated() {
        let _lock = global_test_lock()
            .lock()
            .expect("ring-pool global test lock poisoned");
        let _ = shutdown_global_ring_pool_for_exit();

        assert!(
            global_ring_pool().is_err(),
            "test should start without a leaked production global ring pool"
        );

        let direct_pool = Arc::new(
            RingPool::new_for_test(explicit_test_options(1))
                .expect("direct instrumented test ring pool should start"),
        );
        {
            let _override = scoped_test_ring_pool_override(Arc::clone(&direct_pool));
            let active = global_ring_pool().expect("test override should satisfy global lookup");
            assert!(
                Arc::ptr_eq(&active, &direct_pool),
                "test override should return the direct pool, not install a production global"
            );
            assert_eq!(active.shard_count(), 1);
        }

        assert!(
            global_ring_pool().is_err(),
            "dropping the scoped override must remove the direct pool without leaking a global"
        );

        let _startup = init_global_ring_pool(Some(2), 1024)
            .expect("production global ring pool should initialize after override drops");
        let production_pool = global_ring_pool().expect("production global should be available");
        assert_eq!(production_pool.shard_count(), 2);

        {
            let _override = scoped_test_ring_pool_override(Arc::clone(&direct_pool));
            let active = global_ring_pool().expect("test override should shadow production global");
            assert!(Arc::ptr_eq(&active, &direct_pool));
        }

        let active =
            global_ring_pool().expect("production global should be restored after override");
        assert!(Arc::ptr_eq(&active, &production_pool));

        drop(active);
        drop(production_pool);
        shutdown_global_ring_pool_for_exit()
            .expect("global ring-pool shutdown should succeed")
            .expect("production global should be present for shutdown");
        drop(direct_pool);
    }

    #[test]
    fn ring_pool_explicit_instance_lifecycle_metadata() {
        let pool = RingPool::new_for_test(explicit_test_options(3))
            .expect("explicit RingPool instance should start");

        assert_eq!(pool.shard_count(), 3);
        assert_eq!(
            pool.driver_names(),
            [
                "bobs-uring-test-0".to_owned(),
                "bobs-uring-test-1".to_owned(),
                "bobs-uring-test-2".to_owned(),
            ]
        );
        assert_eq!(pool.in_flight_operations(), 0);

        let shutdown = pool.shutdown().expect("RingPool shutdown should succeed");
        assert_eq!(shutdown.joined_driver_handles, 3);
        assert_eq!(shutdown.in_flight_operations_remaining, 0);
    }

    #[test]
    fn ring_pool_rejects_zero_queue_capacity() {
        let err = RingPool::new_for_test(RingPoolOptions {
            shard_count: 1,
            queue_capacity: 0,
            driver_name_prefix: "bobs-uring-test".to_owned(),
        })
        .expect_err("zero queue capacity should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn ring_pool_accepts_tokio_queue_capacity_boundary() {
        let pool = RingPool::new_for_test(RingPoolOptions {
            shard_count: 1,
            queue_capacity: tokio::sync::Semaphore::MAX_PERMITS,
            driver_name_prefix: "bobs-uring-boundary-test".to_owned(),
        })
        .expect("Tokio's maximum channel capacity should construct without panicking");

        let shutdown = pool.shutdown().expect("boundary pool should shut down");
        assert_eq!(shutdown.joined_driver_handles, 1);
    }

    #[test]
    fn ring_pool_options_reject_queue_capacity_above_tokio_limit() {
        let err = RingPoolOptions::production(Some(1), tokio::sync::Semaphore::MAX_PERMITS + 1)
            .expect_err("production options must reject an oversized queue");

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("must not exceed"));
        assert!(matches!(
            err.get_ref()
                .and_then(|source| source.downcast_ref::<crate::error::BobsError>()),
            Some(crate::error::BobsError::ConfigurationError(_))
        ));
    }

    #[test]
    fn ring_pool_construction_rejects_oversized_custom_options_without_panicking() {
        let err = RingPool::new_for_test(RingPoolOptions {
            shard_count: 1,
            queue_capacity: tokio::sync::Semaphore::MAX_PERMITS + 1,
            driver_name_prefix: "bobs-uring-test".to_owned(),
        })
        .expect_err("custom options must be revalidated before channel construction");

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("must not exceed"));
        assert!(matches!(
            err.get_ref()
                .and_then(|source| source.downcast_ref::<crate::error::BobsError>()),
            Some(crate::error::BobsError::ConfigurationError(_))
        ));
    }

    #[test]
    fn ring_pool_rejects_queue_capacity_that_cannot_be_bounded() {
        let err = RingPool::new_for_test(RingPoolOptions {
            shard_count: 1,
            queue_capacity: usize::MAX,
            driver_name_prefix: "bobs-uring-test".to_owned(),
        })
        .expect_err("unrepresentable queue capacity should be rejected, not panic");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn ring_pool_driver_shutdown() {
        let pool = RingPool::new_for_test(explicit_test_options(4))
            .expect("explicit RingPool instance should start");

        assert_eq!(pool.shard_count(), 4);
        assert_eq!(pool.driver_join_handle_count(), 4);
        assert_eq!(pool.in_flight_operations(), 0);

        let shutdown = pool
            .shutdown()
            .expect("RingPool shutdown should join all drivers");
        assert_eq!(
            shutdown.joined_driver_handles, 4,
            "all shard driver JoinHandles must be joined"
        );
        assert_eq!(
            shutdown.in_flight_operations_remaining, 0,
            "shutdown must leave no in-flight operations"
        );
        assert!(shutdown.driver_threads_all_stopped);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn ring_driver_bounds_backpressure_and_submits_under_sustained_arrivals() {
        const QUEUE_CAPACITY: usize = RING_ENTRIES as usize;
        const REQUESTS: usize = RING_ENTRIES as usize + QUEUE_CAPACITY + 32;
        const DRIVER_ONE_SQE_LIMIT: usize = RING_ENTRIES as usize - MAX_SQES_PER_REQUEST + 1;

        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_dir.path().join("bounded-driver.dat"))
            .expect("bounded driver test file should open");
        let fd = Arc::new(OwnedFd::from(file));
        let pool = Arc::new(
            RingPool::new_for_test(RingPoolOptions {
                shard_count: 1,
                queue_capacity: QUEUE_CAPACITY,
                driver_name_prefix: "bobs-uring-backpressure-test".to_owned(),
            })
            .expect("backpressure test pool should start"),
        );
        let receive_pause = pool.pause_receives_for_test();
        let submission_pause = pool.pause_submissions_for_test();
        let accepted = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let pool = Arc::clone(&pool);
            let fd = Arc::clone(&fd);
            let accepted = Arc::clone(&accepted);
            let completed = Arc::clone(&completed);
            handles.push(tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                pool.submit_to_ring(0, Request::SyncData { fd, tx }).await?;
                accepted.fetch_add(1, Ordering::SeqCst);
                let result = rx.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "driver dropped response")
                })?;
                if result.is_ok() {
                    completed.fetch_add(1, Ordering::SeqCst);
                }
                result
            }));
        }

        timeout(Duration::from_secs(2), async {
            loop {
                if accepted.load(Ordering::SeqCst) > QUEUE_CAPACITY {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("blocked driver should leave the bounded channel full");
        drop(receive_pause);

        timeout(Duration::from_secs(2), async {
            loop {
                if pool.submission_events().len() == DRIVER_ONE_SQE_LIMIT
                    && accepted.load(Ordering::SeqCst) == DRIVER_ONE_SQE_LIMIT + QUEUE_CAPACITY
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("driver should fill only its SQE budget and the bounded channel");

        sleep(Duration::from_millis(25)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            DRIVER_ONE_SQE_LIMIT + QUEUE_CAPACITY,
            "later senders must remain blocked while driver and channel budgets are exhausted"
        );
        assert_eq!(pool.successful_submissions_for_test(), 0);
        assert_eq!(
            pool.max_driver_sqe_work_for_test(),
            DRIVER_ONE_SQE_LIMIT,
            "driver-held work must stay within the ring SQE budget"
        );

        drop(submission_pause);
        timeout(Duration::from_secs(2), async {
            loop {
                if pool.successful_submissions_for_test() > 0
                    && completed.load(Ordering::SeqCst) > 0
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("real ring submissions must progress while later arrivals remain queued");

        for handle in handles {
            timeout(Duration::from_secs(5), handle)
                .await
                .expect("sustained-arrival request should not hang")
                .expect("sender task should not panic")
                .expect("fsync request should complete");
        }
        assert_eq!(completed.load(Ordering::SeqCst), REQUESTS);

        drop(fd);
        let pool = Arc::try_unwrap(pool).expect("test should own ring pool after tasks finish");
        let shutdown = pool.shutdown().expect("ring pool should shut down");
        assert_eq!(shutdown.in_flight_operations_remaining, 0);
        assert!(shutdown.driver_threads_all_stopped);
    }

    #[tokio::test]
    async fn submit_failure_quiesces_ring_before_releasing_buffers_or_reporting_errors() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_dir.path().join("submit-failure.dat"))
            .expect("submit-failure test file should open");
        let fd = Arc::new(OwnedFd::from(file));
        let pool = Arc::new(
            RingPool::new_for_test(explicit_test_options(1))
                .expect("submit-failure test pool should start"),
        );
        pool.inject_submit_failure_for_test();

        let (tx, rx) = oneshot::channel();
        pool.submit_to_ring(
            0,
            Request::Write {
                fd: Arc::clone(&fd),
                offset: 0,
                data: bytes::Bytes::from(vec![0xA5; 1024 * 1024]),
                tx,
            },
        )
        .await
        .expect("driver should accept injected-failure request");
        let error = timeout(Duration::from_secs(2), rx)
            .await
            .expect("submit failure should be reported promptly")
            .expect("driver should report an explicit submit error")
            .expect_err("injected submit must fail");
        assert_eq!(error.kind(), io::ErrorKind::Other);

        assert_eq!(
            pool.safety_events(),
            vec![
                RingPoolSafetyEvent::SubmitFailed,
                RingPoolSafetyEvent::RingDropped,
                RingPoolSafetyEvent::InFlightReleased,
                RingPoolSafetyEvent::ErrorReported,
            ],
            "the ring must quiesce before SQE-backed storage is released or callers are notified"
        );
        assert_eq!(pool.in_flight_operations(), 0);

        let (later_tx, _later_rx) = oneshot::channel();
        let later_error = pool
            .submit_to_ring(0, Request::SyncData { fd, tx: later_tx })
            .await
            .expect_err("a failed shard must remain fail-stop");
        assert_eq!(later_error.kind(), io::ErrorKind::BrokenPipe);

        drop(pool);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn submit_failure_reports_driver_and_channel_backlog_after_ring_drop() {
        const QUEUE_CAPACITY: usize = RING_ENTRIES as usize;
        const DRIVER_ONE_SQE_LIMIT: usize = RING_ENTRIES as usize - MAX_SQES_PER_REQUEST + 1;
        const REQUESTS: usize = DRIVER_ONE_SQE_LIMIT + QUEUE_CAPACITY;

        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_dir.path().join("submit-failure-backlog.dat"))
            .expect("submit-failure backlog file should open");
        let fd = Arc::new(OwnedFd::from(file));
        let pool = Arc::new(
            RingPool::new_for_test(RingPoolOptions {
                shard_count: 1,
                queue_capacity: QUEUE_CAPACITY,
                driver_name_prefix: "bobs-uring-submit-failure-test".to_owned(),
            })
            .expect("submit-failure backlog pool should start"),
        );
        let receive_pause = pool.pause_receives_for_test();
        let submission_pause = pool.pause_submissions_for_test();
        pool.inject_submit_failure_for_test();
        let accepted = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let pool = Arc::clone(&pool);
            let fd = Arc::clone(&fd);
            let accepted = Arc::clone(&accepted);
            handles.push(tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                pool.submit_to_ring(0, Request::SyncData { fd, tx }).await?;
                accepted.fetch_add(1, Ordering::SeqCst);
                rx.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "driver dropped response")
                })?
            }));
        }

        timeout(Duration::from_secs(2), async {
            loop {
                if accepted.load(Ordering::SeqCst) > QUEUE_CAPACITY {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("blocked driver should leave the bounded channel full");
        drop(receive_pause);

        timeout(Duration::from_secs(2), async {
            loop {
                if pool.submission_events().len() == DRIVER_ONE_SQE_LIMIT
                    && accepted.load(Ordering::SeqCst) == REQUESTS
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("both driver budget and channel backlog should fill before failure");
        drop(submission_pause);

        for handle in handles {
            let error = timeout(Duration::from_secs(3), handle)
                .await
                .expect("submit-failure request should not hang")
                .expect("sender task should not panic")
                .expect_err("injected submit must fail every accepted request");
            assert_eq!(error.kind(), io::ErrorKind::Other);
        }

        let events = pool.safety_events();
        assert_eq!(events.first(), Some(&RingPoolSafetyEvent::SubmitFailed));
        assert_eq!(events.get(1), Some(&RingPoolSafetyEvent::RingDropped));
        assert_eq!(
            events
                .iter()
                .filter(|event| **event == RingPoolSafetyEvent::InFlightReleased)
                .count(),
            DRIVER_ONE_SQE_LIMIT
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| **event == RingPoolSafetyEvent::ErrorReported)
                .count(),
            REQUESTS
        );
        assert_eq!(pool.in_flight_operations(), 0);
    }

    #[test]
    fn large_read_buffers_skip_initialization_until_kernel_completion() {
        let requested = 16 * 1024 * 1024;
        let buffer = super::allocate_read_buffer(requested);
        assert_eq!(buffer.len(), 0, "read allocation must not initialize bytes");
        assert!(
            buffer.capacity() >= requested,
            "read allocation must reserve the full kernel-visible range"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn ring_pool_submission_contention_safety() {
        const TASKS: usize = 128;

        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_dir.path().join("contention-safety.dat"))
            .expect("contention test file should open");
        let fd = Arc::new(OwnedFd::from(file));
        let pool = Arc::new(
            RingPool::new_for_test(explicit_test_options(1))
                .expect("single-shard contention test pool should start"),
        );

        let mut handles = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            let pool = Arc::clone(&pool);
            let fd = Arc::clone(&fd);
            handles.push(tokio::spawn(async move {
                let (tx, rx) = tokio::sync::oneshot::channel();
                pool.submit_to_ring(0, Request::SyncData { fd, tx }).await?;
                rx.await.map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "ring-pool contention response channel closed",
                    )
                })?
            }));
        }

        for handle in handles {
            handle
                .await
                .expect("contention submitter task should not panic")
                .expect("contention submitter operation should complete");
        }

        let mut events = pool.submission_events();
        events.sort_by_key(|event| event.push_sequence);
        assert_eq!(
            events.len(),
            TASKS,
            "each submitted operation should be pushed exactly once; events={events:?}"
        );

        let expected_driver_name = "bobs-uring-test-0".to_owned();
        let mut seen_user_data = HashSet::with_capacity(events.len());
        let mut previous_user_data = 0;
        let mut operation_event_indices: HashMap<u64, Vec<usize>> = HashMap::new();

        for (index, event) in events.iter().enumerate() {
            assert_eq!(
                event.shard_index, 0,
                "all submissions should target shard 0"
            );
            assert_eq!(event.driver_name, expected_driver_name);
            assert_eq!(
                event.pushing_thread_name.as_deref(),
                Some(expected_driver_name.as_str()),
                "SQEs must be pushed only by the shard driver thread; event={event:?}"
            );
            assert_eq!(event.push_sequence, index as u64);
            assert!(
                seen_user_data.insert(event.user_data),
                "SQE user_data values must be unique per shard; duplicate event={event:?}"
            );
            assert!(
                event.user_data > previous_user_data,
                "SQE user_data values must be monotonic per shard: previous={previous_user_data}, event={event:?}"
            );
            previous_user_data = event.user_data;
            operation_event_indices
                .entry(event.operation_id)
                .or_default()
                .push(index);
        }

        for (operation_id, indices) in operation_event_indices {
            let first = indices[0];
            let chain_len = events[first].chain_len as usize;
            assert_eq!(
                indices.len(),
                chain_len,
                "operation {operation_id} should record exactly its declared chain length"
            );
            for (chain_index, event_index) in indices.iter().copied().enumerate() {
                assert_eq!(
                    event_index,
                    first + chain_index,
                    "chain entries for operation {operation_id} must be contiguous, not interleaved"
                );
                assert_eq!(events[event_index].chain_index as usize, chain_index);
                assert_eq!(events[event_index].chain_len as usize, chain_len);
            }
        }

        drop(fd);
        let pool = Arc::try_unwrap(pool).expect("test should own the ring pool after tasks finish");
        let shutdown = pool
            .shutdown()
            .expect("contention test ring pool should shut down cleanly");
        assert_eq!(shutdown.in_flight_operations_remaining, 0);
        assert!(shutdown.driver_threads_all_stopped);
    }
}
