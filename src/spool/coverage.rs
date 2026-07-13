// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::mem;

/// Tracks which byte ranges of an object have not yet been served.
///
/// Internally maintains a sorted set of non-overlapping half-open intervals
/// `[start, end)` that are still *missing* (not yet served). When the map
/// is empty **and** `total_size` is known, every byte has been served at
/// least once and `is_complete()` returns `true`.
///
/// A fragmentation cap prevents unbounded memory growth from random-access
/// clients. Adjacent and overlapping pre-completion reads are coalesced before
/// the cap is checked, so a sequential follow stream remains one interval.
/// Genuinely fragmented coverage that exceeds the cap enters a conservative
/// fallback: aggregate tracking stops and `is_complete()` remains false. A
/// later successfully completed contiguous full-object response can recover
/// exact coverage without retaining the discarded fragments.
///
/// Ranges served before `initialize()` is called are buffered in `pending`
/// and replayed once `total_size` is known.
pub struct MissingRanges {
    /// Sorted non-overlapping half-open intervals `[gap_start, gap_end)` not yet served.
    /// Empty AND `total_size` is `Some` → full coverage achieved.
    gaps: BTreeMap<u64, u64>,
    /// `None` until `initialize()` is called.
    pub total_size: Option<u64>,
    /// Maximum number of distinct intervals allowed before capping.
    fragmentation_cap: usize,
    /// `true` after fragmented coverage overflow; aggregate tracking is disabled.
    capped: bool,
    /// Coalesced served intervals observed before `total_size` was known.
    pending: BTreeMap<u64, u64>,
}

impl MissingRanges {
    /// Create a new tracker with the given fragmentation cap.
    ///
    /// The cap bounds genuinely fragmented missing/served interval state.
    /// Sequential and overlapping streams are coalesced before it is applied.
    pub fn new(fragmentation_cap: usize) -> Self {
        Self {
            gaps: BTreeMap::new(),
            total_size: None,
            fragmentation_cap,
            capped: false,
            pending: BTreeMap::new(),
        }
    }

    /// Called once when the total object size is known (e.g. on `complete()`).
    ///
    /// Idempotent: subsequent calls are ignored. Sets up the initial single gap
    /// `[0, total_size)` and replays any ranges that were served before the
    /// size was known.
    pub fn initialize(&mut self, total_size: u64) {
        if self.total_size.is_some() {
            return;
        }
        self.total_size = Some(total_size);
        if total_size == 0 {
            // Zero-byte objects are complete regardless of prior irrelevant ranges.
            self.capped = false;
            self.pending.clear();
            return;
        }
        if self.capped {
            self.pending.clear();
            return;
        }
        self.gaps.insert(0, total_size);
        let pending = mem::take(&mut self.pending);
        for (start, end) in pending {
            self.apply(start, end);
            if self.capped {
                break;
            }
        }
    }

    /// Record that bytes `[start, end)` have been served.
    ///
    /// - If `start >= end` this is a no-op.
    /// - If `total_size` is not yet known, adjacent and overlapping ranges are
    ///   coalesced in `pending` and replayed when `initialize()` is called.
    /// - Once `capped` is `true`, aggregate calls are ignored. Recovery requires
    ///   `mark_contiguous_response_complete()` for one successful full response.
    pub fn mark_served(&mut self, start: u64, end: u64) {
        if self.capped || start >= end {
            return;
        }
        if self.total_size.is_none() {
            self.insert_pending(start, end);
            return;
        }
        self.apply(start, end);
    }

    /// Record the exact extent of one successfully completed contiguous response.
    ///
    /// In the normal tracking mode, per-chunk `mark_served()` calls already carry
    /// coverage. In fragmented fallback this is the bounded recovery path: only a
    /// single response covering `[0, total_size)` can restore completeness.
    pub fn mark_contiguous_response_complete(&mut self, start: u64, end: u64) {
        if !self.capped {
            return;
        }
        if self
            .total_size
            .is_some_and(|total_size| start == 0 && end >= total_size)
        {
            self.capped = false;
            self.gaps.clear();
            self.pending.clear();
        }
    }

    /// Returns `true` when every byte `[0, total_size)` has been served.
    ///
    /// Always `false` if `total_size` is unknown or fragmented tracking is in
    /// conservative fallback.
    pub fn is_complete(&self) -> bool {
        !self.capped && self.total_size.is_some() && self.gaps.is_empty()
    }

    /// Number of distinct gap intervals currently tracked.
    ///
    /// `0` when capped (all gap data is discarded on cap).
    pub fn gap_count(&self) -> usize {
        self.gaps.len()
    }

    /// Whether the fragmentation cap was exceeded.
    pub fn is_capped(&self) -> bool {
        self.capped
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Insert one pre-completion served interval, merging overlap and adjacency
    /// before enforcing the fragmentation cap.
    fn insert_pending(&mut self, start: u64, end: u64) {
        let overlapping: Vec<(u64, u64)> = self
            .pending
            .range(..=end)
            .filter(|(_, &pending_end)| pending_end >= start)
            .map(|(&pending_start, &pending_end)| (pending_start, pending_end))
            .collect();

        let mut merged_start = start;
        let mut merged_end = end;
        for (pending_start, pending_end) in overlapping {
            self.pending.remove(&pending_start);
            merged_start = merged_start.min(pending_start);
            merged_end = merged_end.max(pending_end);
        }
        self.pending.insert(merged_start, merged_end);

        if self.pending.len() > self.fragmentation_cap {
            self.cap();
        }
    }

    /// Apply a served range `[start, end)` against the current gap map.
    ///
    /// Complexity: O((k + 1) log n) where k = overlapping gaps, n = total gaps.
    fn apply(&mut self, start: u64, end: u64) {
        // Collect every gap whose interval overlaps [start, end).
        // Overlap condition: gap_start < end  AND  gap_end > start.
        // BTreeMap::range(..end) gives us all entries with gap_start < end.
        let overlapping: Vec<(u64, u64)> = self
            .gaps
            .range(..end)
            .filter(|(_, &gap_end)| gap_end > start)
            .map(|(&gap_start, &gap_end)| (gap_start, gap_end))
            .collect();

        for (gap_start, gap_end) in overlapping {
            self.gaps.remove(&gap_start);
            // Preserve left portion of gap not covered by served range.
            if gap_start < start {
                self.gaps.insert(gap_start, start);
            }
            // Preserve right portion of gap not covered by served range.
            if gap_end > end {
                self.gaps.insert(end, gap_end);
            }
        }

        if self.gaps.len() > self.fragmentation_cap {
            self.cap();
        }
    }

    fn cap(&mut self) {
        self.capped = true;
        self.gaps.clear();
        self.pending.clear();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Basic completeness
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_object_is_immediately_complete() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(0);
        assert!(
            mr.is_complete(),
            "zero-byte object should be immediately complete"
        );
    }

    #[test]
    fn test_single_full_range() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(0, 100);
        assert!(mr.is_complete());
    }

    #[test]
    fn test_partial_range_not_complete() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(0, 50);
        assert!(!mr.is_complete());
        assert_eq!(mr.gap_count(), 1, "one gap [50, 100) should remain");
    }

    // -----------------------------------------------------------------------
    // Multi-range scenarios
    // -----------------------------------------------------------------------

    #[test]
    fn test_two_adjacent_ranges() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(0, 50);
        mr.mark_served(50, 100);
        assert!(mr.is_complete());
    }

    #[test]
    fn test_two_overlapping_ranges() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(0, 60);
        mr.mark_served(40, 100);
        assert!(mr.is_complete());
    }

    #[test]
    fn test_out_of_order_ranges() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(50, 100);
        mr.mark_served(0, 50);
        assert!(mr.is_complete());
    }

    #[test]
    fn test_covering_superset_range() {
        // A range that extends beyond total_size should still complete coverage.
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(0, 200);
        assert!(
            mr.is_complete(),
            "superset range covering [0,200) should complete a 100-byte object"
        );
    }

    #[test]
    fn test_non_contiguous_partial() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        mr.mark_served(0, 30);
        mr.mark_served(70, 100);
        assert!(!mr.is_complete());
        assert_eq!(mr.gap_count(), 1, "one gap [30, 70) should remain");
    }

    // -----------------------------------------------------------------------
    // Huge object — no large allocation
    // -----------------------------------------------------------------------

    /// Simple 64-bit LCG for deterministic pseudo-random numbers without
    /// pulling in the `rand` crate.
    fn lcg_next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state >> 33
    }

    #[test]
    fn test_huge_object_no_large_alloc() {
        // 100 GiB object — no GiB-scale allocation should occur.
        const TOTAL: u64 = 100 * 1024 * 1024 * 1024;
        const CHUNK: u64 = 1024 * 1024; // 1 MiB chunks
        const N_CHUNKS: u64 = 10_000;

        let mut mr = MissingRanges::new(1024);
        mr.initialize(TOTAL);

        let mut rng: u64 = 0xDEAD_BEEF_1337_CAFE;
        for _ in 0..N_CHUNKS {
            // Pick a chunk index in [0, N_CHUNKS) pseudo-randomly.
            let idx = lcg_next(&mut rng) % N_CHUNKS;
            let start = idx * CHUNK;
            let end = start + CHUNK;
            mr.mark_served(start, end);
            // Fragmentation cap guarantees gap count never exceeds the cap.
            assert!(
                mr.gap_count() <= 1024,
                "gap_count {} exceeded fragmentation cap",
                mr.gap_count()
            );
        }
        // Whether capped or not, we should not have OOM'd. Test reaching here
        // is the primary assertion.
    }

    // -----------------------------------------------------------------------
    // Fragmentation cap
    // -----------------------------------------------------------------------

    #[test]
    fn test_fragmentation_cap_is_conservative_and_full_response_recovers() {
        // Use a small cap so we can trigger it within total_size=100.
        let mut mr = MissingRanges::new(10);
        mr.initialize(100);

        // Serve non-adjacent 1-byte ranges: [0,1), [2,3), [4,5), ...
        // Each one splits the rightmost gap, growing gap count by 1.
        for i in 0u64..11 {
            mr.mark_served(i * 2, i * 2 + 1);
        }

        assert!(mr.is_capped(), "fragmentation cap should be triggered");
        assert!(!mr.is_complete(), "fallback must not report complete");

        // Per-chunk or partial-response progress cannot recover discarded state.
        mr.mark_served(0, 100);
        mr.mark_contiguous_response_complete(0, 99);
        assert!(!mr.is_complete(), "partial recovery must stay incomplete");

        // One successfully completed contiguous full response is exact evidence.
        mr.mark_contiguous_response_complete(0, 100);
        assert!(
            mr.is_complete(),
            "full response must recover exact coverage"
        );
        assert!(!mr.is_capped());
    }

    // -----------------------------------------------------------------------
    // Pending ranges (served before initialize)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pending_ranges_applied_on_init() {
        let mut mr = MissingRanges::new(1024);
        mr.mark_served(0, 50); // buffered to pending
        mr.initialize(100); // replays pending → gap [50, 100)
        mr.mark_served(50, 100);
        assert!(mr.is_complete());
    }

    #[test]
    fn test_pending_ranges_cover_full_on_init() {
        let mut mr = MissingRanges::new(1024);
        mr.mark_served(0, 100); // buffered to pending
        mr.initialize(100); // pending covers everything → complete immediately
        assert!(
            mr.is_complete(),
            "pending range covering the full object should make it complete after init"
        );
    }

    #[test]
    fn test_sequential_pending_ranges_coalesce_before_cap() {
        const PAGE_SIZE: u64 = 4096;
        const PAGES: u64 = 1025;
        let mut mr = MissingRanges::new(1024);

        for page in 0..PAGES {
            let start = page * PAGE_SIZE;
            mr.mark_served(start, start + PAGE_SIZE);
        }

        assert!(
            !mr.is_capped(),
            "sequential follow progress must not overflow"
        );
        assert_eq!(
            mr.pending.len(),
            1,
            "adjacent pages must remain one interval"
        );
        mr.initialize(PAGES * PAGE_SIZE);
        assert!(
            mr.is_complete(),
            "all followed pages cover the completed object"
        );
    }

    // -----------------------------------------------------------------------
    // No-op edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_range_is_noop() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        let before = mr.gap_count();
        mr.mark_served(5, 5); // start == end → no-op
        assert_eq!(mr.gap_count(), before);
        assert!(!mr.is_complete());
    }

    #[test]
    fn test_inverted_range_is_noop() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        let before = mr.gap_count();
        mr.mark_served(10, 5); // start > end → no-op
        assert_eq!(mr.gap_count(), before);
        assert!(!mr.is_complete());
    }

    // -----------------------------------------------------------------------
    // Double initialize
    // -----------------------------------------------------------------------

    #[test]
    fn test_double_initialize_is_idempotent() {
        let mut mr = MissingRanges::new(1024);
        mr.initialize(100);
        assert_eq!(mr.gap_count(), 1);

        // Second call must be ignored — no duplicate gaps.
        mr.initialize(100);
        assert_eq!(
            mr.gap_count(),
            1,
            "second initialize() must not add extra gaps"
        );

        mr.mark_served(0, 100);
        assert!(mr.is_complete());
    }

    // -----------------------------------------------------------------------
    // Pending cap triggers fallback before initialize
    // -----------------------------------------------------------------------

    #[test]
    fn test_pending_cap_triggers_fallback() {
        // cap=3 → adding a 4th pending range before init should set capped=true.
        let mut mr = MissingRanges::new(3);
        assert!(!mr.is_capped());

        mr.mark_served(0, 10);
        mr.mark_served(20, 30);
        mr.mark_served(40, 50);
        assert!(
            !mr.is_capped(),
            "3 pending ranges is exactly at cap, not over"
        );

        mr.mark_served(60, 70); // 4th range → pending.len() == 4 > cap=3 → capped
        assert!(
            mr.is_capped(),
            "4 pending ranges should exceed cap=3 and set capped=true"
        );

        // Even after initialize, capped state persists.
        mr.initialize(100);
        assert!(
            !mr.is_complete(),
            "capped tracker must not complete even after initialize"
        );
    }
}
