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
/// clients. Once the number of distinct gaps exceeds `fragmentation_cap`,
/// `capped` is set and all further tracking is disabled. `is_complete()`
/// stays `false` in the capped state — a safe false-negative that lets the
/// idle TTL handle cleanup instead.
///
/// Ranges served before `initialize()` is called are buffered in `pending`
/// and replayed once `total_size` is known.
pub struct MissingRanges {
    /// Sorted non-overlapping half-open intervals `[gap_start, gap_end)` not yet served.
    /// Empty AND `total_size` is `Some` → full coverage achieved.
    gaps: BTreeMap<u64, u64>,
    /// `None` until `initialize()` is called.
    pub total_size: Option<u64>,
    /// Maximum number of distinct gaps allowed before capping.
    fragmentation_cap: usize,
    /// `true` when the fragmentation cap was exceeded; `is_complete()` is always `false`.
    capped: bool,
    /// Ranges served before `total_size` was known, buffered for replay at `initialize()`.
    pending: Vec<(u64, u64)>,
}

impl MissingRanges {
    /// Create a new tracker with the given fragmentation cap.
    ///
    /// `fragmentation_cap` sets the maximum number of distinct gaps that may
    /// be tracked simultaneously. A value of 1024 is a safe default for any
    /// realistic HTTP Range access pattern.
    pub fn new(fragmentation_cap: usize) -> Self {
        Self {
            gaps: BTreeMap::new(),
            total_size: None,
            fragmentation_cap,
            capped: false,
            pending: Vec::new(),
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
            // Zero-byte object is immediately complete; leave gaps empty.
            self.pending.clear();
            return;
        }
        self.gaps.insert(0, total_size);
        let pending = mem::take(&mut self.pending);
        for (s, e) in pending {
            self.apply(s, e);
        }
    }

    /// Record that bytes `[start, end)` have been served.
    ///
    /// - If `start >= end` this is a no-op.
    /// - If `total_size` is not yet known the range is buffered in `pending`
    ///   and applied when `initialize()` is called.
    /// - Once `capped` is `true` all calls are no-ops.
    pub fn mark_served(&mut self, start: u64, end: u64) {
        if self.capped || start >= end {
            return;
        }
        if self.total_size.is_none() {
            self.pending.push((start, end));
            if self.pending.len() > self.fragmentation_cap {
                self.capped = true;
                self.pending.clear();
            }
            return;
        }
        self.apply(start, end);
    }

    /// Returns `true` when every byte `[0, total_size)` has been served.
    ///
    /// Always `false` if `total_size` is unknown or if the fragmentation cap
    /// was exceeded (safe false-negative).
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
            .filter(|(_, &ge)| ge > start)
            .map(|(&gs, &ge)| (gs, ge))
            .collect();

        for (gs, ge) in overlapping {
            self.gaps.remove(&gs);
            // Preserve left portion of gap not covered by served range.
            if gs < start {
                self.gaps.insert(gs, start);
            }
            // Preserve right portion of gap not covered by served range.
            if ge > end {
                self.gaps.insert(end, ge);
            }
        }

        // Enforce fragmentation cap.
        if self.gaps.len() > self.fragmentation_cap {
            self.capped = true;
            self.gaps.clear();
        }
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
    fn test_fragmentation_cap_triggers_fallback() {
        // Use a small cap so we can trigger it within total_size=100.
        let mut mr = MissingRanges::new(10);
        mr.initialize(100);

        // Serve non-adjacent 1-byte ranges: [0,1), [2,3), [4,5), ...
        // Each one splits the rightmost gap, growing gap count by 1.
        // After 11 such ranges the gap count exceeds cap=10 → capped.
        for i in 0u64..11 {
            mr.mark_served(i * 2, i * 2 + 1);
        }

        assert!(
            mr.is_capped(),
            "fragmentation cap should have been triggered"
        );
        assert!(!mr.is_complete(), "capped tracker must not report complete");

        // Serving the rest of the object still cannot flip is_complete.
        mr.mark_served(0, 100);
        assert!(!mr.is_complete(), "is_complete must stay false when capped");
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
