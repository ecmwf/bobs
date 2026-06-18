use bytes::Bytes;
use std::collections::VecDeque;

/// Bounded FIFO page cache for a single spool.
/// NOT thread-safe — caller must wrap in Mutex.
/// Uses FIFO eviction (correct for sequential spool access patterns).
pub struct PageCache {
    capacity: usize,
    // (page_idx, data) pairs in insertion order
    entries: VecDeque<(u64, Bytes)>,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        }
    }

    /// Insert a page. If at capacity, evicts the oldest entry (FIFO).
    pub fn insert(&mut self, page_idx: u64, data: Bytes) {
        // Remove existing entry for this page_idx if present
        if let Some(pos) = self.entries.iter().position(|(idx, _)| *idx == page_idx) {
            self.entries.remove(pos);
        }
        // Evict oldest if at capacity
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((page_idx, data));
    }

    /// Get a page by index. Returns None if not cached.
    pub fn get(&self, page_idx: u64) -> Option<Bytes> {
        self.entries
            .iter()
            .find(|(idx, _)| *idx == page_idx)
            .map(|(_, data)| data.clone())
    }

    pub fn contains(&self, page_idx: u64) -> bool {
        self.entries.iter().any(|(idx, _)| *idx == page_idx)
    }

    /// Drop all cached pages, releasing their memory. Used once a spool has been
    /// fully read: the cache only serves the first read of freshly-written,
    /// not-yet-served data, so after full coverage it is pure overhead.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.entries.shrink_to_fit();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut cache = PageCache::new(4);
        let data = Bytes::from_static(b"hello page");
        cache.insert(0, data.clone());
        assert_eq!(cache.get(0), Some(data));
        assert_eq!(cache.get(1), None);
    }

    #[test]
    fn test_eviction_fifo() {
        let mut cache = PageCache::new(3);
        cache.insert(0, Bytes::from_static(b"page0"));
        cache.insert(1, Bytes::from_static(b"page1"));
        cache.insert(2, Bytes::from_static(b"page2"));
        assert_eq!(cache.len(), 3);
        // Insert 4th — should evict page 0 (oldest)
        cache.insert(3, Bytes::from_static(b"page3"));
        assert_eq!(cache.len(), 3);
        assert!(cache.get(0).is_none(), "page 0 should be evicted");
        assert!(cache.get(3).is_some(), "page 3 should be present");
    }

    #[test]
    fn test_capacity_bound() {
        let mut cache = PageCache::new(2);
        for i in 0..10u64 {
            cache.insert(i, Bytes::from(vec![i as u8; 4096]));
        }
        assert_eq!(cache.len(), 2);
        // Only last 2 pages should be present
        assert!(cache.get(8).is_some());
        assert!(cache.get(9).is_some());
        assert!(cache.get(0).is_none());
    }

    #[test]
    fn test_clear_releases_all_pages() {
        let mut cache = PageCache::new(4);
        cache.insert(0, Bytes::from_static(b"page0"));
        cache.insert(1, Bytes::from_static(b"page1"));
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert!(cache.get(0).is_none());
        // Cache remains usable after clear.
        cache.insert(2, Bytes::from_static(b"page2"));
        assert_eq!(cache.get(2), Some(Bytes::from_static(b"page2")));
    }

    #[test]
    fn test_update_existing_page() {
        let mut cache = PageCache::new(4);
        cache.insert(0, Bytes::from_static(b"original"));
        cache.insert(0, Bytes::from_static(b"updated"));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(0), Some(Bytes::from_static(b"updated")));
    }
}
