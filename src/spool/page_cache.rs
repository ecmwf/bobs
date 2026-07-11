// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    spool_key: String,
    page_idx: u64,
}

impl CacheKey {
    fn new(spool_key: &str, page_idx: u64) -> Self {
        Self {
            spool_key: spool_key.to_string(),
            page_idx,
        }
    }
}

/// Shared bounded FIFO page cache for all spools.
/// NOT thread-safe — caller must wrap in Mutex.
/// Uses FIFO eviction (correct for sequential spool access patterns).
pub struct PageCache {
    max_bytes: usize,
    current_bytes: usize,
    entries: HashMap<CacheKey, Bytes>,
    eviction_order: VecDeque<CacheKey>,
}

impl PageCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: 0,
            entries: HashMap::new(),
            eviction_order: VecDeque::new(),
        }
    }

    /// Insert a page for a spool. If needed, evicts the oldest entries (FIFO)
    /// until the total cached bytes are within `max_bytes`.
    ///
    /// A max size of zero disables caching. Pages larger than `max_bytes` are
    /// never cached; if such a page updates an existing key, the old cached
    /// value is removed.
    pub fn insert(&mut self, spool_key: &str, page_idx: u64, data: Bytes) {
        let key = CacheKey::new(spool_key, page_idx);

        if let Some(old) = self.entries.remove(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(old.len());
            self.eviction_order.retain(|queued| queued != &key);
        }

        if self.max_bytes == 0 || data.len() > self.max_bytes {
            return;
        }

        self.current_bytes += data.len();
        self.entries.insert(key.clone(), data);
        self.eviction_order.push_back(key);

        self.evict_to_limit();
    }

    /// Get a page by spool key and page index. Returns None if not cached.
    pub fn get(&self, spool_key: &str, page_idx: u64) -> Option<Bytes> {
        self.entries
            .get(&CacheKey::new(spool_key, page_idx))
            .cloned()
    }

    /// Drop all pages for one spool while preserving other spools' cached pages.
    /// Used when the spool's object has been fully read and its first-read cache
    /// is no longer useful.
    pub fn free_spool(&mut self, spool_key: &str) {
        let mut removed_bytes = 0usize;
        self.entries.retain(|key, data| {
            if key.spool_key == spool_key {
                removed_bytes += data.len();
                false
            } else {
                true
            }
        });
        self.current_bytes = self.current_bytes.saturating_sub(removed_bytes);
        self.eviction_order
            .retain(|key| key.spool_key.as_str() != spool_key);
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn contains(&self, spool_key: &str, page_idx: u64) -> bool {
        self.entries
            .contains_key(&CacheKey::new(spool_key, page_idx))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn evict_to_limit(&mut self) {
        while self.current_bytes > self.max_bytes {
            let Some(oldest) = self.eviction_order.pop_front() else {
                self.current_bytes = 0;
                break;
            };

            if let Some(data) = self.entries.remove(&oldest) {
                self.current_bytes = self.current_bytes.saturating_sub(data.len());
            }
        }
    }

    #[cfg(test)]
    pub fn cached_keys(&self) -> Vec<(String, u64)> {
        self.eviction_order
            .iter()
            .filter(|key| self.entries.contains_key(*key))
            .map(|key| (key.spool_key.clone(), key.page_idx))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_and_assert_bound(cache: &mut PageCache, spool_key: &str, page_idx: u64, data: Bytes) {
        cache.insert(spool_key, page_idx, data);
        assert!(
            cache.current_bytes() <= cache.max_bytes(),
            "cache exceeded byte cap after inserting ({spool_key}, {page_idx}): {} > {}",
            cache.current_bytes(),
            cache.max_bytes()
        );
    }

    #[test]
    fn test_insert_and_get_by_spool_key_and_page_index() {
        let mut cache = PageCache::new(1024);
        let data = Bytes::from_static(b"hello page");
        insert_and_assert_bound(&mut cache, "spool-a", 0, data.clone());

        assert_eq!(cache.current_bytes(), data.len());
        assert_eq!(cache.get("spool-a", 0), Some(data));
        assert_eq!(cache.get("spool-a", 1), None);
        assert_eq!(cache.get("spool-b", 0), None);
    }

    #[test]
    fn test_byte_capacity_accounting_evicts_fifo_across_spools() {
        let mut cache = PageCache::new(15);
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"aaaaa"));
        insert_and_assert_bound(&mut cache, "b", 0, Bytes::from_static(b"bbbbb"));
        insert_and_assert_bound(&mut cache, "a", 1, Bytes::from_static(b"ccccc"));
        assert_eq!(cache.current_bytes(), 15);
        assert_eq!(
            cache.cached_keys(),
            vec![
                ("a".to_string(), 0),
                ("b".to_string(), 0),
                ("a".to_string(), 1),
            ]
        );

        insert_and_assert_bound(&mut cache, "b", 1, Bytes::from_static(b"ddddd"));
        assert_eq!(cache.current_bytes(), 15);
        assert!(cache.get("a", 0).is_none(), "oldest page should be evicted");
        assert!(
            cache.get("b", 0).is_some(),
            "other spool pages compete under same cap"
        );
        assert!(
            cache.get("a", 1).is_some(),
            "later page from evicted spool remains"
        );
        assert!(cache.get("b", 1).is_some(), "newest page should be present");
        assert_eq!(
            cache.cached_keys(),
            vec![
                ("b".to_string(), 0),
                ("a".to_string(), 1),
                ("b".to_string(), 1)
            ]
        );
    }

    #[test]
    fn test_zero_capacity_disables_cache() {
        let mut cache = PageCache::new(0);
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"page0"));
        insert_and_assert_bound(&mut cache, "b", 0, Bytes::from_static(b"page1"));

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
        assert!(cache.is_empty());
        assert!(cache.get("a", 0).is_none());
        assert!(cache.get("b", 0).is_none());
    }

    #[test]
    fn test_byte_capacity_bound_after_every_insert() {
        let mut cache = PageCache::new(8192);
        for i in 0..10u64 {
            let spool_key = if i % 2 == 0 { "even" } else { "odd" };
            insert_and_assert_bound(&mut cache, spool_key, i, Bytes::from(vec![i as u8; 4096]));
        }
        assert_eq!(cache.len(), 2);
        assert!(cache.get("even", 8).is_some());
        assert!(cache.get("odd", 9).is_some());
        assert!(cache.get("even", 0).is_none());
        assert!(cache.get("odd", 1).is_none());
    }

    #[test]
    fn test_update_existing_page_adjusts_bytes_and_fifo_position() {
        let mut cache = PageCache::new(20);
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"aaaa"));
        insert_and_assert_bound(&mut cache, "b", 0, Bytes::from_static(b"bbbb"));
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"updated"));

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.current_bytes(), 11);
        assert_eq!(cache.get("a", 0), Some(Bytes::from_static(b"updated")));
        assert_eq!(
            cache.cached_keys(),
            vec![("b".to_string(), 0), ("a".to_string(), 0)]
        );

        insert_and_assert_bound(&mut cache, "c", 0, Bytes::from_static(b"cccccccccc"));
        assert!(
            !cache.contains("b", 0),
            "oldest remaining key should evict first"
        );
        assert!(
            cache.contains("a", 0),
            "updated key should be treated as newest"
        );
        assert!(cache.contains("c", 0));
    }

    #[test]
    fn test_oversized_page_is_not_cached_and_removes_existing() {
        let mut cache = PageCache::new(4);
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"1234"));
        assert!(cache.contains("a", 0));
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"12345"));
        assert!(!cache.contains("a", 0));
        assert_eq!(cache.current_bytes(), 0);
    }

    #[test]
    fn test_eviction_triggered_by_insert_from_any_spool() {
        let mut cache = PageCache::new(10);
        insert_and_assert_bound(&mut cache, "hot", 0, Bytes::from_static(b"1111"));
        insert_and_assert_bound(&mut cache, "hot", 1, Bytes::from_static(b"2222"));
        assert_eq!(cache.current_bytes(), 8);

        insert_and_assert_bound(&mut cache, "cold", 0, Bytes::from_static(b"3333"));
        assert!(
            !cache.contains("hot", 0),
            "cold spool insert should evict global oldest"
        );
        assert!(cache.contains("hot", 1));
        assert!(cache.contains("cold", 0));
        assert_eq!(cache.current_bytes(), 8);
    }

    #[test]
    fn test_free_spool_drops_only_that_spool_and_preserves_accounting() {
        let mut cache = PageCache::new(100);
        insert_and_assert_bound(&mut cache, "a", 0, Bytes::from_static(b"aaaa"));
        insert_and_assert_bound(&mut cache, "b", 0, Bytes::from_static(b"bbbb"));
        insert_and_assert_bound(&mut cache, "a", 1, Bytes::from_static(b"cccc"));

        cache.free_spool("a");

        assert_eq!(cache.current_bytes(), 4);
        assert!(!cache.contains("a", 0));
        assert!(!cache.contains("a", 1));
        assert!(cache.contains("b", 0));
        assert_eq!(cache.cached_keys(), vec![("b".to_string(), 0)]);
    }

    #[test]
    fn test_interleaved_hot_and_cold_spools_follow_fifo_not_access_recency() {
        let mut cache = PageCache::new(12);
        insert_and_assert_bound(&mut cache, "hot", 0, Bytes::from_static(b"hhhh"));
        insert_and_assert_bound(&mut cache, "cold", 0, Bytes::from_static(b"cccc"));
        insert_and_assert_bound(&mut cache, "hot", 1, Bytes::from_static(b"HHHH"));
        assert_eq!(cache.current_bytes(), 12);

        assert!(
            cache.get("hot", 0).is_some(),
            "reads must not refresh FIFO order"
        );
        assert!(
            cache.get("hot", 0).is_some(),
            "repeated hot reads still do not refresh FIFO order"
        );

        insert_and_assert_bound(&mut cache, "cold", 1, Bytes::from_static(b"CCCC"));
        assert!(
            !cache.contains("hot", 0),
            "FIFO evicts oldest even if it was just read"
        );
        assert!(cache.contains("cold", 0));
        assert!(cache.contains("hot", 1));
        assert!(cache.contains("cold", 1));
        assert_eq!(
            cache.cached_keys(),
            vec![
                ("cold".to_string(), 0),
                ("hot".to_string(), 1),
                ("cold".to_string(), 1),
            ]
        );
    }
}
