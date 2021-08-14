//!
//! The PerfCounters module contains everything associated with the "perf_counters" feature
//! 

/// Performance counters for optimizing [Table] parameters.
/// 
/// These counters don't reflect stats and totals across the whole database, rather they can
/// be reset and therefore used to measure individual operations or sequences of operations.
/// 
/// NOTE: Counters are being implemented on an as-needed basis
#[cfg(feature = "perf_counters")]
use core::cell::Cell;

/// All of the performance counters to measure and tune the behavior of the system
/// 
/// NOTE: In order to get valid data, you must enable the `perf_counters` feature in the `Cargo.toml` file
/// with an entry similar to this:
/// 
/// ```toml
/// [dependencies]
/// fuzzy_rocks = { version = "0.2.0", features = ["perf_counters"] }
/// ```
#[derive(Debug, Copy, Clone)]
pub struct PerfCounterFields {

    /// The number of times the DB was queried for a variant during a fuzzy lookup
    /// 
    /// This is based on the number of variants created from the query keys
    pub variant_lookup_count : usize,

    /// The number of times a variant entry was sucessfully loaded from the DB during a fuzzy lookup
    /// 
    /// This is based on how much overlap there is between the variants in the database and the variants
    /// generated from the keys
    pub variant_load_count : usize,

    /// The total number of references to key groups, across all loaded variant entries
    /// 
    /// Each loaded variant entry may contain references to multiple key groups.  A higher value for this
    /// counter means variant entries tend to reference more key groups.
    /// 
    /// The average number of key group references per variant entry is: `key_group_ref_count / variant_load_count`
    pub key_group_ref_count : usize,

    /// The maximum number of key_group references encountered in a single variant entry, among all variant
    /// entries loaded during all lookups
    pub max_variant_entry_refs : usize,

    /// The number of time a key_group was loaded from the DB
    /// 
    /// Many key group references will be common across many variants, so this number reflects the number
    /// of unique key groups loaded from the DB.
    pub key_group_load_count : usize,

    /// The number of keys found across all key groups loaded during fuzzy lookups
    pub keys_found_count : usize,

    /// The number of times the distance function is invoked to compare two keys during fuzzy lookups
    /// 
    /// The current implementation tests all keys in a key group, so this value will exactly match
    /// [keys_found_count](Self::keys_found_count).  In the future, this will be optimized.
    pub distance_function_invocation_count : usize,

    /// The number of unique records that were found with fuzzy lookups
    /// 
    /// This counter include doesn't include records that were rejected because of a distance threshold,
    /// but does include records that didn't qualify as the "best" distance in [lookup_best](crate::Table::lookup_best).
    /// 
    /// The number of keys found per record can be calculated by: [keys_found_count](Self::keys_found_count) / [records_found_count](Self::records_found_count).
    /// Ideally we would only find one key for the record we are looking for although we must test all found
    /// keys to find the shortest distance.  Adjusting the [group_variant_overlap_threshold](crate::TableConfig::GROUP_VARIANT_OVERLAP_THRESHOLD) is one way to
    /// lower this ratio, although it may hurt performance in other ways.
    pub records_found_count : usize,
}

impl PerfCounterFields {
    pub fn new() -> Self {
        Self {
            variant_lookup_count : 0,
            variant_load_count : 0,
            key_group_ref_count : 0,
            max_variant_entry_refs : 0,
            key_group_load_count : 0,
            keys_found_count : 0,
            distance_function_invocation_count : 0,
            records_found_count : 0,
        }
    }
}

#[cfg(feature = "perf_counters")]
pub struct PerfCounters(Cell<PerfCounterFields>);

#[cfg(feature = "perf_counters")]
impl PerfCounters {
    pub fn new() -> Self {
        Self(Cell::new(PerfCounterFields::new()))
    }
    pub fn reset(&self) {
        self.set(PerfCounterFields::new())
    }
    pub fn update<F : Fn(&mut PerfCounterFields)>(&self, func : F) {
        let mut fields = self.get();
        func(&mut fields);
        self.set(fields);
    }
    pub fn get(&self) -> PerfCounterFields {
        self.0.get()
    }
    pub fn set(&self, fields : PerfCounterFields) {
        self.0.set(fields);
    }
}

#[cfg(not(feature = "perf_counters"))]
pub struct PerfCounters();

#[cfg(not(feature = "perf_counters"))]
impl PerfCounters {
    pub fn new() -> Self {
        Self()
    }
    pub fn reset(&self) {
    }
    pub fn get(&self) -> PerfCounterFields {
        PerfCounterFields::new()
    }
}


