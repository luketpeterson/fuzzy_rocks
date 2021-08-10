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

#[cfg(feature = "perf_counters")]
#[derive(Debug, Copy, Clone)]
pub struct PerfCounterFields {

    /// The number of times we query the DB for a variant during a fuzzy lookup
    /// 
    /// This is based on the number of variants created from the query keys
    pub variant_lookup_count : usize,

    /// The number of times we sucessfully load a variant entry from the DB during a fuzzy lookup
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

    /// The number of time we load a key_group from the DB
    /// 
    /// Many key group references will be common across many variants, so this number reflects the number
    /// of unique key groups we load from the DB.
    pub key_group_load_count : usize,

    /// The number of keys we find across all key groups that we loaded during fuzzy lookups
    pub keys_found_count : usize,

    /// The number of times the distance function is invoked to compare two keys during fuzzy lookups
    pub distance_function_invocation_count : usize,

    /// The number of unique records that we found with fuzzy lookups
    /// 
    /// This counter include doesn't include records that were rejected because of a distance threshold,
    /// but does include records that didn't qualify as the "best" distance in [lookup_best].
    pub records_found_count : usize,
}

#[cfg(feature = "perf_counters")]
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
}


