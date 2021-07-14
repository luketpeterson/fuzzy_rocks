
//! # fuzzy_rocks Overview
//! 
//! A persistent datastore backed by [RocksDB](https://rocksdb.org) with fuzzy key lookup using an arbitrary
//! distance function accelerated by the [SymSpell](https://github.com/wolfgarbe/SymSpell) algorithm.
//! 
//! The reasons to use this crate over another SymSpell implementation are:
//! - You need to use a custom distance function
//! - Startup time matters more than lookups-per-second
//! - You care about resident memory footprint
//! 
//! ## Records
//! 
//! This crate manages records, each of which has a unique [RecordID].  Keys are used to perform fuzzy
//! lookups but keys are not guaranteed to be unique. [Insert](Table::insert)ing the same key into a [Table] twice
//! will result in two distinct records.
//! 
//! ## Usage Example
//! 
//! ```
//! use fuzzy_rocks::{*};
//! 
//! //Create and reset the FuzzyRocks Table
//! let mut table = Table::<String, 2, 8, true>::new("test.rocks").unwrap();
//! table.reset().unwrap();
//!
//! //Insert some records
//! let thu = table.insert("Thursday", &"Mokuyoubi".to_string()).unwrap();
//! let wed = table.insert("Wednesday", &"Suiyoubi".to_string()).unwrap();
//! let tue = table.insert("Tuesday", &"Kayoubi".to_string()).unwrap();
//! let mon = table.insert("Monday", &"Getsuyoubi".to_string()).unwrap();
//! 
//! //Try out lookup_best, to get the closest fuzzy match
//! let result = table.lookup_best("Bonday", table.default_distance_func()).unwrap();
//! assert_eq!(result, mon);
//! 
//! //Try out lookup_fuzzy, to get all matches and their distances
//! let results : Vec<(RecordID, u64)> = table
//!     .lookup_fuzzy("Tuesday", table.default_distance_func(), 2)
//!     .unwrap().collect();
//! assert_eq!(results.len(), 2);
//! assert!(results.contains(&(tue, 0))); //Tuesday -> Tuesday with 0 edits
//! assert!(results.contains(&(thu, 2))); //Thursday -> Tuesday with 2 edits
//! 
//! //Retrieve the key and value from a record
//! assert_eq!(table.get_one_key(wed).unwrap(), "Wednesday");
//! assert_eq!(table.get_value(wed).unwrap(), "Suiyoubi");
//! ```
//! 
//! Additional usage examples can be found in the tests, located at the bottom of the `src/lib.rs` file.
//! 
//! ## Distance Functions
//! 
//! A distance function is any function that returns a scalar distance between two keys.  The smaller the
//! distance, the closer the match.  Two identical keys must have a distance of [zero](num_traits::Zero).  The `fuzzy` methods
//! in this crate, such as [lookup_fuzzy](Table::lookup_fuzzy), require a distance function to be supplied.
//! 
//! This crate includes a simple [Levenstein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) function
//! called [edit_distance](Table::edit_distance).  However, you may often want to use a different function.
//! 
//! One reason to use a custom distance function is to account for expected variation patterns. For example:
//! a distance function that considers likely [OCR](https://en.wikipedia.org/wiki/Optical_character_recognition)
//! errors might consider 'lo' to be very close to 'b', '0' to be extremely close to 'O', and 'A' to be
//! somewhat near to '^', while '#' would be much further from ',' even though the Levenstein distances
//! tell a different story with 'lo' being two edits away from 'b' and '#' being only one edit away from
//! ',' (comma).
//! 
//! You may have a different distance function to catch common typos on a QWERTY keyboard, etc.
//! 
//! Another reason for a custom distance function is if your keys are not human-readable strings, in which
//! case you may need a different interpretation of variances between keys.  For example DNA snippets could
//! be used as keys.
//! 
//! Any distance function you choose must be compatible with SymSpell's delete-distance optimization.  In other
//! words, you must be able to delete no more than `MAX_DELETES` characters from each of the record's
//! key and the lookup key and arrive at identical key-variants.  If your distance function is incompatible
//! with this property then the SymSpell optimization won't work for you and you should use a different fuzzy
//! lookup technique and a different crate.
//! 
//! Distance functions may return any scalar type, so floating point distances will work.  However, the
//! `MAX_DELETES` constant is an integer.  Records that can't be reached by deleting `MAX_DELETES` characters
//! from both the record key and the lookup key will never be evaluated by the distance function and are
//! conceptually "too far away".  Once the distance function has been evaluated, its return value is
//! considered the authoritative distance and the delete distance is irrelevant.
//! 
//! ## Unicode Support
//! 
//! A [Table] may allow for unicode keys or not, depending on the value of the `UNICODE_KEYS` constant used
//! when the Table was created.
//! 
//! If `UNICODE_KEYS` is `true`, keys may use unicode characters and multi-byte characters will still be
//! considered as single characters for the purpose of deleting characters to create key variants.
//! 
//! If `UNICODE_KEYS` is `false`, keys are just strings of [u8] characters.
//! This option has better performance.
//! 
//! ## Performance Characteristics
//! 
//! This crate is designed for large databases where startup time and resident memory footprint are significant
//! considerations.  This create has been tested with 200,000 records cumulatively having over 1 million keys,
//! and about 140 million key variants.  In this situation, a fuzzy lookup was about 500 microseconds running
//! on my laptop - which is very expensive in absolute terms.
//! 
//! The performance will also vary greatly depending on the key distribution and the table parameters.  Keys
//! that are distinct from eachother will lead to faster searches vs. keys that share many variants in common.
//! 
//! A smaller `MAX_DELETES` value will perform better but be able to find fewer results for a search.
//! 
//! A higher value for `MEANINGFUL_KEY_LEN` will result in fewer wasted evaluations of the distance function
//! but will lead to more entries in the variants database and thus more memory pressure.
//! 
//! If your use-case can cope with a higher startup latency and you are ok with all of your keys and
//! variants being loaded into memory, then query performance will certainly be better using a solution
//! built on Rust's native collections, such as this [symspell](https://crates.io/crates/symspell)
//! crate on [crates.io](http://crates.io).
//! 
//! ## Misc
//! 
//! **NOTE**: The included `geonames_megacities.txt` file is a stub for the `geonames_test`, designed to stress-test
//! this crate.  The abriged file is included so the test will pass regardless, and to avoid bloating the
//! download.  The content of `geonames_megacities.txt` was derived from data on [geonames.org](http://geonames.org),
//! and licensed under a [Creative Commons Attribution 4.0 License](https://creativecommons.org/licenses/by/4.0/legalcode)
//! 

use core::marker::PhantomData;
use core::borrow::Borrow;
use core::cmp::min;
use core::hash::Hash;

use num_traits::Zero;

use serde::{Serialize, Deserialize};
use bincode::Options;

use std::collections::HashSet;
use std::convert::TryInto;
use std::iter::FromIterator;

use rocksdb::{DB, DBWithThreadMode, ColumnFamily, ColumnFamilyDescriptor, MergeOperands};

/// A collection containing records that may be searched by `key`
/// 
/// -`MAX_DELETES` is the number of deletes to store in the database for variants created
/// by the SymSpell optimization.  If `MAX_DELETES` is too small, the variant will not be found
/// and therefore the `distance_function` will not have an opportunity to evaluate the match.  However,
/// if `MAX_DELETES` is too large, it will hurt performance by evaluating too many candidates.
/// 
/// Empirically, values near 2 seem to be good in most situations I have found.  I.e. 1 and 3 might be
/// appropriate sometimes.  4 ends up exploding in most cases I've seen so the SymSpell logic may not
/// be a good fit if you need to find keys 4 edits away.  0 edits is an exact match.
/// 
/// -`MEANINGFUL_KEY_LEN` is an optimization where only a subset of the key is used for creating
/// variants.  So, if `MEANINGFUL_KEY_LEN = 10` then only the first 10 characters of the key will be used
/// to generate and search for variants.
/// 
/// This optimization is predicated on the idea that long key strings will not be very similar to each
/// other.  For example the key *incomprehensibilities* will cause variants to be generated for
///  *incomprehe*, meaning that a search for *incomprehension* would find *incomprehensibilities*
///  and evauate it with the `distance_function` even though it is further than `MAX_DELETES`.
/// 
/// In a dataset where many keys share a common prefix, or where keys are organized into a namespace by
/// concatenating strings, this optimization will cause problems and you should either pass a high number
/// to effectively disable it, or rework this code to use different logic to select the substring
/// 
/// -`UNICODE_KEYS` specifies whether the keys are UTF-8 encoded strings or not.  UFT-8 awareness is
/// required to avoid deleting partial characters thus rendering the string invalid.  This comes at a
/// performance cost, however, so passing `false` is more efficient if you plan to use regular ascii or
/// any other kind of data as the table's keys.
/// 
pub struct Table<ValueT : Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool> {
    record_count : usize,
    db : DBWithThreadMode<rocksdb::SingleThreaded>,
    path : String,
    deleted_records : Vec<RecordID>, //NOTE: Currently we don't try to hold onto deleted records across unloads, but we may change this in the future.
    phantom: PhantomData<ValueT>,
}

/// A unique identifier for a record within a [Table]
#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, derive_more::Display, Serialize, Deserialize)]
pub struct RecordID(usize);
impl RecordID {
    pub const NULL : RecordID = RecordID(usize::MAX);
}

const KEYS_CF_NAME : &str = "keys";
const VALUES_CF_NAME : &str = "values";
const VARIANTS_CF_NAME : &str = "variants";

impl <ValueT : 'static + Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool>Table<ValueT, MAX_DELETES, MEANINGFUL_KEY_LEN, UNICODE_KEYS> {

    /// Creates a new Table, backed by the database at the path provided
    /// 
    /// WARNING:  No sanity checks are performed to ensure the database being opened matches the parameters
    /// of the table being created.  Therefore you may see bugs if you are opening a table that was created
    /// using a different set of parameters.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn new(path : &str) -> Result<Self, String> {

        //Configure the "keys" and "values" column families
        let keys_cf = ColumnFamilyDescriptor::new(KEYS_CF_NAME, rocksdb::Options::default());
        let values_cf = ColumnFamilyDescriptor::new(VALUES_CF_NAME, rocksdb::Options::default());

        //Configure the "variants" column family
        let mut variants_opts = rocksdb::Options::default();
        variants_opts.create_if_missing(true);
        variants_opts.set_merge_operator_associative("append to RecordID vec", Self::variant_append_merge);
        let variants_cf = ColumnFamilyDescriptor::new(VARIANTS_CF_NAME, variants_opts);

        //Configure the database itself
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        //Open the database
        let db = DB::open_cf_descriptors(&db_opts, path, vec![keys_cf, values_cf, variants_cf])?;

        //Find the maximum RecordID, by probing the entries in the "keys" column family
        let keys_cf_handle = db.cf_handle(KEYS_CF_NAME).unwrap();
        let record_count = probe_for_max_sequential_key(&db, keys_cf_handle, 255)?;

        Ok(Self {
            record_count,
            db,
            path : path.to_string(),
            deleted_records : vec![],
            phantom : PhantomData
        })
    }

    /// Resets a Table, dropping every record in the table and restoring it to an empty state.
    /// 
    /// (Dropping in a database sense, not a Rust sense)
    pub fn reset(&mut self) -> Result<(), String> {
        
        //Drop both the "records" and the "variants" column families
        self.db.drop_cf(KEYS_CF_NAME)?;
        self.db.drop_cf(VALUES_CF_NAME)?;
        self.db.drop_cf(VARIANTS_CF_NAME)?;

        //Recreate the "keys" and "values" column families
        self.db.create_cf(KEYS_CF_NAME, &rocksdb::Options::default())?;
        self.db.create_cf(VALUES_CF_NAME, &rocksdb::Options::default())?;
        
        //Recreate the "variants" column family
        let mut variants_opts = rocksdb::Options::default();
        variants_opts.create_if_missing(true);
        variants_opts.set_merge_operator_associative("append to RecordID vec", Self::variant_append_merge);
        self.db.create_cf(VARIANTS_CF_NAME, &variants_opts)?;

        //Reset the record_count, so newly inserted entries begin at 0 again
        self.record_count = 0;
        Ok(())
    }

    /// Deletes a record from the Table.
    /// 
    /// A deleted record cannot be accessed or otherwise found, but the RecordID may be reassigned
    /// using [Table::replace].
    pub fn delete(&mut self, record_id : RecordID) -> Result<(), String> {

        self.delete_keys_internal(record_id)?;
        self.delete_value_internal(record_id)?;
        self.deleted_records.push(record_id);

        Ok(())
    }

    /// Deletes the keys belonging to a record, and all associated variants
    /// 
    /// Leaves the record in a half-composed state, so should only be called as part of another
    /// operation.
    fn delete_keys_internal(&mut self, record_id : RecordID) -> Result<(), String> {

        //Get all the keys for the record we're removing, so we can compute all the variants
        let raw_keys_iter = self.get_keys_internal(record_id)?;
        let mut variants = HashSet::new();
        for raw_key in raw_keys_iter {
            variants = Self::variants(&raw_key, Some(variants));
        }

        self.delete_variants_internal(record_id, variants)?;

        //Now replace the record with an empty sentinel vec in the keys table
        //NOTE: We replace the record rather than delete it because we assume there are no gaps in the
        // RecordIDs, when assigning new a RecordID
        let empty_set : HashSet<&[u8]> = HashSet::new();
        self.put_keys_internal(record_id, &empty_set)?;

        Ok(())
    }

    fn delete_variants_internal(&mut self, record_id : RecordID, variants : HashSet<Vec<u8>>) -> Result<(), String> {
        
        //Loop over each variant, and remove the record_id from its associated variant entry in
        // the database, and remove the variant entry if it only referenced the record we're removing
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants.iter() {

            if let Some(variant_entry_bytes) = self.db.get_pinned_cf(variants_cf_handle, variant)? {

                let variant_entry_len = bincode_vec_fixint_len(&variant_entry_bytes);

                //If the variant entry references more than one record, rebuild it with our record absent
                if variant_entry_len > 1 {
                    let mut new_vec : Vec<RecordID> = Vec::with_capacity(variant_entry_len-1);
                    for record_id_bytes in bincode_vec_iter::<RecordID>(&variant_entry_bytes) {
                        let other_record_id = RecordID(usize::from_le_bytes(record_id_bytes.try_into().unwrap()));
                        if other_record_id != record_id {
                            new_vec.push(other_record_id);
                        }
                    }
                    let vec_coder = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian();
                    self.db.put_cf(variants_cf_handle, variant, vec_coder.serialize(&new_vec).unwrap())?;
                } else {
                    //Otherwise, remove the variant entry entirely
                    self.db.delete_cf(variants_cf_handle, variant)?;
                }
            }
        }

        Ok(())
    }

    /// Creates entries in the keys table
    /// If we are updating an old record, we will overwrite it.
    /// 
    /// NOTE: This function will NOT update any variants used to locate the key
    fn put_keys_internal<K : Borrow<[u8]> + Eq + Hash + Serialize>(&mut self, record_id : RecordID, raw_keys : &HashSet<K>) -> Result<(), String> {
        
        //Create the keys vector, serialize it, and put in into the keys table.
        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
        let keys_bytes = record_coder.serialize(&raw_keys).unwrap();
        self.db.put_cf(keys_cf_handle, usize::to_le_bytes(record_id.0), keys_bytes)?;

        Ok(())
    }

    /// Adds the RecordID to each variant of all supplied keys
    fn put_key_variants_internal<K : Borrow<[u8]> + Eq + Hash + Serialize>(&mut self, record_id : RecordID, raw_keys : &HashSet<K>) -> Result<(), String>
    {

        //Now compute all the variants so we'll be able to add an entry to each one
        let mut variants = HashSet::new();
        for raw_key in raw_keys {
            variants = Self::variants(raw_key.borrow(), Some(variants));
        }

        self.put_variants_internal(record_id, variants)
    }

    /// Adds the RecordID to each variant of all supplied keys
    fn put_variants_internal(&mut self, record_id : RecordID, variants : HashSet<Vec<u8>>) -> Result<(), String> {

        //Add the record_id to each variant
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants {
            //TODO: Benchmark using merge_cf() against using a combination of get_pinned_cf() and put_cf()
            let val_bytes = Self::new_variant_vec(record_id);
            self.db.merge_cf(variants_cf_handle, variant, val_bytes)?;
        }

        Ok(())
    }

    /// Add additional keys to a record, including creation of all associated variants
    fn add_keys_internal<K : Borrow<[u8]> + Eq + Hash + Serialize>(&mut self, record_id : RecordID, raw_keys : &HashSet<K>) -> Result<(), String> {

        //Compute all variants for the keys we're going to add
        let mut all_keys = HashSet::new();
        let mut new_keys_variants = HashSet::new();
        for raw_key in raw_keys {
            new_keys_variants = Self::variants(raw_key.borrow(), Some(new_keys_variants));
            all_keys.insert(raw_key.borrow());
        }

        //Get the record's existing keys and Compute all existing variants based on those
        let mut existing_keys_variants = HashSet::new();
        let existing_keys : Vec<Vec<u8>> = self.get_keys_internal(record_id)?.map(|key_box| key_box.to_vec()).collect();
        for existing_key in existing_keys.iter() {
            existing_keys_variants = Self::variants(existing_key, Some(existing_keys_variants));
            all_keys.insert(existing_key);
        }

        //Get the set of variants that is unique to the keys we'll be adding
        let mut unique_keys_variants = HashSet::new();
        for unique_keys_variant in new_keys_variants.difference(&existing_keys_variants) {
            unique_keys_variants.insert(unique_keys_variant.to_owned());
        }

        //Add the new record_id to the appropriate entries for each of the new variants
        self.put_variants_internal(record_id, unique_keys_variants)?;

        //Add the new keys to the record's entry in the keys table by replacing the keys vector
        // with the superset
        self.put_keys_internal(record_id, &all_keys)
    }

    /// Removes the specified keys from the keys associated with a record
    /// 
    /// If one of the specified keys is not associated with the record then that specified
    /// key will be ignored.
    fn remove_keys_internal<K : Borrow<[u8]> + Eq + Hash + Serialize>(&mut self, record_id : RecordID, remove_keys : &HashSet<K>) -> Result<(), String> {

        //Get the record's existing keys, and exclude any that are part of the remove set
        let mut remaining_keys = HashSet::new();
        for existing_key in self.get_keys_internal(record_id)? {
            if !remove_keys.contains(&*existing_key) {
                remaining_keys.insert(existing_key);
            }
        }
        
        //If we're left with no remaining keys, we should throw an error because all records must
        //have at least one key
        if remaining_keys.len() < 1 {
            return Err("cannot remove all keys from record".to_string());
        }

        //Compute all variants for the keys we're removing
        let mut remove_keys_variants = HashSet::new();
        for remove_key in remove_keys {
            remove_keys_variants = Self::variants(remove_key.borrow(), Some(remove_keys_variants));
        }

        //Compute all the variants for the keys that must remain
        let mut remaining_keys_variants = HashSet::new();
        for remaining_key in remaining_keys.iter() {
            remaining_keys_variants = Self::variants(&*remaining_key, Some(remaining_keys_variants));
        }

        //Exclude all of the overlapping variants, leaving only the variants that are unique
        //to the keys we're removing
        let mut unique_keys_variants = HashSet::new();
        for unique_keys_variant in remove_keys_variants.difference(&remaining_keys_variants) {
            unique_keys_variants.insert(unique_keys_variant.to_owned());
        }

        //Delete our RecordID from each variant's list, and remove the list if we made it empty
        self.delete_variants_internal(record_id, unique_keys_variants)?;

        //Update our record's keys in the keys table
        self.put_keys_internal(record_id, &remaining_keys)
    }

    /// Replaces all of the keys in a record with the supplied keys
    ///
    /// NOTE: This function will NOT update any variants used to locate the key
    fn replace_keys_internal<K : Borrow<[u8]> + Eq + Hash + Serialize>(&mut self, record_id : RecordID, raw_keys : &HashSet<K>) -> Result<(), String> {

        if raw_keys.len() < 1 {
            return Err("record must have at least one key".to_string());
        }

        self.delete_keys_internal(record_id)?;
        self.put_key_variants_internal(record_id, raw_keys)?;
        self.put_keys_internal(record_id, raw_keys)
    }

    /// Deletes a record's value in the values table
    /// 
    /// This should only be called as part of another operation as it leaves the record in an
    /// inconsistent state
    fn delete_value_internal(&mut self, record_id : RecordID) -> Result<(), String> {

        let value_cf_handle = self.db.cf_handle(VALUES_CF_NAME).unwrap();
        self.db.delete_cf(value_cf_handle, usize::to_le_bytes(record_id.0))?;

        Ok(())
    }

    /// Creates entries in the values table
    /// If we are updating an old record, we will overwrite it.
    /// 
    /// NOTE: This function will NOT update any variants used to locate the key
    fn put_value_internal(&mut self, record_id : RecordID, value : &ValueT) -> Result<(), String> {
        
        //Serialize the value and put it in the values table.
        let value_cf_handle = self.db.cf_handle(VALUES_CF_NAME).unwrap();
        let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
        let value_bytes = record_coder.serialize(value).unwrap();
        self.db.put_cf(value_cf_handle, usize::to_le_bytes(record_id.0), value_bytes)?;

        Ok(())
    }

    /// Replaces a record's value with the supplied value.  Returns the value that was replaced
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace_value(&mut self, record_id : RecordID, value : &ValueT) -> Result<ValueT, String> {

        let old_value = self.get_value(record_id)?;

        self.put_value_internal(record_id, value)?;

        Ok(old_value)
    }

    /// Inserts a record into the Table, called by insert(), which is implemented differently depending
    /// on the UNICODE_KEYS constant
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    fn insert_internal<K : Borrow<[u8]> + Eq + Hash + Serialize>(&mut self, raw_keys : &HashSet<K>, value : &ValueT) -> Result<RecordID, String> {

        if raw_keys.len() < 1 {
            return Err("record must have at least one key".to_string());
        }

        let new_record_id = match self.deleted_records.pop() {
            None => {
                //We'll be creating a new record, so get the next unique record_id
                let new_record_id = RecordID(self.record_count);
                self.record_count += 1;
                new_record_id
            },
            Some(record_id) => record_id
        };

        //Put the keys, variants and value into the respective tables
        self.put_key_variants_internal(new_record_id, raw_keys)?;
        self.put_keys_internal(new_record_id, raw_keys)?;
        self.put_value_internal(new_record_id, value)?;

        Ok(new_record_id)
    }

    /// Returns an iterator over all possible candidates for a given fuzzy search key, based on
    /// MAX_DELETES, without any distance function applied
    fn fuzzy_candidates_iter<'a>(&'a self, raw_key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {

        //Create all of the potential variants based off of the "meaningful" part of the key
        let variants = Self::variants(raw_key, None);

        //Create a new HashSet to hold all of the RecordIDs that we find
        let mut result_set = HashSet::new(); //TODO, may want to allocate this with a non-zero capacity

        //Check to see if we have entries in the "variants" database for any of the key variants
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants {
            if let Some(variant_vec_bytes) = self.db.get_pinned_cf(variants_cf_handle, variant)? {

                //If we have an entry in the variants database, add all of the referenced RecordIDs to our results
                for record_id_bytes in bincode_vec_iter::<RecordID>(&variant_vec_bytes) {
                    result_set.insert(RecordID(usize::from_le_bytes(record_id_bytes.try_into().unwrap())));
                }
            }
        }

        //Turn our results into an iter for the return value
        Ok(result_set.into_iter())
    }

    fn lookup_fuzzy_raw_internal<'a>(&'a self, raw_key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {
        self.fuzzy_candidates_iter(raw_key)
    }

    fn lookup_fuzzy_internal<'a, D : 'static + Copy + Zero + PartialOrd, F : 'a + Fn(&[u8], &[u8])->D>(&'a self, raw_key : &'a [u8], distance_function : F, threshold : Option<D>) -> Result<impl Iterator<Item=(RecordID, D)> + 'a, String> {
        self.fuzzy_candidates_iter(raw_key).map(move |candidate_iter|
            candidate_iter.filter_map(move |record_id| {

                //QUESTION: Should we have an alternate fast path that only evaluates until we find
                // any distance smaller than threshold?  It would mean we couldn't return a reliable
                // distance but would save us evaluating distance for potentially many keys
                
                //Check the record's keys with the distance function and find the smallest distance
                let mut record_keys_iter = self.get_keys_internal(record_id).unwrap().into_iter();
                let mut smallest_distance = distance_function(&record_keys_iter.next().unwrap()[..], raw_key); //If we have a zero-element keys array, it's a bug elsewhere
                for record_key in record_keys_iter {
                    let distance = distance_function(&record_key, raw_key);
                    if distance < smallest_distance {
                        smallest_distance = distance;
                    }
                }

                match threshold {
                    Some(threshold) => {
                        if smallest_distance <= threshold{
                            Some((record_id, smallest_distance))
                        } else {
                            None
                        }        
                    }
                    None => Some((record_id, smallest_distance))
                }
            })
        )
    }

    fn lookup_best_internal<D : 'static + Copy + Zero + PartialOrd, F : Fn(&[u8], &[u8])->D>(&self, raw_key : &[u8], distance_function : F) -> Result<RecordID, String> {
        let mut result_iter = self.lookup_fuzzy_internal(raw_key, distance_function, None)?;
        
        if let Some(first_result) = result_iter.next() {
            let mut best_result = first_result;
            for result in result_iter {
                if result.1 < best_result.1 {
                    best_result = result;
                }
            }

            return Ok(best_result.0);
        }

        Err("No Matching Record Found".to_string())
    }

    /// Checks the table for records with keys that precisely match the key supplied
    /// 
    /// This function will be more efficient than a fuzzy lookup.
    fn lookup_exact_internal<'a>(&'a self, raw_key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {

        let meaningful_key = Self::meaningful_key_substring(raw_key);

        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        if let Some(variant_vec_bytes) = self.db.get_pinned_cf(variants_cf_handle, meaningful_key)? {

            let record_id_iter = bincode_vec_iter::<RecordID>(&variant_vec_bytes)
                .filter_map(|record_id_bytes| {
                    
                    // Return only the RecordIDs for records if their keys match the key we are looking up
                    if let Some(key_vec_bytes) = self.db.get_pinned_cf(keys_cf_handle, record_id_bytes).unwrap() {

                        //Decode the record's keys
                        let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
                        let record_keys_vec : Vec<Vec<u8>> = record_coder.deserialize(&key_vec_bytes).unwrap();
    
                        //If the full key in the DB matches the key we're checking return the RecordID
                        if record_keys_vec.into_iter().any(|key| key == *raw_key) {
                            Some(RecordID(usize::from_le_bytes(record_id_bytes.try_into().unwrap())))
                        } else {
                            None
                        }
                    } else {
                        panic!("Internal Error: bad record_id in variant");
                    }
                });

            //I guess it's simpler (and more efficeint) to assemble the results in a vec than to try
            // and keep the iterator machinations alive outside this function
            //
            //NOTE: Clippy complains about this, but Clippy doesn't seem to understand that this iterator
            //can't live outside of this function
            #[allow(clippy::needless_collect)] 
            let record_ids : Vec<RecordID> = record_id_iter.collect();

            Ok(record_ids.into_iter())

        } else {

            //No variant found, so return an empty Iterator
            Ok(vec![].into_iter())
        }
    }

    /// Returns the value associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_value(&self, record_id : RecordID) -> Result<ValueT, String> {

        //Get the value object by deserializing the bytes from the db
        let values_cf_handle = self.db.cf_handle(VALUES_CF_NAME).unwrap();
        if let Some(value_bytes) = self.db.get_pinned_cf(values_cf_handle, record_id.0.to_le_bytes())? {
            let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
            let value : ValueT = record_coder.deserialize(&value_bytes).unwrap();

            Ok(value)
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Returns the number of keys associated with a specified record
    pub fn keys_count(&self, record_id : RecordID) -> Result<usize, String> {

        //Get the keys vec from the db
        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        if let Some(keys_vec_bytes) = self.db.get_pinned_cf(keys_cf_handle, record_id.0.to_le_bytes())? {

            //The vector element count should be the first encoded usize
            let mut skip_bytes = 0;
            let keys_count = bincode_u64_le_varint(&keys_vec_bytes, &mut skip_bytes);

            if keys_count > 0 {
                Ok(keys_count as usize)
            } else {
                Err("Invalid record_id".to_string())
            }
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Returns the keys associated with a specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    fn get_keys_internal(&self, record_id : RecordID) -> Result<impl Iterator<Item=Box<[u8]>>, String> {

        //Get the keys vec by deserializing the bytes from the db
        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        if let Some(keys_vec_bytes) = self.db.get_pinned_cf(keys_cf_handle, record_id.0.to_le_bytes())? {
            let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
            let keys_vec : Vec<Box<[u8]>> = record_coder.deserialize(&keys_vec_bytes).unwrap();

            if keys_vec.len() > 0 {
                Ok(keys_vec.into_iter())
            } else {
                Err("Invalid record_id".to_string())
            }
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    // Creates a Vec<RecordID> with one entry, serialized out as a string of bytes
    fn new_variant_vec(record_id : RecordID) -> Vec<u8> {

        //Create a new vec and Serialize it out
        let new_vec = vec![record_id];
        let vec_coder = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian();
        vec_coder.serialize(&new_vec).unwrap()
    }

    // The function to add a new entry for a variant in the database, formulated as a RocksDB callback
    fn variant_append_merge(_key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Option<Vec<u8>> {

        // Note: I've seen this function be called at odd times by RocksDB, such as when a DB is
        // opened.  I haven't been able to get a straight answer on why RocksDB calls this function
        // unnecessarily, but it doesn't seem to be hurting performance much.

        //TODO: Status prints in this function to understand the behavior of RocksDB.
        // Remove them when this is understood.
        // println!("Append-Called {:?}", std::str::from_utf8(key).unwrap());
        let vec_coder = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian();

        //Deserialize the existing database entry into a vec of RecordIDs
        //NOTE: we're actually using a HashSet because we don't want any duplicates
        let mut variant_vec = if let Some(existing_bytes) = existing_val {
            let new_vec : HashSet<RecordID> = vec_coder.deserialize(existing_bytes).unwrap();
            new_vec
        } else {
            //TODO: Remove status println!()
            // println!("MERGE WITH NONE!!");
            HashSet::with_capacity(operands.size_hint().0)
        };

        //Add the new RecordID(s)
        for op in operands {
            //Deserialize the vec on the operand, and merge its entries into the existing vec
            let operand_vec : HashSet<RecordID> = vec_coder.deserialize(op).unwrap();
            variant_vec.extend(operand_vec);
        }

        //TODO: Remove status println!()
        // println!("AppendResults {:?}", variant_vec);

        //Serialize the vec back out again
        let result = vec_coder.serialize(&variant_vec).unwrap();
        Some(result)
    }

    // Returns the "meaningful" part of a key, that is used as the starting point to generate the variants
    fn meaningful_key_substring(key: &[u8]) -> Vec<u8> {
        Self::unicode_truncate(key, MEANINGFUL_KEY_LEN)
    }

    // Returns all of the variants of a key that we will put into the variants database
    fn variants(key: &[u8], existing_variants : Option<HashSet<Vec<u8>>>) -> HashSet<Vec<u8>> {

        let mut variants_set : HashSet<Vec<u8>> = match existing_variants {
            Some(existing_variants) => existing_variants,
            None => HashSet::new()
        };

        let meaningful_key = Self::meaningful_key_substring(key);

        if 0 < MAX_DELETES {
            Self::variants_recursive(&meaningful_key[..], 0, &mut variants_set);
        }
        variants_set.insert(meaningful_key);
        
        variants_set
    }
    
    // The recursive part of the variants() function
    fn variants_recursive(key: &[u8], edit_distance: usize, variants_set: &mut HashSet<Vec<u8>>) {
    
        let edit_distance = edit_distance + 1;
    
        let key_len = Self::unicode_len(key);
    
        if key_len > 1 {
            for i in 0..key_len {
                let variant = Self::unicode_remove(key, i);
    
                if !variants_set.contains(&variant) {
    
                    if edit_distance < MAX_DELETES {
                        Self::variants_recursive(&variant, edit_distance, variants_set);
                    }
    
                    variants_set.insert(variant);
                }
            }
        }
    }

    // Returns the length of a utf-8 string, stored in a slice of bytes
    // The "UNICODE_KEYS" path relies on the buffer being valid unicode
    fn unicode_len(s: &[u8]) -> usize {
        if UNICODE_KEYS {
            let the_str = unsafe{std::str::from_utf8_unchecked(s)};
            the_str.chars().count()
        } else {
            s.len()
        }
    }

    // Returns the unicode character at the idx, counting through each character in the string
    // The "UNICODE_KEYS" path relies on the buffer being valid unicode
    // Will panic if idx is greater than the number of characters in the parsed string
    fn unicode_char_at_index(s: &[u8], idx : usize) -> char {
        if UNICODE_KEYS {
            let the_str = unsafe{std::str::from_utf8_unchecked(s)};
            the_str.chars().nth(idx).unwrap()
        } else {
            s[idx] as char
        }
    }

    // Removes a single unicode character at the specified index from a utf-8 string stored in a slice of bytes
    // The "UNICODE_KEYS" path relies on the buffer being valid unicode
    fn unicode_remove(s: &[u8], index: usize) -> Vec<u8> {
        if UNICODE_KEYS {
            let the_str = unsafe{std::str::from_utf8_unchecked(s)};
            let new_str : String = the_str.chars()
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(_, the_char)| the_char)
                .collect();
            new_str.into_bytes()
        } else {
            let mut new_vec = s.to_owned();
            new_vec.remove(index);
            new_vec
        }
    }

    // Returns the first n characters up to len from a utf-8 string stored in a slice of bytes
    // The "UNICODE_KEYS" path relies on the buffer being valid unicode
    fn unicode_truncate(s: &[u8], len: usize) -> Vec<u8> {
        if UNICODE_KEYS {
            let the_str = unsafe{std::str::from_utf8_unchecked(s)};
            let new_str : String = the_str.chars()
                .enumerate()
                .filter(|(i, _)| *i < len)
                .map(|(_, the_char)| the_char)
                .collect();
            new_str.into_bytes()
        } else {
            if s.len() > len {
                let (prefix, _remainder) = s.split_at(len);
                prefix.to_owned()
            } else {
                s.to_owned()
            }
        }
    }

    /// Convenience function that returns the [edit_distance](Table::edit_distance) function associated with a Table
    pub fn default_distance_func(&self) -> impl Fn(&[u8], &[u8]) -> u64 {
        Self::edit_distance
    }

    /// An implementation of the basic Levenstein distance function, which may be passed to
    /// [lookup_fuzzy](Table::lookup_fuzzy), [lookup_best](Table::lookup_best), or used anywhere
    /// else a distance function is needed.
    /// 
    /// This implementation uses the Wagner-Fischer Algorithm, as it's described [here](https://en.wikipedia.org/wiki/Levenshtein_distance)
    pub fn edit_distance(key_a : &[u8], key_b : &[u8]) -> u64 {

        let m = Self::unicode_len(key_a)+1;
        let n = Self::unicode_len(key_b)+1;

        //Allocate a 2-dimensional vec for the distances between the first i characters of key_a
        //and the first j characters of key_b
        let mut d = vec![vec![0; n]; m];

        //NOTE: I personally find this (below) more readable, but clippy really like the other style.  -\_(..)_/-
        // for i in 1..m {
        //     d[i][0] = i;
        // }
        for (i, row) in d.iter_mut().enumerate().skip(1) {
            row[0] = i;
        }

        for j in 1..n {
            d[0][j] = j;
        }

        for j in 1..n {
            for i in 1..m {

                let substitution_cost = if Self::unicode_char_at_index(key_a, i-1) == Self::unicode_char_at_index(key_b, j-1) {
                    0
                } else {
                    1
                };

                let deletion_distance = d[i-1][j] + 1;
                let insertion_distance = d[i][j-1] + 1;
                let substitution_distance = d[i-1][j-1] + substitution_cost;

                let smallest_distance = min(min(deletion_distance, insertion_distance), substitution_distance);
                
                d[i][j] = smallest_distance;  
            }
        }

        d[m-1][n-1] as u64
    }
}

impl <ValueT : 'static + Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize>Table<ValueT, MAX_DELETES, MEANINGFUL_KEY_LEN, true> {

    /// Inserts a new key-value pair into the table and returns the RecordID of the new record
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [create](Table::create)
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn insert(&mut self, key : &str, value : &ValueT) -> Result<RecordID, String> {
        let mut keys_set = HashSet::with_capacity(1);
        keys_set.insert(key.as_bytes());
        self.insert_internal(&keys_set, value)
    }

    /// Retrieves a key-value pair using a RecordID
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [get_one_key](Table::get_one_key) / [get_value](Table::get_value)
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get(&self, record_id : RecordID) -> Result<(String, ValueT), String> {
        let key = self.get_one_key(record_id)?;
        let value = self.get_value(record_id)?;

        Ok((key, value))
    }

    /// Creates a new record in the table and returns the RecordID of the new record
    /// 
    /// This function will always create a new record, regardless of whether an identical key exists.
    /// It is permissible to have two distinct records with identical keys.
    /// 
    /// NOTE: This function takes an &T for value rather than an owned T because it must make an
    /// internal copy regardless of passed ownership, so requiring an owned object would ofter
    /// result in a redundant copy.  However this is different from most containers, and makes things
    /// feel awkward when using [String] types for values.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn create<K : Borrow<str>>(&mut self, keys : &[K], value : &ValueT) -> Result<RecordID, String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow().as_bytes()));
        self.insert_internal(&keys_set, value)
    }

    /// Adds the supplied keys to the record's keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn add_keys<K : Borrow<str>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow().as_bytes()));
        self.add_keys_internal(record_id, &keys_set)
    }

    /// Removes the supplied keys from the keys associated with a record
    /// 
    /// If one of the specified keys is not associated with the record then that specified
    /// key will be ignored.
    /// 
    /// If removing the keys would result in a record with no keys, this operation will return
    /// an error and no keys will be removed, because all records must have at least one key.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn remove_keys<K : Borrow<str>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow().as_bytes()));
        self.remove_keys_internal(record_id, &keys_set)
    }

    /// Replaces a record's keys with the supplied keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace_keys<K : Borrow<str>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow().as_bytes()));
        self.replace_keys_internal(record_id, &keys_set)
    }

    /// Returns an iterator over all of the key associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_keys(&self, record_id : RecordID) -> Result<impl Iterator<Item=String>, String> {
        Ok(self.get_keys_internal(record_id)?.map(|key| {
            unsafe{ String::from_utf8_unchecked(key.to_vec()) }
        }))
    }

    /// Returns one key associated with the specified record.  If the record has more than one key
    /// then which key is unspecified
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_one_key(&self, record_id : RecordID) -> Result<String, String> {
        //TODO: Perhaps we can speed this up in the future by avoiding deserializing all keys
        let first_key = self.get_keys_internal(record_id)?.next().unwrap();
        Ok(unsafe{ String::from_utf8_unchecked(first_key.to_vec()) } )
    }

    /// Locates all records in the table with keys that precisely match the key supplied
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_exact<'a>(&'a self, key : &'a str) -> Result<impl Iterator<Item=RecordID> + 'a, String> {
        self.lookup_exact_internal(key.as_bytes())
    }

    /// Locates all records in the table with a key that is within a deletion distance of MAX_DELETES of
    /// the key supplied, based on the SymSpell algorithm.
    /// 
    /// This function underlies all fuzzy lookups, and does no further filtering based on any distance function.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy_raw<'a>(&'a self, key : &'a str) -> Result<impl Iterator<Item=RecordID> + 'a, String> {
        self.lookup_fuzzy_raw_internal(key.as_bytes())
    }

    /// Locates all records in the table for which the supplied `distance_function` evaluates to a result smaller
    /// than the supplied `threshold` when comparing the record's key with the supplied `key`
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy<'a, D : 'static + Copy + Zero + PartialOrd, F : 'a + Fn(&[u8], &[u8])->D>(&'a self, key : &'a str, distance_function : F, threshold : D) -> Result<impl Iterator<Item=(RecordID, D)> + 'a, String> {
        self.lookup_fuzzy_internal(key.as_bytes(), distance_function, Some(threshold))
    }

    /// Locates the record in the table for which the supplied `distance_function` evaluates to the lowest value
    /// when comparing the record's key with the supplied `key`.
    /// 
    /// If no matching record is found within the table's `MAX_DELETES`, this method will return an error.
    /// 
    /// NOTE: If two or more results have the same returned distance value and that is the smallest value, the
    /// implementation does not specify which result will be returned.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_best<D : 'static + Copy + Zero + PartialOrd, F : Fn(&[u8], &[u8])->D>(&self, key : &str, distance_function : F) -> Result<RecordID, String> {
        self.lookup_best_internal(key.as_bytes(), distance_function)
    }
}

impl <ValueT : 'static + Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize>Table<ValueT, MAX_DELETES, MEANINGFUL_KEY_LEN, false> {

    /// Inserts a new key-value pair into the table and returns the RecordID of the new record
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [create](Table::create)
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn insert(&mut self, key : &[u8], value : &ValueT) -> Result<RecordID, String> {
        let mut keys_set = HashSet::with_capacity(1);
        keys_set.insert(key);
        self.insert_internal(&keys_set, value)
    }

    /// Retrieves a key-value pair using a RecordID
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [get_one_key](Table::get_one_key) / [get_value](Table::get_value)
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get(&self, record_id : RecordID) -> Result<(Box<[u8]>, ValueT), String> {
        let key = self.get_one_key(record_id)?;
        let value = self.get_value(record_id)?;

        Ok((key, value))
    }

    /// Creates a new record in the table and returns the RecordID of the new record
    /// 
    /// This function will always create a new record, regardless of whether an identical key exists.
    /// It is permissible to have two distinct records with identical keys.
    /// 
    /// NOTE: This function takes an &T for value rather than an owned T because it must make an
    /// internal copy regardless of passed ownership, so requiring an owned object would ofter
    /// result in a redundant copy.  However this is different from most containers, and makes things
    /// feel awkward when using [String] types for values.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn create<K : Borrow<[u8]>>(&mut self, keys : &[K], value : &ValueT) -> Result<RecordID, String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow()));
        self.insert_internal(&keys_set, value)
    }

    /// Adds the supplied keys to the record's keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn add_keys<K : Borrow<[u8]>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow()));
        self.add_keys_internal(record_id, &keys_set)
    }

    /// Removes the supplied keys from the keys associated with a record
    /// 
    /// If one of the specified keys is not associated with the record then that specified
    /// key will be ignored.
    /// 
    /// If removing the keys would result in a record with no keys, this operation will return
    /// an error and no keys will be removed, because all records must have at least one key.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn remove_keys<K : Borrow<[u8]>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow()));
        self.remove_keys_internal(record_id, &keys_set)
    }

    /// Replaces a record's keys with the supplied keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace_keys<K : Borrow<[u8]>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&[u8]> = HashSet::from_iter(keys.into_iter().map(|key| key.borrow()));
        self.replace_keys_internal(record_id, &keys_set)
    }

    /// Returns an iterator over all of the key associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_keys(&self, record_id : RecordID) -> Result<impl Iterator<Item=Box<[u8]>>, String> {
        self.get_keys_internal(record_id)
    }

    /// Returns one key associated with the specified record.  If the record has more than one key
    /// then which key is unspecified
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_one_key(&self, record_id : RecordID) -> Result<Box<[u8]>, String> {
        //TODO: Perhaps we can speed this up in the future by avoiding deserializing all keys
        Ok(self.get_keys_internal(record_id)?.next().unwrap())
    }

    /// Locates all records in the table with keys that precisely match the key supplied
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_exact<'a>(&'a self, key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {
        self.lookup_exact_internal(key)
    }

    /// Locates all records in the table with a key that is within a deletion distance of MAX_DELETES of
    /// the key supplied, based on the SymSpell algorithm.
    /// 
    /// This function underlies all fuzzy lookups, and does no further filtering based on any distance function.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy_raw<'a>(&'a self, key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {
        self.lookup_fuzzy_raw_internal(key)
    }

    /// Locates all records in the table for which the supplied `distance_function` evaluates to a result smaller
    /// than the supplied `threshold` when comparing the record's key with the supplied `key`
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy<'a, D : 'static + Copy + Zero + PartialOrd, F : 'a + Fn(&[u8], &[u8])->D>(&'a self, key : &'a [u8], distance_function : F, threshold : D) -> Result<impl Iterator<Item=(RecordID, D)> + 'a, String> {
        self.lookup_fuzzy_internal(key, distance_function, Some(threshold))
    }

    /// Locates the record in the table for which the supplied `distance_function` evaluates to the lowest value
    /// when comparing the record's key with the supplied `key`.
    /// 
    /// If no matching record is found within the table's `MAX_DELETES`, this method will return an error.
    /// 
    /// NOTE: If two or more results have the same returned distance value and that is the smallest value, the
    /// implementation does not specify which result will be returned.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_best<D : 'static + Copy + Zero + PartialOrd, F : Fn(&[u8], &[u8])->D>(&self, key : &[u8], distance_function : F) -> Result<RecordID, String> {
        self.lookup_best_internal(key, distance_function)
    }
}

impl <ValueT : Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool>Drop for Table<ValueT, MAX_DELETES, MEANINGFUL_KEY_LEN, UNICODE_KEYS> {
    fn drop(&mut self) {
        //Close down Rocks
        self.db.flush().unwrap();
        let _ = DB::destroy(&rocksdb::Options::default(), self.path.as_str());
    }
}

// Returns the usize that is one larger than the largest key, assuming the column family contains a
// all of the smaller keys without any gaps.  If there are missing keys, the results are undefined.
//
// Implements a binary search through the possible keys, looking for the highest numbered key
// This function should resolve one bit of the key, each time through the loop, so it should loop
// at most 64 times for a 64 bit key, and likely much less because of the starting hint
fn probe_for_max_sequential_key(db : &DBWithThreadMode<rocksdb::SingleThreaded>, cf : &ColumnFamily, starting_hint : usize) -> Result<usize, rocksdb::Error> {

    let mut min = 0;
    let mut max = usize::MAX;

    //Need to adjust this constant so we don't risk overflow if we don't have 64-bit usize
    debug_assert!(::std::mem::size_of::<usize>() == 8);
    let mut guess_max = if starting_hint > 0xFFFFFFFF {
        usize::MAX
    } else if starting_hint < 1 {
        1
    } else {
        starting_hint * starting_hint
    };
    
    let mut cur_val = starting_hint;
    loop {

        //NOTE: this is an optimization to save one DB query at the cost of an extra test each loop
        //The case where max == min will result in no hit and exit at the bottom of the loop body
        if max == min {
            return Ok(cur_val)
        }

        if let Some(_value) = db.get_pinned_cf(cf, cur_val.to_le_bytes())? {
            //println!("Yes, cur_val = {}, min = {}, max = {}, guess_max = {}", cur_val, min, max, guess_max);
            min = cur_val + 1;
            if guess_max < max/2 {
                guess_max *= 2;
            } else {
                guess_max = max;
            }
        } else {
            //println!("No, cur_val = {}, min = {}, max = {}, guess_max = {}", cur_val, min, max, guess_max);
            max = cur_val;
            guess_max = max;

            if max == min {
                return Ok(cur_val)
            }    
        }

        cur_val = ((guess_max - min) / 2) + min;
    }
}

/// This Iterator object is designed to iterate over the entries in a bincode-encoded Vec<T>
/// without needing to actually deserialize the Vec into a temporary memory object
/// 
/// **NOTE** This type assumes bincode is configured with [FixintEncoding](bincode::config::FixintEncoding),
/// i.e. 64-bit usize types and [LittleEndian](bincode::config::LittleEndian) byte order.  Any other
/// configuration and your data may be corrupt.
/// 
/// TODO: Try using `with_varint_encoding` rather than `with_fixint_encoding`, and measure performance.
/// It's highly likely that the data size reduction completely makes up for the extra work and memcpy incurred
/// deserializing the structure, and it's faster not to mess with trying to read the buffer without fully
/// deserializing it.
struct BinCodeVecIterator<'a, T : Sized + Copy> {
    remaining_buf : &'a [u8],
    phantom: PhantomData<&'a T>,
}

/// Returns the length of a Vec<T> that has been encoded with bincode, using
/// [FixintEncoding](bincode::config::FixintEncoding) and
/// [LittleEndian](bincode::config::LittleEndian) byte order.
fn bincode_vec_fixint_len(buf : &[u8]) -> usize {

    let (len_chars, _remainder) = buf.split_at(8);
    usize::from_le_bytes(len_chars.try_into().unwrap())
}

/// Returns a [BinCodeVecIterator] to iterate over a Vec<T> that has been encoded with bincode,
/// without requiring an actual [Vec] to be recreated in memory
fn bincode_vec_iter<T : Sized + Copy>(buf : &[u8]) -> BinCodeVecIterator<'_, T> {

    //Skip over the length at the beginning (8 bytes = 64bit usize), because we can infer
    // the Vec length from the buffer size
    let (_len_chars, remainder) = buf.split_at(8);

    BinCodeVecIterator{remaining_buf: remainder, phantom : PhantomData}
}

impl <'a, T : Sized + Copy>Iterator for BinCodeVecIterator<'a, T> {
    type Item = &'a [u8];

    //NOTE: type Item = &T; would be better.
    // Ideally we'd decode T inside of next(), but we would need a way to decode it without making
    // a copy because making a copy would defeat the whole purpose of the in-place access

    fn next(&mut self) -> Option<&'a [u8]> {
        let t_size_bytes = ::std::mem::size_of::<T>();
        
        if self.remaining_buf.len() >= t_size_bytes {
            let (t_chars, remainder) = self.remaining_buf.split_at(t_size_bytes);
            self.remaining_buf = remainder;
            Some(t_chars)  
        } else {
            None
        }
    }
}

/// Interprets the bytes at the start of `buf` as an encoded 64-bit unsigned number that has been
/// encoded with bincode, using [VarintEncoding](bincode::config::VarintEncoding) and
/// [LittleEndian](bincode::config::LittleEndian) byte order.
/// 
/// Returns the encoded value, and sets `num_bytes` to the number of bytes in the buffer used to encode
/// the value.
fn bincode_u64_le_varint(buf : &[u8], num_bytes : &mut usize) -> u64 {

    match buf[0] {
        251 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(2);
            let value = u16::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 3;
            value as u64
        },
        252 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(4);
            let value = u32::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 5;
            value as u64
        },
        253 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(8);
            let value = u64::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 9;
            value
        },
        254 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(16);
            let value = u128::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 17;
            value as u64
        },
        _ => {
            *num_bytes = 1;
            buf[0] as u64
        }
    }
}

//TODO: Currently Unused.  Delete
// /// Returns a slice representing the characters of a String that has been encoded with bincode, using
// /// [VarintEncoding](bincode::config::VarintEncoding) and [LittleEndian](bincode::config::LittleEndian) byte order.
// fn bincode_string_varint(buf : &[u8]) -> &[u8] {

//     //Interpret the length
//     let mut skip_bytes = 0;
//     let string_len = bincode_u64_le_varint(buf, &mut skip_bytes);

//     //Split the slice to grab the string
//     let (_len_chars, remainder) = buf.split_at(skip_bytes);
//     let (string_slice, _remainder) = remainder.split_at(string_len as usize);
//     string_slice
// }

#[cfg(test)]
mod tests {
    use crate::{*};
    use std::fs;
    use std::path::PathBuf;
    use csv::ReaderBuilder;
    use serde::{Deserialize};

    #[test]
    /// This test is designed to stress the database with many thousand entries.
    ///  
    /// You may download an alternate GeoNames file in order to get a more rigorous test.  The included
    /// `geonames_megacities.txt` file is just a stub to avoid bloating the crate download.  The content
    /// of `geonames_megacities.txt` was derived from data on [http://geonames.org], and licensed under
    /// a [Creative Commons Attribution 4.0 License](https://creativecommons.org/licenses/by/4.0/legalcode)
    /// 
    /// A geonames file may be downloaded from: [http://download.geonames.org/export/dump/cities15000.zip]
    /// for the smallest file, and "cities500.zip" for the largest, depending on whether you want this
    /// to pass in the a lightweight way or the most thorough.
    fn geonames_test() {

        let mut geonames_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        geonames_file_path.push("geonames_megacities.txt");
        
        // Alternate geonames file
        // NOTE: Uncomment this to use a different file
        // let geonames_file_path = PathBuf::from("/path/to/file/cities500.txt");
    
        //Create the FuzzyRocks Table, and clear out any records that happen to be hanging out
        let mut table = Table::<i32, 2, 12, true>::new("geonames.rocks").unwrap();
        table.reset().unwrap();

        //Data structure to parse the GeoNames TSV file into
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct GeoName {
            geonameid         : i32, //integer id of record in geonames database
            name              : String, //name of geographical point (utf8) varchar(200)
            asciiname         : String, //name of geographical point in plain ascii characters, varchar(200)
            alternatenames    : String, //alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
            latitude          : f32, //latitude in decimal degrees (wgs84)
            longitude         : f32, //longitude in decimal degrees (wgs84)
            feature_class     : char, //see http://www.geonames.org/export/codes.html, char(1)
            feature_code      : String,//[char; 10], //see http://www.geonames.org/export/codes.html, varchar(10)
            country_code      : String,//[char; 2], //ISO-3166 2-letter country code, 2 characters
            cc2               : String, //alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
            admin1_code       : String,//[char; 20], //fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
            admin2_code       : String, //code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
            admin3_code       : String,//[char; 20], //code for third level administrative division, varchar(20)
            admin4_code       : String,//[char; 20], //code for fourth level administrative division, varchar(20)
            population        : i64, //bigint (8 byte int)
            #[serde(deserialize_with = "default_if_empty")]
            elevation         : i32, //in meters, integer
            #[serde(deserialize_with = "default_if_empty")]
            dem               : i32, //digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
            timezone          : String, //the iana timezone id (see file timeZone.txt) varchar(40)
            modification_date : String, //date of last modification in yyyy-MM-dd format
        }

        fn default_if_empty<'de, D, T>(de: D) -> Result<T, D::Error>
            where D: serde::Deserializer<'de>, T: serde::Deserialize<'de> + Default,
        {
            Option::<T>::deserialize(de).map(|x| x.unwrap_or_else(|| T::default()))
        }

        //Open the tab-saparated value file
        let tsv_file_contents = fs::read_to_string(geonames_file_path).expect("Error reading geonames file");
        let mut tsv_parser = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .flexible(true) //We want to permit situations where some rows have fewer columns for now
            .quote(0)
            .double_quote(false)
            .from_reader(tsv_file_contents.as_bytes());

        //Iterate over every geoname entry in the geonames file and insert it (lowercase) into our table
        let mut record_id = RecordID::NULL;
        let mut tsv_record_count = 0;
        for geoname in tsv_parser.deserialize::<GeoName>().map(|result| result.unwrap()) {

            //Separate the comma-separated alternatenames field
            let mut names : Vec<String> = geoname.alternatenames.split(',').map(|string| string.to_lowercase()).collect();
            
            //Add the primary name for the place
            names.push(geoname.name.to_lowercase());
            
            //Create a record in the table
            record_id = table.create(&names[..], &geoname.geonameid).unwrap();
            tsv_record_count += 1;

            //Status Print
            if record_id.0 % 500 == 499 {
                println!("inserting... {}, {}", geoname.name.to_lowercase(), record_id.0);
            }
        }

        //Indirectly that the number of records roughly matches the number of entries from the CSV
        //NOTE: RocksDB doesn't have a "record_count" feature, and therefore neither does our Table,
        //but since we started from a reset table, we can ensure that the last assigned record_id
        //should roughly correspond to the number of entries we inserted
        assert_eq!(record_id.0 + 1, tsv_record_count);

        //Confirm we can find a known city (London)
        let london_results : Vec<i32> = table.lookup_exact("london").unwrap().map(|record_id| table.get_value(record_id).unwrap()).collect();
        assert!(london_results.contains(&2643743)); //2643743 is the geonames_id of "London"

        //Close RocksDB connection by dropping the table object
        drop(table);
        drop(london_results);

        //Reopen the table and confirm that "London" is still there
        let table = Table::<i32, 2, 12, true>::new("geonames.rocks").unwrap();
        let london_results : Vec<i32> = table.lookup_exact("london").unwrap().map(|record_id| table.get_value(record_id).unwrap()).collect();
        assert!(london_results.contains(&2643743)); //2643743 is the geonames_id of "London"
    }

    #[test]
    fn fuzzy_rocks_test() {

        //Create and reset the FuzzyRocks Table
        let mut table = Table::<String, 2, 8, true>::new("test.rocks").unwrap();
        table.reset().unwrap();

        //Insert some records
        let sun = table.insert("Sunday", &"Nichiyoubi".to_string()).unwrap();
        let sat = table.insert("Saturday", &"Douyoubi".to_string()).unwrap();
        let fri = table.insert("Friday", &"Kinyoubi".to_string()).unwrap();
        let thu = table.insert("Thursday", &"Mokuyoubi".to_string()).unwrap();
        let wed = table.insert("Wednesday", &"Suiyoubi".to_string()).unwrap();
        let tue = table.insert("Tuesday", &"Kayoubi".to_string()).unwrap();
        let mon = table.insert("Monday", &"Getsuyoubi".to_string()).unwrap();

        //Test lookup_exact
        let results : Vec<(String, String)> = table.lookup_exact("Friday").unwrap().map(|record_id| table.get(record_id).unwrap()).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Friday");
        assert_eq!(results[0].1, "Kinyoubi");

        //Test lookup_exact, with a query that should provide no results
        let results : Vec<RecordID> = table.lookup_exact("friday").unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test lookup_best, using the supplied edit_distance function
        let result = table.lookup_best("Bonday", table.default_distance_func()).unwrap();
        assert_eq!(result, mon);

        //Test lookup_best, when there is no acceptable match
        let result = table.lookup_best("Rahu", table.default_distance_func());
        assert!(result.is_err());

        //Test lookup_fuzzy with a perfect match, using the supplied edit_distance function
        //In this case, we should only get one match within edit-distance 2
        let results : Vec<(String, String, u64)> = table.lookup_fuzzy("Saturday", table.default_distance_func(), 2)
            .unwrap().map(|(record_id, distance)| {
                let (key, val) = table.get(record_id).unwrap();
                (key, val, distance)
            }).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Saturday");
        assert_eq!(results[0].1, "Douyoubi");
        assert_eq!(results[0].2, 0);

        //Test lookup_fuzzy with a perfect match, but where we'll hit another imperfect match as well
        let results : Vec<(String, String, u64)> = table.lookup_fuzzy("Tuesday", table.default_distance_func(), 2)
            .unwrap().map(|(record_id, distance)| {
                let (key, val) = table.get(record_id).unwrap();
                (key, val, distance)
            }).collect();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&("Tuesday".to_string(), "Kayoubi".to_string(), 0)));
        assert!(results.contains(&("Thursday".to_string(), "Mokuyoubi".to_string(), 2)));

        //Test lookup_fuzzy where we should get no match
        let results : Vec<(RecordID, u64)> = table.lookup_fuzzy("Rahu", table.default_distance_func(), 2).unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test lookup_fuzzy_raw, to get all of the SymSpell Delete variants
        //We're testing the fact that characters beyond MEANINGFUL_KEY_LEN aren't used for the comparison
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Sunday. That's my fun day.").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sun);

        //Test deleting a record, and ensure we can't access it or any trace of its variants
        table.delete(tue).unwrap();
        assert!(table.get_one_key(tue).is_err());

        //Since "Tuesday" had one variant overlap with "Thursday", i.e. "Tusday", make sure we now find
        // "Thursday" when we attempt to lookup "Tuesday"
        let result = table.lookup_best("Tuesday", table.default_distance_func()).unwrap();
        assert_eq!(result, thu);

        //Delete "Saturday" and make sure we see no matches when we try to search for it
        table.delete(sat).unwrap();
        assert!(table.get_one_key(sat).is_err());
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Saturday").unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test replacing a record with another one and ensure the right data is retained
        table.replace_keys(wed, &["Miercoles"]).unwrap();
        table.replace_value(wed, &"Zhousan".to_string()).unwrap();
        let results : Vec<(String, String)> = table.lookup_exact("Miercoles").unwrap().map(|record_id| (table.get_one_key(record_id).unwrap(), table.get_value(record_id).unwrap())).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Miercoles");
        assert_eq!(results[0].1, "Zhousan");
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Mercoledi").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], wed);

        //Try replacing the keys and value on a record that we deleted earlier, and make sure
        // we get the right errors
        assert!(table.replace_keys(sat, &["Sabado"]).is_err());
        assert!(table.replace_value(sat, &"Zhouliu".to_string()).is_err());

        //Attempt to replace a record's keys with an empty list, check the error
        let empty_slice : &[&str] = &[];
        assert!(table.replace_keys(sat, empty_slice).is_err());
        
        //Attempt to replace an invalid record and confirm we get a reasonable error
        assert!(table.replace_keys(RecordID::NULL, &["Nullday"]).is_err());
        assert!(table.replace_value(RecordID::NULL, &"Null".to_string()).is_err());

        //Test that create() returns the right error if no keys are supplied
        let empty_slice : &[&str] = &[];
        assert!(table.create(empty_slice, &"Douyoubi".to_string()).is_err());

        //Recreate Saturday using the create() api
        //While we're here, Also test that the same key string occurring more than once
        // doesn't result in additional keys being added
        let sat = table.create(&["Saturday", "Saturday"], &"Douyoubi".to_string()).unwrap();
        assert_eq!(table.keys_count(sat).unwrap(), 1);

        //Add some new keys to it, and verify that it can be found using any of its three keys
        table.add_keys(sat, &["Sabado", "Zhouliu"]).unwrap();
        assert_eq!(table.keys_count(sat).unwrap(), 3);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Saturday").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sat);
        let results : Vec<RecordID> = table.lookup_exact("Zhouliu").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sat);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Sabato").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sat);

        //Test deleting one of the keys from a record, and make sure we can't find it using that
        //key but the other keys are unaffected
        table.remove_keys(sat, &["Sabado"]).unwrap();
        assert_eq!(table.keys_count(sat).unwrap(), 2);
        let results : Vec<RecordID> = table.lookup_exact("Sabado").unwrap().collect();
        assert_eq!(results.len(), 0);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Sabato").unwrap().collect();
        assert_eq!(results.len(), 0);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Saturnsday").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sat);
        let results : Vec<RecordID> = table.lookup_exact("Zhouliu").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sat);

        //Attempt to remove the remaining keys and ensure we get an error and both keys remain
        assert!(table.remove_keys(sat, &["Saturday", "Zhouliu"]).is_err());
        assert_eq!(table.keys_count(sat).unwrap(), 2);
        let results : Vec<RecordID> = table.lookup_exact("Saturday").unwrap().collect();
        assert_eq!(results.len(), 1);
        let results : Vec<RecordID> = table.lookup_exact("Zhouliu").unwrap().collect();
        assert_eq!(results.len(), 1);

        //Test that replacing the keys of a record doesn't leave any orphaned variants
        table.replace_keys(sat, &["Sabado"]).unwrap();
        assert_eq!(table.keys_count(sat).unwrap(), 1);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Saturday").unwrap().collect();
        assert_eq!(results.len(), 0);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Zhouliu").unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test that adding the same key again doesn't result in multiple copies of the key
        table.add_keys(sat, &["Sabado"]).unwrap();
        assert_eq!(table.keys_count(sat).unwrap(), 1);
        table.add_keys(sat, &["Saturday", "Saturday"]).unwrap();
        assert_eq!(table.keys_count(sat).unwrap(), 2);

        //Test that nothing breaks when we have two keys with overlapping variants, and then
        // delete one
        // "Venerdi" & "Vendredi" have overlapping variant: "Venedi"
        table.add_keys(fri, &["Geumyoil", "Viernes", "Venerdi", "Vendredi"]).unwrap();
        assert_eq!(table.keys_count(fri).unwrap(), 5);
        table.remove_keys(fri, &["Vendredi"]).unwrap();
        assert_eq!(table.keys_count(fri).unwrap(), 4);
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Vendredi").unwrap().collect();
        assert_eq!(results.len(), 1); //We'll still get Venerdi as a fuzzy match

        //Try deleting the non-existent key with valid variants, to make sure nothing breaks
        table.remove_keys(fri, &["Vendredi"]).unwrap();
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Vendredi").unwrap().collect();
        assert_eq!(results.len(), 1); //We'll still get Venerdi as a fuzzy match
        assert_eq!(table.keys_count(fri).unwrap(), 4);

        //Finally delete "Venerdi", and make sure the variants are all gone
        table.remove_keys(fri, &["Venerdi"]).unwrap();
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Vendredi").unwrap().collect();
        assert_eq!(results.len(), 0);
        assert_eq!(table.keys_count(fri).unwrap(), 3);

    }

    #[test]
    /// This test is tests some basic non-unicode key functionality.
    fn non_unicode_key_test() {

        let mut table = Table::<f32, 1, 8, false>::new("test2.rocks").unwrap();
        table.reset().unwrap();

        let one = table.insert(b"One", &1.0).unwrap();
        let _two = table.insert(b"Dos", &2.0).unwrap();
        let _three = table.insert(b"San", &3.0).unwrap();
        let pi = table.insert(b"Pi", &3.1415926535).unwrap();

        let result = table.lookup_best(b"P", table.default_distance_func()).unwrap();
        assert_eq!(result, pi);
        
        let results : Vec<RecordID> = table.lookup_fuzzy_raw(b"ne").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], one);
    }
}


//GOATGOATGOAT
//Next, Add multi-key support
//√ 1.) Function to add a key to a record
//√ 2.) Function to remove a key from a record
//√ 3.) Function to insert a record with multiple keys
//√ 4.) Separate out records table into keys table and values table
//√ 5.) Separate calls to get a value and get an iterator for keys
//√ 6.) Update test to insert keys for every alternative in the Geonames test
//√ 7.) Will deprecate get_record that gets both a key and a value
//√ 8.) Function to replace all keys on a record
//√ 9.) Get rid of is_valid()
//√ 10.) provide convenience fucntion called simply "get"
//√ 11.) API that counts the number of keys that a given record has

//GOATGOATGOAT, Move "BinCode Helpers" into separate file

//GOATGOATGOAT, Do a separate test for a ValueT of size 0

//GOATGOATGOAT, Clippy, and update documentation
