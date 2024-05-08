//!
//! The Table module contains the main [Table] object
//! 

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;

use num_traits::Zero;
use serde::Serialize;

use super::records::RecordID;
use super::key::*;
use super::database::*;
use super::table_config::*;
use super::sym_spell::*;
use super::key_groups::*;
use super::perf_counters::*;
use crate::Coder;

/// A collection containing records that may be searched using [Key]s
///
/// IMPLEMENTATION NOTE: Currently Rust doesn't let us bound an impl by an associated constant.  In other words
/// the bound `ConfigT : TableConfig<UTF8_KEYS = true>` won't work, and we need to reflect the UTF8_KEYS associated
/// constant as a const generic parameter on Table.  This is tracked by <https://github.com/rust-lang/rust/issues/70256>
/// 
/// When the following Rust language issues are resolved, I will remove the UTF8_KEYS generic constant from Table
/// - <https://github.com/rust-lang/rust/issues/44580>
/// - <https://github.com/rust-lang/rust/issues/70256>
/// - <https://github.com/rust-lang/rust/issues/76560>
/// 
/// In the meantime, the UTF8_KEYS generic constant set for the table must match the value in the config parameter.
pub struct Table<ConfigT : TableConfig, const UTF8_KEYS : bool> {
    record_count : usize,
    db : DBConnection<ConfigT::CoderT>,
    coder: ConfigT::CoderT,
    config : ConfigT,
    deleted_records : Vec<RecordID>, //NOTE: Currently we don't try to hold onto deleted records across unloads, but we may change this in the future.
    perf_counters : PerfCounters,
}

/// A private trait implemented by a [Table] to provide access to the keys in the DB, 
/// whether they are UTF-8 encoded strings or arrays of KeyCharT
/// 
/// NOTE: This is the "Yandros Type-Lifter" pattern, invented by Yandros here:
/// https://users.rust-lang.org/t/conditional-compilation-based-on-generic-constant/61131/5
pub trait TableKeyEncoding {
    type OwnedKeyT : OwnedKey;
}
impl <ConfigT : TableConfig>TableKeyEncoding for Table<ConfigT, true> {
    type OwnedKeyT = String;
}
impl <ConfigT : TableConfig>TableKeyEncoding for Table<ConfigT, false> {
    type OwnedKeyT = Vec<ConfigT::KeyCharT>;
}

/// The implementation of the shared parts of Table, that are the same regardless of UTF8_KEYS
impl <OwnedKeyT, ConfigT : TableConfig, const UTF8_KEYS : bool>Table<ConfigT, UTF8_KEYS>
    where
    ConfigT::KeyCharT : 'static + Copy + PartialEq + Serialize + serde::de::DeserializeOwned,
    ConfigT::DistanceT : 'static + Copy + Zero + PartialOrd + PartialEq + From<u8>,
    ConfigT::ValueT : 'static + Serialize + serde::de::DeserializeOwned,
    OwnedKeyT : OwnedKey<KeyCharT = ConfigT::KeyCharT>,
    Self : TableKeyEncoding<OwnedKeyT = OwnedKeyT>,
    {

    /// Creates a new Table, backed by the database at the path provided
    /// 
    /// WARNING:  No sanity checks are performed to ensure the database being opened matches the parameters
    /// of the table being created.  Therefore you may see bugs if you are opening a table that was created
    /// using a different set of parameters.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn new(path : &str, config : ConfigT) -> Result<Self, String> {

        //Make sure the Config agrees with the UTF8_KEYS const generic param
        //NOTE: this check will be unnecessary when Rust lets us bound an impl by an associated constant
        if UTF8_KEYS != ConfigT::UTF8_KEYS {
            panic!("Config Error! UTF8_KEYS generic constant must match ConfigT");
        }

        //Open the Database
        let mut db = DBConnection::new(ConfigT::CoderT::new(), path)?;

        let version = match db.get_version() {
            Ok(v) => v,
            Err(_) => {
              db.put_version()?;
              env!("CARGO_PKG_VERSION").to_owned()
            },
        };

        if version != env!("CARGO_PKG_VERSION") {
            panic!(
                "Table was created with incompatible version of {} - {}",
                env!("CARGO_CRATE_NAME"),
                env!("CARGO_PKG_VERSION")
            )
        }

        //Find the next value for new RecordIDs, by probing the entries in the "rec_data" column family
        let record_count = db.record_count()?;

        Ok(Self {
            record_count,
            config,
            db,
            coder: ConfigT::CoderT::new(),
            deleted_records : vec![],
            perf_counters : PerfCounters::new(),
        })
    }

    pub fn get_version(&self) -> Result<String, String> {
        self.db.get_version()
    }

    /// Resets a Table, dropping every record in the table and restoring it to an empty state.
    /// 
    /// (Dropping in a database sense, not a Rust sense)
    pub fn reset(&mut self) -> Result<(), String> {

        //Reset the database
        self.db.reset_database()?;

        //Reset the record_count, so newly inserted entries begin at 0 again
        self.record_count = 0;
        Ok(())
    }

    /// Deletes a record from the Table.
    /// 
    /// A deleted record cannot be accessed or otherwise found.  All of the record's associated keys
    /// and the associated value may be purged from the database. 
    pub fn delete(&mut self, record_id : RecordID) -> Result<(), String> {

        self.delete_keys_internal(record_id)?;
        self.db.delete_value(record_id)?;
        self.deleted_records.push(record_id);

        Ok(())
    }

    /// Deletes all of the keys belonging to a record, and all associated variants
    /// 
    /// Leaves the record in a half-composed state, so should only be called as part of another
    /// operation.
    fn delete_keys_internal(&mut self, record_id : RecordID) -> Result<(), String> {

        //Get all of the key-groups belonging to the record
        for key_group in self.db.get_record_key_groups(record_id)? {

            //Get all the keys for the group we're removing, so we can compute all the variants
            let keys_iter = self.db.get_keys_in_group::<<Self as TableKeyEncoding>::OwnedKeyT>(key_group, &self.perf_counters)?;
            let mut variants = HashSet::new();
            for key in keys_iter {
                let key_variants = SymSpell::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS >::variants(&key, &self.config);
                variants.extend(key_variants);
            }

            //Remove the variants' reference to this key group
            self.db.delete_variant_references(key_group, variants)?;
            
            //Delete the key group entry in the table
            self.db.delete_key_group_entry(key_group)?;
        }

        //Now replace the key groups vec in the "rec_data" table with an empty sentinel vec
        //NOTE: We replace the record rather than delete it because we assume there are no gaps in the
        // RecordIDs, when assigning new a RecordID
        self.db.put_record_key_groups(record_id, &[])?;

        Ok(())
    }

    /// Divides the keys up into key groups and assigns them to a record.
    ///
    /// Should NEVER be called on a record that already has keys or orphaned database entries will result
    fn put_record_keys<'a, K, KeysIterT : Iterator<Item=&'a K>>(&mut self, record_id : RecordID, keys_iter : KeysIterT, num_keys : usize) -> Result<(), String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT> + 'a
    {

        //Make groups for the keys
        let groups = KeyGroups::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS>::make_groups_from_keys(keys_iter, num_keys, &self.config).unwrap();
        let num_groups = groups.key_group_keys.len();

        //Put the variants for each group into the right table
        for (idx, variant_set) in groups.key_group_variants.into_iter().enumerate() {
            let key_group_id = KeyGroupID::from_record_and_idx(record_id, idx); 
            self.db.put_variant_references(key_group_id, variant_set)?;
        }

        //Put the keys for each group into the table
        for (idx, key_set) in groups.key_group_keys.into_iter().enumerate() {
            let key_group_id = KeyGroupID::from_record_and_idx(record_id, idx); 
            self.db.put_key_group_entry(key_group_id, &key_set)?;
        }

        //Put the key group record into the rec_data table
        let group_indices : Vec<usize> = (0..num_groups).into_iter().collect();
        self.db.put_record_key_groups(record_id, &group_indices[..])
    }

    /// Add additional keys to a record, including creation of all associated variants
    fn add_keys_internal<'a, K, KeysIterT : Iterator<Item=&'a K>>(&mut self, record_id : RecordID, keys_iter : KeysIterT, num_keys : usize) -> Result<(), String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT> + 'a
    {

        //Get the record's existing key groups and variants, so we can figure out the
        //best places for each additional new key
        let mut groups = KeyGroups::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS>::load_key_groups(&self.db, record_id, &self.config, &self.perf_counters)?;

        //Clone the existing groups, so we can determine which variants were added where
        let existing_groups_variants = groups.key_group_variants.clone();

        //Go over each key and add it to the key groups,
        // This add_key_to_groups function encapsulates the logic to add each key to
        // the correct group or create a new group
        for (key_idx, key) in keys_iter.enumerate() {
            let update_reverse_index = key_idx < num_keys-1;
            groups.add_key_to_groups(key, update_reverse_index, &self.config)?;
        }

        //Go over each group, work out the variants we need to add, then add them and update the group
        let empty_set = HashSet::new();
        for (group_idx, keys_set) in groups.key_group_keys.iter().enumerate() {

            //Get the variants for the group, as it existed prior to adding any keys
            let existing_keys_variants = match existing_groups_variants.get(group_idx) {
                Some(variant_set) => variant_set,
                None => &empty_set
            };

            //Calculate the set of variants that is unique to the keys we'll be adding
            let mut unique_keys_variants = HashSet::new();
            for unique_keys_variant in groups.key_group_variants[group_idx].difference(existing_keys_variants) {
                unique_keys_variants.insert(unique_keys_variant.to_owned());
            }

            //Add the key_group_id to the appropriate entries for each of the new variants
            let key_group_id = KeyGroupID::from_record_and_idx(record_id, groups.group_ids[group_idx]);
            self.db.put_variant_references(key_group_id, unique_keys_variants)?;

            //Add the new keys to the key group's entry in the keys table by replacing the keys vector
            // with the superset
            self.db.put_key_group_entry(key_group_id, keys_set)?;
        }

        //Put the new rec_data entry, to reflect all the associated key groups
        self.db.put_record_key_groups(record_id, &groups.group_ids[..])
    }

    /// Removes the specified keys from the keys associated with a record
    /// 
    /// If one of the specified keys is not associated with the record then that specified
    /// key will be ignored.
    fn remove_keys_internal<K>(&mut self, record_id : RecordID, remove_keys : &HashSet<&K>) -> Result<(), String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT>
    {

        //Get all of the existing groups
        let group_ids : Vec<KeyGroupID> = self.db.get_record_key_groups(record_id)?.collect();

        //Go through each existing group, and build a HashSet containing the keys that
        // we will delete and the keys that will remain after the removal
        let mut remaining_key_count = 0;
        let mut deleted_group_keys_sets = vec![];
        let mut remaining_group_keys_sets = vec![];
        for key_group in group_ids.iter() {
            let mut deleted_keys = HashSet::new();
            let mut remaining_keys = HashSet::new();
            for existing_key in self.db.get_keys_in_group::<<Self as TableKeyEncoding>::OwnedKeyT>(*key_group, &self.perf_counters)? {

                //NOTE: We know this is safe because the unsafety comes from the fact that
                // query_key might borrow existing_key, which is temporary, while query_key's
                // lifetime is 'a, which is beyond this function.  However, query_key doesn't
                // outlast this unsafe scope and we know HashSet::contains won't hold onto it.
                let set_contains_key = unsafe {
                    let query_key = K::from_owned_unsafe(&existing_key);
                    remove_keys.contains(&query_key)
                };
                
                //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
                // let query_key = K::new_borrowed_from_owned(&existing_key);
                // let set_contains_key = remove_keys.contains(&query_key);
                
                if !set_contains_key {
                    remaining_keys.insert(existing_key);
                    remaining_key_count += 1;
                } else {
                    deleted_keys.insert(existing_key);
                }
            }
            deleted_group_keys_sets.push(deleted_keys);
            remaining_group_keys_sets.push(remaining_keys);
        }

        //If we're left with no remaining keys, we should throw an error because all records must
        //have at least one key
        if remaining_key_count < 1 {
            return Err("cannot remove all keys from record".to_string());
        }

        //Go through each group and update its keys and variants, or remove the group altogether
        let mut remaining_group_indices = vec![];
        for (idx, group_id) in group_ids.into_iter().enumerate() {

            //If we didn't remove any keys from this group, there is nothing to do here.
            if deleted_group_keys_sets[idx].is_empty() {
                remaining_group_indices.push(group_id.group_idx());
                continue;
            }

            //Compute all variants for the keys we're removing from this group
            let mut remove_keys_variants = HashSet::new();
            for remove_key in deleted_group_keys_sets[idx].iter() {
                let keys_variants = SymSpell::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS>::variants(remove_key, &self.config);
                remove_keys_variants.extend(keys_variants);
            }

            //Compute all the variants for the keys that must remain in the group
            let mut remaining_keys_variants = HashSet::new();
            for remaining_key in remaining_group_keys_sets[idx].iter() {
                let keys_variants = SymSpell::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS>::variants(remaining_key, &self.config);
                remaining_keys_variants.extend(keys_variants);
            }

            //Exclude all of the overlapping variants, leaving only the variants that are unique
            //to the keys we're removing
            let mut unique_keys_variants = HashSet::new();
            for unique_keys_variant in remove_keys_variants.difference(&remaining_keys_variants) {
                unique_keys_variants.insert(unique_keys_variant.to_owned());
            }

            //Delete our KeyGroupID from each variant's list, and remove the variant list if we made it empty
            self.db.delete_variant_references(group_id, unique_keys_variants)?;

            //Update or delete the group
            if remaining_group_keys_sets[idx].is_empty() {
                //Delete the group's keys record if we made the group empty
                self.db.delete_key_group_entry(group_id)?;
            } else {
                //Otherwise update the group's keys record
                self.db.put_key_group_entry(group_id, &remaining_group_keys_sets[idx])?;
                remaining_group_indices.push(group_id.group_idx());
            }
            
        }

        //Update the record's rec_data entry to reflect the new groups after deletion
        self.db.put_record_key_groups(record_id, &remaining_group_indices[..])
    }

    /// Replaces all of the keys in a record with the supplied keys
    fn replace_keys_internal<K>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT>
    {

        if keys.is_empty() {
            return Err("record must have at least one key".to_string());
        }
        if keys.iter().any(|key| key.num_chars() > MAX_KEY_LENGTH) {
            return Err("key length exceeds MAX_KEY_LENGTH".to_string());
        }

        //Delete the old keys
        self.delete_keys_internal(record_id)?;

        //Set the keys on the new record
        self.put_record_keys(record_id, keys.iter(), keys.len())
    }

    /// Replaces a record's value with the supplied value.  Returns the value that was replaced
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace_value(&mut self, record_id : RecordID, value : &ConfigT::ValueT) -> Result<ConfigT::ValueT, String> {

        let old_value = self.db.get_value(record_id)?;

        self.db.put_value(record_id, value)?;

        Ok(old_value)
    }

    /// Inserts a record into the Table, called by insert(), which is implemented differently depending
    /// on the UTF8_KEYS constant
    ///
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    fn insert_internal<'a, K, KeysIterT : Iterator<Item=&'a K>>(&mut self, keys_iter : KeysIterT, num_keys : usize, value : &ConfigT::ValueT) -> Result<RecordID, String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT> + 'a
    {

        if num_keys < 1 {
            return Err("record must have at least one key".to_string());
        }

        let new_record_id = match self.deleted_records.pop() {
            None => {
                //We'll be creating a new record, so get the next unique record_id
                let new_record_id = RecordID::from(self.record_count);
                self.record_count += 1;
                new_record_id
            },
            Some(record_id) => record_id
        };

        //Set the keys on the new record
        self.put_record_keys(new_record_id, keys_iter, num_keys)?;

        //Put the value into its appropriate table
        self.db.put_value(new_record_id, value)?;

        Ok(new_record_id)
    }

    /// Visits all possible candidate keys for a given fuzzy search key, based on config.max_deletes,
    /// and invokes the supplied closure for each candidate KeyGroup found.
    ///
    /// NOTE: The same group may be found via multiple variants.  It is the responsibility of
    /// the closure to avoid doing duplicate work.
    ///
    /// QUESTION: should the visitor closure be able to return a bool, to mean "stop" or "keep going"?
    fn visit_fuzzy_candidates<K, F : FnMut(KeyGroupID)>(&self, key : &K, mut visitor : F) -> Result<(), String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT>
    {

        if key.num_chars() > MAX_KEY_LENGTH {
            return Err("key length exceeds MAX_KEY_LENGTH".to_string());
        }

        //Create all of the potential variants based off of the "meaningful" part of the key
        let variants = SymSpell::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS>::variants(key, &self.config);

        #[cfg(feature = "perf_counters")]
        { self.perf_counters.update(|fields| fields.variant_lookup_count += variants.len() ); }

        //Check to see if we have entries in the "variants" database for any of the key variants
        self.db.visit_variants(variants, |variant_vec_bytes| {

            #[cfg(feature = "perf_counters")]
            {
                let num_key_group_ids = bincode_vec_fixint_len(variant_vec_bytes);
                let mut counter_fields = self.perf_counters.get();
                counter_fields.variant_load_count += 1;
                counter_fields.key_group_ref_count += num_key_group_ids;
                if counter_fields.max_variant_entry_refs < num_key_group_ids {
                    counter_fields.max_variant_entry_refs = num_key_group_ids;
                }
                self.perf_counters.set(counter_fields);
            }

            // Call the visitor for each KeyGroup we found
            let decoded_vec : Vec<KeyGroupID> = self.coder.decode_fmt2_from_bytes(variant_vec_bytes).unwrap();
            for key_group_id in decoded_vec {
                visitor(key_group_id);
            }
        })
    }

    fn lookup_fuzzy_raw_internal<K>(&self, key : &K) -> Result<impl Iterator<Item=RecordID>, String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT>
    {

        //Create a new HashSet to hold all of the RecordIDs that we find
        let mut result_set = HashSet::new(); //TODO, may want to allocate this with a non-zero capacity

        //Our visitor closure just puts the KeyGroup's RecordID into a HashSet
        let raw_visitor_closure = |key_group_id : KeyGroupID| {
            result_set.insert(key_group_id.record_id());
        };

        //Visit all the potential records
        self.visit_fuzzy_candidates(key, raw_visitor_closure)?;

        #[cfg(feature = "perf_counters")]
        { self.perf_counters.update(|fields| fields.records_found_count += result_set.len() ); }

        //Return an iterator through the HashSet we just made
        Ok(result_set.into_iter())
    }

    /// Returns an iterator over all RecordIDs and smallest distance values found with a fuzzy lookup
    /// after evaluating the supplied  distance function for every found candidate key.
    /// 
    /// NOTE: This function evaluates the distance function for all keys in advance of returning the
    /// iterator.  A lazy evaluation would be possible but would incur a sort of the KeyGroupIDs.
    /// This would be needed to ensure that the smallest distance value for a given record was returned.
    /// It would be necessary to evaluate every key group for a particular record before returning the
    /// record.  The decision not to do this is on account of the fact that [lookup_fuzzy_raw_internal]
    /// could be used instead if the caller wants a quick-to-return iterator.
    fn lookup_fuzzy_internal<K : Key<KeyCharT = ConfigT::KeyCharT>>(&self, key : &K, threshold : Option<ConfigT::DistanceT>) -> Result<impl Iterator<Item=(RecordID, ConfigT::DistanceT)>, String> {

        let distance_function = ConfigT::DISTANCE_FUNCTION;

        //Create a new HashMap to hold all of the RecordIDs that we might want to return, and the lowest
        // distance we find for that particular record
        let mut result_map = HashMap::new(); //TODO, may want to allocate this with a non-zero capacity
        let mut visited_groups = HashSet::new();

        //If we can borrow the lookup chars directly then do it, otherwise get them from a buffer
        let key_chars_vec;
        let looup_key_chars = if let Some(key_chars) = key.borrow_key_chars() {
            key_chars
        } else {
            key_chars_vec = key.get_key_chars();
            &key_chars_vec[..]
        };

        //pre-allocate the buffer we'll expand the key-chars into
        let mut key_chars_buf : Vec<ConfigT::KeyCharT> = Vec::with_capacity(MAX_KEY_LENGTH);

        //Our visitor closure tests the key using the distance function and threshold
        let lookup_fuzzy_visitor_closure = |key_group_id : KeyGroupID| {

            //QUESTION: Should we have an alternate fast path that only evaluates until we find
            // any distance smaller than threshold?  It would mean we couldn't return a reliable
            // distance but would save us evaluating distance for potentially many keys

            if !visited_groups.contains(&key_group_id) {

                //Check the record's keys with the distance function and find the smallest distance
                let mut record_keys_iter = self.db.get_keys_in_group::<<Self as TableKeyEncoding>::OwnedKeyT>(key_group_id, &self.perf_counters).unwrap();

                let record_key = record_keys_iter.next().unwrap(); //If we have a zero-element keys array, it's a bug elsewhere, so this unwrap should always succeed
                let record_key_chars = record_key.move_into_buf(&mut key_chars_buf);
                let mut smallest_distance = distance_function(&record_key_chars[..], looup_key_chars);
                #[cfg(feature = "perf_counters")]
                { self.perf_counters.update(|fields| fields.distance_function_invocation_count += 1); }

                for record_key in record_keys_iter {
                    let record_key_chars = record_key.move_into_buf(&mut key_chars_buf);
                    let distance = distance_function(record_key_chars, looup_key_chars);
                    if distance < smallest_distance {
                        smallest_distance = distance;
                    }

                    #[cfg(feature = "perf_counters")]
                    { self.perf_counters.update(|fields| fields.distance_function_invocation_count += 1); }    
                }

                let insert_distance = match threshold {
                    Some(threshold) if smallest_distance <= threshold => {
                        Some(smallest_distance)
                    }
                    None => Some(smallest_distance),
                    _ => None
                };

                if let Some(smallest_distance) = insert_distance {
                    match result_map.entry(key_group_id.record_id()) {
                        Entry::Occupied(mut entry) => {
                            let current_distance = *entry.get();
                            if smallest_distance < current_distance {
                                entry.insert(smallest_distance);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(smallest_distance);
                        }
                    }
                }

                //Record that we've visited this key group, we we can skip it if we encounter it
                //via a different variant
                visited_groups.insert(key_group_id);
            }
        };

        //Visit all the potential records
        self.visit_fuzzy_candidates(key, lookup_fuzzy_visitor_closure)?;

        #[cfg(feature = "perf_counters")]
        { self.perf_counters.update(|fields| fields.records_found_count += result_map.len() ); }

        //Return an iterator through the HashSet we just made
        Ok(result_map.into_iter())
    }

    fn lookup_best_internal<K : Key<KeyCharT = ConfigT::KeyCharT>>(&self, key : &K) -> Result<impl Iterator<Item=RecordID>, String> {

        //First, we should check to see if lookup_exact gives us what we want.  Because if it does,
        // it's muuuuuuch faster.  If we have an exact result, no other key will be a better match
        let mut results_vec = self.lookup_exact_internal(key)?;
        if !results_vec.is_empty() {
            return Ok(results_vec.into_iter());
        }
        
        //Assuming lookup_exact didn't work, we'll need to perform the whole fuzzy lookup and iterate each key
        //to figure out the closest distance
        let mut result_iter = self.lookup_fuzzy_internal(key, None)?;
        
        if let Some(first_result) = result_iter.next() {
            let mut best_distance = first_result.1;
            results_vec.push(first_result.0);
            for result in result_iter {
                if result.1 == best_distance {
                    results_vec.push(result.0);
                } else if result.1 < best_distance {
                    //We've found a shorter distance, so drop the results_vec and start a new one
                    best_distance = result.1;
                    results_vec = vec![result.0];
                }
            }

            return Ok(results_vec.into_iter());
        }

        Ok(vec![].into_iter())
    }

    /// Checks the table for records with keys that precisely match the key supplied
    /// 
    /// This function will be more efficient than a fuzzy lookup.
    fn lookup_exact_internal<K>(&self, lookup_key : &K) -> Result<Vec<RecordID>, String>
        where
        K : Key<KeyCharT = ConfigT::KeyCharT>
    {

        let lookup_key_len = lookup_key.num_chars();
        if lookup_key_len > MAX_KEY_LENGTH {
            return Err("key length exceeds MAX_KEY_LENGTH".to_string());
        }

        let meaningful_key = SymSpell::<<Self as TableKeyEncoding>::OwnedKeyT, UTF8_KEYS>::meaningful_key_substring(lookup_key, &self.config);

        //BUG!! This "meaningful_noop" code path is flawed!!!
        // A variant could point to a key group, without it being an exact match.  For example,
        // the key "londonia" would cause a reference to be created inside the variant entry of
        // "london".  Then an exact lookup on "london" would also return the Record for "londonia".
        //If there is a desire to keep this fast-path, we might have an "lookup_exactly_contains"
        // entry point or something, which reflects the behavior of the current code path
        let meaningful_noop = meaningful_key.num_chars() == lookup_key_len;

        #[cfg(feature = "perf_counters")]
        { self.perf_counters.update(|fields| fields.variant_lookup_count += 1 ); }

        //Get the variant for our meaningful_key
        let mut record_ids : Vec<RecordID> = vec![];
        self.db.visit_exact_variant(meaningful_key.as_bytes(), |variant_vec_bytes| {

            #[cfg(feature = "perf_counters")]
            {
                let num_key_group_ids = bincode_vec_fixint_len(variant_vec_bytes);
                self.perf_counters.update(|fields| { 
                    fields.variant_load_count += 1;
                    fields.key_group_ref_count += num_key_group_ids;
                } );
            }

            record_ids = if meaningful_noop {

                //If the meaningful_key exactly equals our key, we can just return the variant's results
                let decoded_vec : Vec<KeyGroupID> = self.coder.decode_fmt2_from_bytes(variant_vec_bytes).unwrap();
                decoded_vec.into_iter().map(|key_group_id| key_group_id.record_id()).collect()

            } else {

                //But if they are different, we need to Iterate every KeyGroupID in the variant in order
                //  to check if we really have a match on the whole key
                let owned_lookup_key = <Self as TableKeyEncoding>::OwnedKeyT::from_key(lookup_key);
                let decoded_vec : Vec<KeyGroupID> = self.coder.decode_fmt2_from_bytes(variant_vec_bytes).unwrap();
                decoded_vec.into_iter().filter_map(|key_group_id| {

                    // Return only the KeyGroupIDs for records if their keys match the key we are looking up
                    let mut keys_iter = self.db.get_keys_in_group::<<Self as TableKeyEncoding>::OwnedKeyT>(key_group_id, &self.perf_counters).ok()?;
                    if keys_iter.any(|key| key == owned_lookup_key) {
                        Some(key_group_id)
                    } else {
                        None
                    }
                }).map(|key_group_id| key_group_id.record_id()).collect()
            };
        })?;

        Ok(record_ids)
    }

    /// Returns the value associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_value(&self, record_id : RecordID) -> Result<ConfigT::ValueT, String> {
        self.db.get_value(record_id)
    }

    /// Returns the number of keys associated with a specified record
    pub fn keys_count(&self, record_id : RecordID) -> Result<usize, String> {

        let mut keys_count = 0;

        //Go over every key group associated with the record
        for key_group in self.db.get_record_key_groups(record_id)? {
            keys_count += self.db.keys_count_in_group(key_group)?;
        }

        Ok(keys_count as usize)
    }

    /// Returns all of the keys for a record, across all key groups
    fn get_keys_internal(&self, record_id : RecordID) -> Result<impl Iterator<Item=<Self as TableKeyEncoding>::OwnedKeyT> + '_, String> {

        let key_groups_iter = self.db.get_record_key_groups(record_id)?;
        let result_iter = key_groups_iter.flat_map(move |key_group| self.db.get_keys_in_group::<<Self as TableKeyEncoding>::OwnedKeyT>(key_group, &self.perf_counters).unwrap());

        Ok(result_iter)
    }

    /// Resets all values in the performance counters, so the information returned by [get_perf_counters](Table::get_perf_counters) only
    /// reflects activity since the last call to `reset_perf_counters`
    pub fn reset_perf_counters(&self) {
        self.perf_counters.reset();
    }

    /// Returns the values in the performance counters, which should reflect all activity since the previous call
    /// to [reset_perf_counters](Table::reset_perf_counters)
    pub fn get_perf_counters(&self) -> PerfCounterFields {
        self.perf_counters.get()
    }
}

impl <ConfigT : TableConfig<KeyCharT = char>>Table<ConfigT, true> {

    /// Inserts a new key-value pair into the table and returns the RecordID of the new record
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [create](Table::create)
    /// 
    /// NOTE: Inserting the same key into a Table multiple times will result in multiple distinct
    /// records, and all of the records will be found by lookups of that key.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn insert<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = char>>(&mut self, key : K, value : &ConfigT::ValueT) -> Result<RecordID, String> {
        self.insert_internal([&key.into_key()].iter().copied(), 1, value)
    }

    /// Retrieves a key-value pair using a RecordID
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [get_one_key](Table::get_one_key) / [get_value](Table::get_value)
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get(&self, record_id : RecordID) -> Result<(String, ConfigT::ValueT), String> {
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
    pub fn create<K : Key<KeyCharT = char>>(&mut self, keys : &[K], value : &ConfigT::ValueT) -> Result<RecordID, String> {
        self.insert_internal(keys.iter(), keys.len(), value)
    }

    /// Adds the supplied keys to the record's keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn add_keys<K : Key<KeyCharT = char>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        self.add_keys_internal(record_id, keys.iter(), keys.len())
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
    pub fn remove_keys<K : Key<KeyCharT = char>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&K> = HashSet::from_iter(keys.iter());
        self.remove_keys_internal(record_id, &keys_set)
    }

    /// Replaces a record's keys with the supplied keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace_keys<K : Key<KeyCharT = char>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        self.replace_keys_internal(record_id, keys)
    }

    /// Returns an iterator over all of the key associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_keys(&self, record_id : RecordID) -> Result<impl Iterator<Item=String> + '_, String> {
        self.get_keys_internal(record_id)
    }

    /// Returns one key associated with the specified record.  If the record has more than one key
    /// then which key is unspecified
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_one_key(&self, record_id : RecordID) -> Result<String, String> {
        //TODO: Perhaps we can speed this up in the future by avoiding deserializing all keys
        //NOTE: With the Key Groups architecture and the lazy nature of iterators, we'll now only
        // deserialize the keys in one key group. So perhaps that's good enough.  On the downside, we
        // now pull the rec_data entry to get the index of the first key group.  99 times out of 100,
        // we'd be able to sucessfully guess the key group entry by looking at entry 0, and could then
        // fall back to checking the rec_data entry if that failed.  Depends on whether we want the
        // last ounce of performance from this function or not.
        let first_key = self.get_keys_internal(record_id)?.next().unwrap();
        Ok(first_key)
    }

    /// Locates all records in the table with keys that precisely match the key supplied
    /// 
    /// BUG!!: In some cases, this function will return records that have keys that contain
    /// the key searched.  For example, the search key "london" may return a record associated
    /// with the key "londonia".  We may opt to keep this functionality as a separate function
    /// as its performance is better than as loading the exact keys for all records is
    /// significant overhead.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_exact<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = char>>(&self, key : K) -> Result<impl Iterator<Item=RecordID>, String> {
        self.lookup_exact_internal(&key.into_key()).map(|result_vec| result_vec.into_iter())
    }

    /// Locates all records in the table with a key that is within a deletion distance of [config.max_deletes] of
    /// the key supplied, based on the SymSpell algorithm.
    /// 
    /// This function underlies all fuzzy lookups, and does no further filtering based on any distance function.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy_raw<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = char>>(&self, key : K) -> Result<impl Iterator<Item=RecordID>, String> {
        self.lookup_fuzzy_raw_internal(&key.into_key())
    }

    /// Locates all records in the table for which the Table's DISTANCE_FUNCTION(TableConfig::DISTANCE_FUNCTION) evaluates to a result smaller
    /// than the supplied `threshold` when comparing the record's key with the supplied `key`
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = char>>(&self, key : K, threshold : Option<ConfigT::DistanceT>) -> Result<impl Iterator<Item=(RecordID, ConfigT::DistanceT)>, String> {
        self.lookup_fuzzy_internal(&key.into_key(), threshold)
    }

    /// Locates the record in the table for which the Table's DISTANCE_FUNCTION(TableConfig::DISTANCE_FUNCTION) evaluates to the lowest value
    /// when comparing the record's key with the supplied `key`.
    /// 
    /// If no matching record is found within the table's `config.max_deletes`, this method will return an error.
    /// 
    /// NOTE: If two or more results have the same returned distance value and that is the smallest value, the
    /// implementation does not specify which result will be returned.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_best<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = char>>(&self, key : K) -> Result<impl Iterator<Item=RecordID>, String> {
        self.lookup_best_internal(&key.into_key())
    }
}

impl <ConfigT : TableConfig>Table<ConfigT, false> {

    /// Inserts a new key-value pair into the table and returns the RecordID of the new record
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [create](Table::create)
    /// 
    /// NOTE: Inserting the same key into a Table multiple times will result in multiple distinct
    /// records, and all of the records will be found by lookups of that key.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn insert<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = ConfigT::KeyCharT>>(&mut self, key : K, value : &ConfigT::ValueT) -> Result<RecordID, String> {
        self.insert_internal([&key.into_key()].iter().copied(), 1, value)
    }

    /// Retrieves a key-value pair using a RecordID
    /// 
    /// This is a high-level interface to be used if multiple keys are not needed, but is
    /// functions the same as [get_one_key](Table::get_one_key) / [get_value](Table::get_value)
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get(&self, record_id : RecordID) -> Result<(Vec<ConfigT::KeyCharT>, ConfigT::ValueT), String> {
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
    pub fn create<K : Key<KeyCharT = ConfigT::KeyCharT>>(&mut self, keys : &[K], value : &ConfigT::ValueT) -> Result<RecordID, String> {
        self.insert_internal(keys.iter(), keys.len(), value)
    }

    /// Adds the supplied keys to the record's keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn add_keys<K : Key<KeyCharT = ConfigT::KeyCharT>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        self.add_keys_internal(record_id, keys.iter(), keys.len())
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
    pub fn remove_keys<K : Key<KeyCharT = ConfigT::KeyCharT>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        let keys_set : HashSet<&K> = HashSet::from_iter(keys.iter());
        self.remove_keys_internal(record_id, &keys_set)
    }

    /// Replaces a record's keys with the supplied keys
    /// 
    /// The supplied `record_id` must references an existing record that has not been deleted.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace_keys<K : Key<KeyCharT = ConfigT::KeyCharT>>(&mut self, record_id : RecordID, keys : &[K]) -> Result<(), String> {
        self.replace_keys_internal(record_id, keys)
    }

    /// Returns an iterator over all of the key associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_keys(&self, record_id : RecordID) -> Result<impl Iterator<Item=Vec<ConfigT::KeyCharT>> + '_, String> {
        self.get_keys_internal(record_id)
    }

    /// Returns one key associated with the specified record.  If the record has more than one key
    /// then which key is unspecified
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_one_key(&self, record_id : RecordID) -> Result<Vec<ConfigT::KeyCharT>, String> {
        //TODO: Perhaps we can speed this up in the future by avoiding deserializing all keys
        Ok(self.get_keys_internal(record_id)?.next().unwrap())
    }

    /// Locates all records in the table with keys that precisely match the key supplied
    /// 
    /// BUG!!: In some cases, this function will return records that have keys that contain
    /// the key searched.  For example, the search key "london" may return a record associated
    /// with the key "londonia".  We may opt to keep this functionality as a separate function
    /// as its performance is better than as loading the exact keys for all records is
    /// significant overhead.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_exact<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = ConfigT::KeyCharT>>(&self, key : K) -> Result<impl Iterator<Item=RecordID>, String> {
        self.lookup_exact_internal(&key.into_key()).map(|result_vec| result_vec.into_iter())
    }

    /// Locates all records in the table with a key that is within a deletion distance of `config.max_deletes` of
    /// the key supplied, based on the SymSpell algorithm.
    /// 
    /// This function underlies all fuzzy lookups, and does no further filtering based on any distance function.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy_raw<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = ConfigT::KeyCharT>>(&self, key : K) -> Result<impl Iterator<Item=RecordID>, String> {
        self.lookup_fuzzy_raw_internal(&key.into_key())
    }

    /// Locates all records in the table for which the Table's DISTANCE_FUNCTION(TableConfig::DISTANCE_FUNCTION) evaluates to a result smaller
    /// than the supplied `threshold` when comparing the record's key with the supplied `key`
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_fuzzy<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = ConfigT::KeyCharT>>(&self, key : K, threshold : Option<ConfigT::DistanceT>) -> Result<impl Iterator<Item=(RecordID, ConfigT::DistanceT)>, String> {
        self.lookup_fuzzy_internal(&key.into_key(), threshold)
    }

    /// Locates the record in the table for which the Table's DISTANCE_FUNCTION(TableConfig::DISTANCE_FUNCTION) evaluates to the lowest value
    /// when comparing the record's key with the supplied `key`.
    /// 
    /// If no matching record is found within the table's `config.max_deletes`, this method will return an error.
    /// 
    /// NOTE: If two or more results have the same returned distance value and that is the smallest value, the
    /// implementation does not specify which result will be returned.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn lookup_best<K : IntoKey<Key = KeyT>, KeyT : Key<KeyCharT = ConfigT::KeyCharT>>(&self, key : K) -> Result<impl Iterator<Item=RecordID>, String> {
        self.lookup_best_internal(&key.into_key())
    }
}
