//!
//! The Database module contains wrappers around the database (RocksDB) connection, and the
//! functions for getting and setting records in the DB.  Nothing should be re-exported.
//!

use core::hash::Hash;

use std::collections::HashSet;
use serde::Serialize;

use rocksdb::{DB, DBWithThreadMode, ColumnFamily, ColumnFamilyDescriptor, MergeOperands};

use crate::table_config::TableMetadata;
use crate::TableConfig;
#[cfg(feature = "bitcode")]
use crate::CONST_CODER;

use super::encode_decode::Coder;

use super::records::{*};
use super::key_groups::{*};
use super::perf_counters::{*};

/// The ColumnFamily names used for the different types of data
pub const KEYS_CF_NAME : &str = "keys";
pub const RECORD_DATA_CF_NAME : &str = "rec_data";
pub const VALUES_CF_NAME : &str = "values";
pub const METADATA_CF_NAME : &str = "metadata";
pub const VARIANTS_CF_NAME : &str = "variants";

/// Encapsulates a connection to a database
pub struct DBConnection<C: Coder + Send + Sync> {
    db : DBWithThreadMode<rocksdb::SingleThreaded>,
    path : String,
    coder: C,
}

impl<C: Coder> DBConnection<C> {

    pub fn new(coder: C, path : &str) -> Result<Self, String> {

        //Configure the "keys" and "values" column families
        let keys_cf = ColumnFamilyDescriptor::new(KEYS_CF_NAME, rocksdb::Options::default());
        let rec_data_cf = ColumnFamilyDescriptor::new(RECORD_DATA_CF_NAME, rocksdb::Options::default());
        let values_cf = ColumnFamilyDescriptor::new(VALUES_CF_NAME, rocksdb::Options::default());
        let version_cf = ColumnFamilyDescriptor::new(METADATA_CF_NAME, rocksdb::Options::default());

        //Configure the "variants" column family
        let mut variants_opts = rocksdb::Options::default();
        variants_opts.create_if_missing(true);
        let coder_clone = coder.clone();
        variants_opts.set_merge_operator_associative("append to RecordID vec",
            move |key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands| -> Option<Vec<u8>> {
                variant_append_merge(&coder_clone, key, existing_val, operands)
        });
        let variants_cf = ColumnFamilyDescriptor::new(VARIANTS_CF_NAME, variants_opts);

        //Configure the database itself
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        //Open the database
        let db = DB::open_cf_descriptors(&db_opts, path, vec![keys_cf, rec_data_cf, values_cf, variants_cf, version_cf])?;

        Ok(Self{
            db,
            path : path.to_string(),
            coder
        })
    }

    ///Deletes all entries associated with a database and resets it to a fresh state
    pub fn reset_database(&mut self) -> Result<(), String> {

        //Drop all the existing column families
        self.db.drop_cf(KEYS_CF_NAME)?;
        self.db.drop_cf(RECORD_DATA_CF_NAME)?;
        self.db.drop_cf(VALUES_CF_NAME)?;
        self.db.drop_cf(VARIANTS_CF_NAME)?;

        //Recreate the "keys", "rec_data", and "values" column families
        self.db.create_cf(KEYS_CF_NAME, &rocksdb::Options::default())?;
        self.db.create_cf(RECORD_DATA_CF_NAME, &rocksdb::Options::default())?;
        self.db.create_cf(VALUES_CF_NAME, &rocksdb::Options::default())?;

        //Recreate the "variants" column family
        let mut variants_opts = rocksdb::Options::default();
        variants_opts.create_if_missing(true);
        let coder_clone = self.coder.clone();
        variants_opts.set_merge_operator_associative("append to RecordID vec",
            move |key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands| -> Option<Vec<u8>> {
                variant_append_merge(&coder_clone, key, existing_val, operands)
        });
        self.db.create_cf(VARIANTS_CF_NAME, &variants_opts)?;

        Ok(())
    }

    ///Returns the number of record entries in the database, by probing the entries in the
    /// "rec_data" column family
    ///
    ///NOTE: this is not a simple lookup, and is designed to be called when loading a new table, not
    /// as a simple accessor
    pub fn record_count(&self) -> Result<usize, String> {
        let rec_data_cf_handle = self.db.cf_handle(RECORD_DATA_CF_NAME).unwrap();
        let record_count = probe_for_max_sequential_key(&self.db, rec_data_cf_handle, 255)?;
        Ok(record_count)
    }

    /// Returns `true` if the table associated with the DBConnection has no entries.  Otherwise `false`
    pub fn table_is_empty(&self) -> Result<bool, String> {
        let rec_data_cf_handle = self.db.cf_handle(RECORD_DATA_CF_NAME).unwrap();
        Ok(self.db.get_pinned_cf(rec_data_cf_handle, RecordID(0).to_le_bytes())?.is_none())
    }

    /// Returns an iterator for every key group associated with a specified record
    ///
    /// Internal FuzzyRocks interface, but exported outside the key_groups module
    #[inline(always)]
    pub fn get_record_key_groups(&self, record_id : RecordID) -> Result<impl Iterator<Item=KeyGroupID>, String> {

        let rec_data_cf_handle = self.db.cf_handle(RECORD_DATA_CF_NAME).unwrap();
        if let Some(rec_data_vec_bytes) = self.db.get_pinned_cf(rec_data_cf_handle, record_id.to_le_bytes())? {
            let rec_data : RecordData = self.coder.decode_fmt1_from_bytes(&rec_data_vec_bytes).unwrap();

            if !rec_data.key_groups.is_empty() {
                Ok(rec_data.key_groups.into_iter().map(move |group_idx| KeyGroupID::from_record_and_idx(record_id, group_idx)))
            } else {
                Err("Invalid record_id".to_string())
            }
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Replaces the key groups in the specified record with the provided vec
    /// 
    /// Internal FuzzyRocks interface, but exported outside the key_groups module
    pub fn put_record_key_groups(&self, record_id : RecordID, key_groups_vec : &[usize]) -> Result<(), String> {

        //Create the RecordData, serialize it, and put in into the rec_data table.
        let rec_data_cf_handle = self.db.cf_handle(RECORD_DATA_CF_NAME).unwrap();
        let new_rec_data = RecordData::new(key_groups_vec);
        let rec_data_bytes = self.coder.encode_fmt1_to_buf(&new_rec_data).unwrap();
        self.db.put_cf(rec_data_cf_handle, record_id.to_le_bytes(), rec_data_bytes)?;

        Ok(())
    }

    /// Returns the keys associated with a single key group of a single specified record
    #[inline(always)]
    #[allow(unused_variables)] //NOTE: To silence the warning about perf_counters when that code path is disabled
    pub fn get_keys_in_group<OwnedKeyT : 'static + Sized + Serialize + serde::de::DeserializeOwned>(&self, key_group : KeyGroupID, perf_counters : &PerfCounters) -> Result<impl Iterator<Item=OwnedKeyT>, String> {

        //Get the keys vec by deserializing the bytes from the db
        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        if let Some(keys_vec_bytes) = self.db.get_pinned_cf(keys_cf_handle, key_group.to_le_bytes())? {
            let keys_vec : Vec<OwnedKeyT> = self.coder.decode_fmt1_from_bytes(&keys_vec_bytes).unwrap();

            #[cfg(feature = "perf_counters")]
            {
                let mut counter_fields = perf_counters.get();
                counter_fields.key_group_load_count += 1;
                counter_fields.keys_found_count += keys_vec.len();
                perf_counters.set(counter_fields);
            }

            if !keys_vec.is_empty() {
                Ok(keys_vec.into_iter())
            } else {
                Err("Invalid record_id".to_string())
            }
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Returns the number of keys in a key group, without returning the keys themselves
    ///
    /// NOTE: This is intended to be faster than get_keys_in_group, but it's unclear if it actually
    /// saves much as RocksDB still loads the whole entry from the DB, even though the count is
    /// stored in the first few bytes.
    #[inline(always)]
    pub fn keys_count_in_group(&self, key_group : KeyGroupID) -> Result<usize, String> {

        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        if let Some(keys_vec_bytes) = self.db.get_pinned_cf(keys_cf_handle, key_group.to_le_bytes())? {
            Ok(self.coder.fmt1_list_len(&keys_vec_bytes).unwrap())
        } else {
            panic!(); //If we hit this, we have a corrupt DB
        }
    }

    /// Creates entries in the keys table.  If we are updating an old record, we will overwrite it.
    ///
    /// NOTE: This function will NOT update any variants used to locate the key
    pub fn put_key_group_entry<K : Eq + Hash + Serialize>(&mut self, key_group_id : KeyGroupID, raw_keys : &HashSet<K>) -> Result<(), String> {

        //Serialize the keys into a vec of bytes
        let keys_bytes = self.coder.encode_fmt1_list_to_buf(&raw_keys).unwrap();

        //Put the vector of keys into the keys table
        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        self.db.put_cf(keys_cf_handle, key_group_id.to_le_bytes(), keys_bytes)?;

        Ok(())
    }

    /// Deletes a key group entry from the db.  Does not clean up variants that may reference
    /// the key group, so must be called as part of another operation
    pub fn delete_key_group_entry(&mut self, key_group : KeyGroupID) -> Result<(), String> {

        let keys_cf_handle = self.db.cf_handle(KEYS_CF_NAME).unwrap();
        self.db.delete_cf(keys_cf_handle, key_group.to_le_bytes())?;

        Ok(())
    }

    /// Returns the value associated with the specified record
    #[inline(always)]
    pub fn get_value<ValueT : 'static + Serialize + serde::de::DeserializeOwned>(&self, record_id : RecordID) -> Result<ValueT, String> {

        //Get the value object by deserializing the bytes from the db
        let values_cf_handle = self.db.cf_handle(VALUES_CF_NAME).unwrap();
        if let Some(value_bytes) = self.db.get_pinned_cf(values_cf_handle, record_id.to_le_bytes())? {
            #[cfg(not(feature = "bitcode"))]
            let value : ValueT = self.coder.decode_fmt1_from_bytes(&value_bytes).unwrap();
            #[cfg(feature = "bitcode")]
            let value : ValueT = CONST_CODER.decode_fmt1_from_bytes(&value_bytes).unwrap();

            Ok(value)
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Returns the version of the crate this DB was created with
    pub fn get_version(&self) -> Result<String, String> {

        //Get the value object by deserializing the bytes from the db
        let version_cf_handle = self.db.cf_handle(METADATA_CF_NAME).unwrap();
        if let Some(value_bytes) = self.db.get_pinned_cf(version_cf_handle, [])? {
            let value = String::from_utf8(value_bytes.to_owned()).unwrap();
            Ok(value)
        } else {
            Err("Error: No config metadata found for table.  The table was likely built with an old version of fuzzy_rocks".to_string())
        }
    }

    /// Puts current crate version into metadata table
    pub fn put_version(&mut self) -> Result<(), String> {
        let value_cf_handle = self.db.cf_handle(METADATA_CF_NAME).unwrap();
        let value_bytes = env!("CARGO_PKG_VERSION").as_bytes();
        self.db.put_cf(value_cf_handle, [], value_bytes)?;

        Ok(())
    }

    /// Returns the table config this DB was created with
    pub fn get_config(&self) -> Result<TableMetadata, String> {

        //Get the value object by deserializing the bytes from the db
        let version_cf_handle = self.db.cf_handle(METADATA_CF_NAME).unwrap();
        if let Some(value_bytes) = self.db.get_pinned_cf(version_cf_handle, [1])? {
            let value = self.coder.decode_fmt1_from_bytes(&value_bytes).unwrap();
            Ok(value)
        } else {
            Err("Error: No config metadata found for table.  The table was likely built with an old version of fuzzy_rocks".to_string())
        }
    }

    /// Puts current DB config into metadata table
    pub fn put_config<ConfigT : TableConfig>(&mut self) -> Result<(), String> {
        let value_cf_handle = self.db.cf_handle(METADATA_CF_NAME).unwrap();
        let config = ConfigT::metadata();
        let value_bytes = self.coder.encode_fmt1_to_buf(&config).unwrap();
        self.db.put_cf(value_cf_handle, [1], value_bytes)?;

        Ok(())
    }

    /// Deletes a record's value in the values table
    /// 
    /// This should only be called as part of another operation as it leaves the record in an
    /// inconsistent state
    pub fn delete_value(&mut self, record_id : RecordID) -> Result<(), String> {

        let value_cf_handle = self.db.cf_handle(VALUES_CF_NAME).unwrap();
        self.db.delete_cf(value_cf_handle, record_id.to_le_bytes())?;

        Ok(())
    }

    /// Creates entries in the values table
    /// If we are updating an old record, we will overwrite it.
    /// 
    /// NOTE: This function will NOT update any variants used to locate the key
    pub fn put_value<ValueT : 'static + Serialize + serde::de::DeserializeOwned>(&mut self, record_id : RecordID, value : &ValueT) -> Result<(), String> {

        //Serialize the value and put it in the values table.
        let value_cf_handle = self.db.cf_handle(VALUES_CF_NAME).unwrap();
        #[cfg(not(feature = "bitcode"))]
        let value_bytes = self.coder.encode_fmt1_to_buf(value).unwrap();
        #[cfg(feature = "bitcode")]
        let value_bytes = CONST_CODER.encode_fmt1_to_buf(value).unwrap();
        self.db.put_cf(value_cf_handle, record_id.to_le_bytes(), value_bytes)?;

        Ok(())
    }

    /// Executes a provided closure for every variant entry that exists from the provided set
    ///
    /// NOTE: The closure gets the raw entry bytes, rather than the parsed KeyGroupIDs
    /// because sometimes we don't want to parse the whole entry.  Also it takes a set of variants
    /// so we don't need to create the CFHandle every time as we would with a simple "get_variant"
    /// function
    #[inline(always)]
    pub fn visit_variants<F : FnMut(&[u8])>(&self, variants : HashSet<Vec<u8>>, mut visitor_closure : F) -> Result<(), String> {

        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants {

            // See if we have an entry in the "variants" database for the supplied key variant
            if let Some(variant_vec_bytes) = self.db.get_pinned_cf(variants_cf_handle, variant)? {

                visitor_closure(&variant_vec_bytes);
            }
        }

        Ok(())
    }

    /// Visits the only the exact variant specified from the database and executes a closure.
    #[inline(always)]
    pub fn visit_exact_variant<F : FnMut(&[u8])>(&self, variant : &[u8], mut visitor_closure : F) -> Result<(), String> {

        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        if let Some(variant_vec_bytes) = self.db.get_pinned_cf(variants_cf_handle, variant)? {

            visitor_closure(&variant_vec_bytes);
        }

        Ok(())
    }

    /// Deletes references to a specified key group from a number of specified variant entries.
    ///
    /// If the variant references no key groups after deletion then the variant entry is deleted
    pub fn delete_variant_references(&mut self, key_group : KeyGroupID, variants : HashSet<Vec<u8>>) -> Result<(), String> {

        //Loop over each variant, and remove the KeyGroupID from its associated variant entry in
        // the database, and remove the variant entry if it only referenced the key_group we're removing
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants.iter() {

            if let Some(variant_entry_bytes) = self.db.get_pinned_cf(variants_cf_handle, variant)? {

                let variant_entry_len = self.coder.fmt2_list_len(&variant_entry_bytes).unwrap();

                //If the variant entry references more than one record, rebuild it with our records absent
                if variant_entry_len > 1 {
                    let mut new_vec : Vec<KeyGroupID> = Vec::with_capacity(variant_entry_len-1);
                    let decoded_vec : Vec<KeyGroupID> = self.coder.decode_fmt2_from_bytes(&variant_entry_bytes).unwrap();
                    for other_key_group_id in decoded_vec {
                        if other_key_group_id != key_group {
                            new_vec.push(other_key_group_id);
                        }
                    }

                    self.db.put_cf(variants_cf_handle, variant, self.coder.encode_fmt2_list_to_buf(&new_vec).unwrap())?;
                } else {
                    //Otherwise, remove the variant entry entirely
                    self.db.delete_cf(variants_cf_handle, variant)?;
                }
            }
        }

        Ok(())
    }

    /// Adds the KeyGroupID to each of the supplied variants
    pub fn put_variant_references(&mut self, key_group : KeyGroupID, variants : HashSet<Vec<u8>>) -> Result<(), String> {

        // Creates a Vec<KeyGroupID> with one entry, serialized out as a string of bytes
        let new_variant_vec = |key_group : KeyGroupID| -> Vec<u8> {
            self.coder.encode_fmt2_list_to_buf(&vec![key_group]).unwrap()
        };

        //Add the key_group to each variant
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants {
            //TODO: Benchmark using merge_cf() against using a combination of get_pinned_cf() and put_cf()
            let val_bytes = new_variant_vec(key_group);
            self.db.merge_cf(variants_cf_handle, variant, val_bytes)?;
        }

        Ok(())
    }

}

impl<C: Coder + Send + Sync> Drop for DBConnection<C> {
    fn drop(&mut self) {
        //Close down Rocks
        self.db.flush().unwrap();
        let _ = DB::destroy(&rocksdb::Options::default(), self.path.as_str());
    }
}

// The function to add a new entry for a variant in the database, formulated as a RocksDB callback
fn variant_append_merge<C: Coder>(coder: &C, _key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {

    // Note: I've seen this function be called at odd times by RocksDB, such as when a DB is
    // opened.  I haven't been able to get a straight answer on why RocksDB calls this function
    // unnecessarily, but it doesn't seem to be hurting performance much.

    //TODO: Status prints in this function to understand the behavior of RocksDB.
    // Remove them when this is understood.
    // println!("Append-Called {:?}", std::str::from_utf8(_key).unwrap());

    let operands_iter = operands.into_iter();

    //Deserialize the existing database entry into a vec of KeyGroupIDs
    //NOTE: we're actually using a HashSet because we don't want any duplicates
    let mut variant_vec = if let Some(existing_bytes) = existing_val {
        let new_vec : HashSet<KeyGroupID> = coder.decode_fmt2_from_bytes(existing_bytes).unwrap();
        new_vec
    } else {
        //TODO: Remove status println!()
        // println!("MERGE WITH NONE!!");
        HashSet::with_capacity(operands_iter.size_hint().0)
    };

    //Add the new KeyGroupID(s)
    for op in operands_iter {
        //Deserialize the vec on the operand, and merge its entries into the existing vec
        let operand_vec : HashSet<KeyGroupID> = coder.decode_fmt2_from_bytes(op).unwrap();
        variant_vec.extend(operand_vec);
    }

    //TODO: Remove status println!()
    // println!("AppendResults {:?}", variant_vec);

    //Serialize the vec back out again
    coder.encode_fmt2_to_buf(&variant_vec).ok()
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


