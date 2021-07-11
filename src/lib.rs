
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
//! GOATGOATGOAT, Add usage example
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
//! One reason to use a custom distance function is to account for expected variations. For example:
//! a distance function that considers likely [OCR](https://en.wikipedia.org/wiki/Optical_character_recognition)
//! errors might consider `ol` to be very close to `d`, `0` to be extremely close to `O`, and `A` to be
//! somewhat near to `^`, while `#` would be much further from `,`
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
//! considerations.  This create has been tested with 250,000 unique keys and about 10 million variants.
//! 
//! If your use-case can cope with a higher startup latency and you are ok with all of your keys and
//! variants being loaded into memory, then query performance will certainly be better using a solution
//! built on Rust's native collections, such as this [symspell](https://crates.io/crates/symspell)
//! crate on [crates.io](http://crates.io).
//! 
//! **NOTE**: The included `geonames_megacities.txt` file is a stub for the `geonames_test`, designed to stress-test
//! this crate.  The abriged file is included so the test will pass regardless, and to avoid bloating the
//! download.  The content of `geonames_megacities.txt` was derived from data on [geonames.org](http://geonames.org),
//! and licensed under a [Creative Commons Attribution 4.0 License](https://creativecommons.org/licenses/by/4.0/legalcode)
//! 

use core::marker::PhantomData;
use core::cmp::min;

use num_traits::Zero;

use serde::{Serialize, Deserialize};
use bincode::Options;

use std::collections::HashSet;
use std::convert::TryInto;

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
pub struct Table<T : Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool> {
    record_count : usize,
    db : DBWithThreadMode<rocksdb::SingleThreaded>,
    path : String,
    phantom: PhantomData<T>,
}

//NOTE: We have two flavors of the Record struct so we don't need to make an extra copy of the data when
//serializing, but I'm not sure how to avoid the copy when deserializing
#[derive(Serialize)]
struct RecordSer<'a, T : Serialize, const UNICODE_KEYS : bool> {
    key : &'a [u8],
    value : Option<&'a T>
}

#[derive(Deserialize)]
struct RecordDeser<T : serde::de::DeserializeOwned, const UNICODE_KEYS : bool> {
    key : Box<[u8]>, //NOTE: we could avoid this secondary allocation with a maximum_key_length, but currently we don't have one
    #[serde(bound(deserialize = "T: serde::de::DeserializeOwned"))]
    value : Option<T>
}

/// A unique identifier for a record within a [Table]
#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, derive_more::Display, Serialize, Deserialize)]
pub struct RecordID(usize);
impl RecordID {
    pub const NULL : RecordID = RecordID(usize::MAX);
}

const RECORDS_CF_NAME : &str = "records";
const VARIANTS_CF_NAME : &str = "variants";

impl <T : 'static + Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool>Table<T, MAX_DELETES, MEANINGFUL_KEY_LEN, UNICODE_KEYS> {

    /// Creates a new Table, backed by the database at the path provided
    /// 
    /// WARNING:  No sanity checks are performed to ensure the database being opened matches the parameters
    /// of the table being created.  Therefore you may see bugs if you are opening a table that was created
    /// using a different set of parameters.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn new(path : &str) -> Result<Self, String> {

        //Configure the "records" column family
        let records_cf = ColumnFamilyDescriptor::new(RECORDS_CF_NAME, rocksdb::Options::default());

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
        let db = DB::open_cf_descriptors(&db_opts, path, vec![records_cf, variants_cf])?;

        //Find the maximum RecordID, by probing the keys in the "records" column family
        let records_cf_handle = db.cf_handle(RECORDS_CF_NAME).unwrap();
        let record_count = probe_for_max_sequential_key(&db, records_cf_handle, 255)?;

        Ok(Self {
            record_count,
            db,
            path : path.to_string(),
            phantom : PhantomData
        })
    }

    /// Resets a Table, dropping every record in the table and restoring it to an empty state.
    /// 
    /// (Dropping in a database sense, not a Rust sense)
    pub fn reset(&mut self) -> Result<(), String> {
        
        //Drop both the "records" and the "variants" column families
        self.db.drop_cf(RECORDS_CF_NAME)?;
        self.db.drop_cf(VARIANTS_CF_NAME)?;

        //Recreate the "records" column family
        self.db.create_cf(RECORDS_CF_NAME, &rocksdb::Options::default())?;

        //Recreate the "variants" column family
        let mut variants_opts = rocksdb::Options::default();
        variants_opts.create_if_missing(true);
        variants_opts.set_merge_operator_associative("append to RecordID vec", Self::variant_append_merge);
        self.db.create_cf(VARIANTS_CF_NAME, &variants_opts)?;

        //Reset the record_count, so newly inserted entries begin at 0 again
        self.record_count = 0;
        Ok(())
    }

    /// Returns `true` if a [RecordID] **can** be used to refer to a record in a Table, otherwise returns
    /// `false`.  A deleted record will still have a valid [RecordID].
    /// 
    /// Use this function instead of comparing a [RecordID] to [RecordID::NULL].
    pub fn record_id_is_valid(&self, record_id : RecordID) -> bool {
        record_id.0 < self.record_count
    }

    /// Deletes a record from the Table.
    /// 
    /// A deleted record cannot be accessed or otherwise found, but the RecordID may be reassigned
    /// using [Table::replace].
    pub fn delete(&mut self, record_id : RecordID) -> Result<T, String> {

        //Get the key for the record we're removing, so we can compute all the variants
        let (raw_key, value) = self.get_with_record_id_internal(record_id)?;
        let variants = Self::variants(&raw_key);

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

        //Now replace the record with an empty sentinel in the records table
        //NOTE: We replace the record rather than delete it because we assume there are no gaps in the
        // RecordIDs, when assigning new a RecordID
        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        let empty_record = RecordSer::<T, UNICODE_KEYS>{
            key : b"",
            value : None
        };
        let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
        let record_bytes = record_coder.serialize(&empty_record).unwrap();
        self.db.put_cf(records_cf_handle, usize::to_le_bytes(record_id.0), record_bytes)?;

        Ok(value)
    }

    fn replace_internal(&mut self, record_id : RecordID, raw_key : &[u8], value : &T) -> Result<(), String> {
        if self.record_id_is_valid(record_id) {

            let existing_record_result = self.get_with_record_id_internal(record_id);

            //NOTE: There are 3 paths through this function once we validate we have a valid record_id
            //1. The existing record has been deleted, in which case we just insert a new record
            //2. The existing record exists and the keys match, in which case we just replace the value
            //3. The existing record exists and the keys don't match, in which case we must delete the existing record and insert a new record

            if let Ok((existing_key, _existing_val)) = existing_record_result {
                if *existing_key == *raw_key {
                    //Case 2
                    self.put_record_internal(record_id, raw_key, value)?;
                } else {
                    //Case 3
                    self.delete(record_id)?;
                    self.insert_internal(raw_key, value, Some(record_id))?;
                }
            } else {
                //Case 1
                self.insert_internal(raw_key, value, Some(record_id))?;
            }
            
            Ok(())
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Creates the records structure in the records table
    /// If we are updating an old record, we will overwrite it.
    /// 
    /// NOTE: This function will NOT update any variants used to locate the key
    fn put_record_internal(&mut self, record_id : RecordID, raw_key : &[u8], value : &T) -> Result<(), String>{
        
        //Create the records struct, serialize it, and put in into the records table.
        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        let record = RecordSer::<T, UNICODE_KEYS>{
            key : raw_key,
            value : Some(value)
        };
        let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
        let record_bytes = record_coder.serialize(&record).unwrap();
        self.db.put_cf(records_cf_handle, usize::to_le_bytes(record_id.0), record_bytes)?;

        Ok(())
    }

    /// Inserts a record into the Table, called by insert(), which is implemented differently depending
    /// on the UNICODE_KEYS constant
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    fn insert_internal(&mut self, raw_key : &[u8], value : &T, supplied_id : Option<RecordID>) -> Result<RecordID, String> {

        let new_record_id = match supplied_id {
            None => {
                //We'll be creating a new record, so get the next unique record_id
                let new_record_id = RecordID(self.record_count);
                self.record_count += 1;
                new_record_id
            },
            Some(record_id) => record_id
        };

        //Put the record into the records table
        self.put_record_internal(new_record_id, raw_key, value)?;

        //Now compute all the variants so we'll be able to add an entry to each one
        let variants = Self::variants(raw_key);

        //Add the new_record_id to each variant
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        for variant in variants {
            //TODO: Benchmark using merge_cf() against using a combination of get_pinned_cf() and put_cf()
            let val_bytes = Self::new_variant_vec(new_record_id);
            self.db.merge_cf(variants_cf_handle, variant, val_bytes)?;
        }

        Ok(new_record_id)
    }

    /// Returns an iterator over all possible candidates for a given fuzzy search key, based on
    /// MAX_DELETES, without any distance function applied
    fn fuzzy_candidates_iter<'a>(&'a self, raw_key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {

        //Create all of the potential variants based off of the "meaningful" part of the key
        let variants = Self::variants(raw_key);

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

                //Check the record's key with the distance function
                let (record_key, _val) = self.get_with_record_id_internal(record_id).unwrap();
                let distance = distance_function(&record_key, raw_key);

                match threshold {
                    Some(threshold) => {
                        if distance <= threshold{
                            Some((record_id, distance))
                        } else {
                            None
                        }        
                    }
                    None => Some((record_id, distance))
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
    fn lookup_exact_internal<'a>(&'a self, raw_key : &'a [u8]) -> Result<impl Iterator<Item=RecordID> + 'a, String> {

        let meaningful_key = Self::meaningful_key_substring(raw_key);

        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        if let Some(variant_vec_bytes) = self.db.get_pinned_cf(variants_cf_handle, meaningful_key)? {

            let record_id_iter = bincode_vec_iter::<RecordID>(&variant_vec_bytes)
                .filter_map(|record_id_bytes| {
                    
                    // Return only the RecordIDs for records if their keys match the key we are looking up
                    if let Some(record_bytes) = self.db.get_pinned_cf(records_cf_handle, record_id_bytes).unwrap() {

                        //Get the key from the record we just looked up in the DB
                        //NOTE: Fully decoding the record is a lot of unnecessary work.  It's probably faster just to
                        // peek inside it.
                        // let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
                        // let record : RecordDeser::<T, UNICODE_KEYS> = record_coder.deserialize(&record_bytes).unwrap();
                        let record_key = bincode_string_varint(&record_bytes as &[u8]);
    
                        //If the full key in the DB matches the key we're checking return the RecordID
                        if *record_key == *raw_key {
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
    pub fn get_value_with_record_id(&self, record_id : RecordID) -> Result<T, String> {

        //Get the Record structure by deserializing the bytes from the db
        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        if let Some(record_bytes) = self.db.get_pinned_cf(records_cf_handle, record_id.0.to_le_bytes())? {
            let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
            let record : RecordDeser::<T, UNICODE_KEYS> = record_coder.deserialize(&record_bytes).unwrap();

            match record.value {
                Some(value) => Ok(value),
                None => Err("Invalid record_id".to_string())
            }
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    /// Returns the key and value at a specified record.  This function will be faster than doing a
    /// fuzzy lookup
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    fn get_with_record_id_internal(&self, record_id : RecordID) -> Result<(Box<[u8]>, T), String> {

        //Get the Record structure by deserializing the bytes from the db
        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        if let Some(record_bytes) = self.db.get_pinned_cf(records_cf_handle, record_id.0.to_le_bytes())? {
            let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
            let record : RecordDeser::<T, UNICODE_KEYS> = record_coder.deserialize(&record_bytes).unwrap();

            match record.value {
                Some(value) => Ok((record.key, value)),
                None => Err("Invalid record_id".to_string())
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
        let mut variant_vec = if let Some(existing_bytes) = existing_val {
            let new_vec : Vec<RecordID> = vec_coder.deserialize(existing_bytes).unwrap();
            new_vec
        } else {

            //TODO: Remove status println!()
            // println!("MERGE WITH NONE!!");
            Vec::with_capacity(operands.size_hint().0)
        };

        //Add the new RecordID(s)
        for op in operands {
            //Deserialize the vec on the operand, and merge its entries into the existing vec
            let operand_vec : Vec<RecordID> = vec_coder.deserialize(op).unwrap();
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
    fn variants(key: &[u8]) -> HashSet<Vec<u8>> {

        let mut variants_set : HashSet<Vec<u8>> = HashSet::new();
        
        let meaningful_key = Self::meaningful_key_substring(key);

        variants_set.insert(meaningful_key.clone());
        Self::variants_recursive(&meaningful_key[..], 0, &mut variants_set);
    
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
            let (prefix, _remainder) = s.split_at(MEANINGFUL_KEY_LEN);
            prefix.to_owned()
        }
    }

    /// An implementation of the basic Levenstein distance function, which may be passed to [Table::lookup_fuzzy]
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

impl <T : 'static + Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize>Table<T, MAX_DELETES, MEANINGFUL_KEY_LEN, true> {

    /// Inserts a new key-value pair into the table and returns the RecordID of the new record
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
    pub fn insert(&mut self, key : &str, value : &T) -> Result<RecordID, String> {
        self.insert_internal(key.as_bytes(), value, None)
    }

    /// Replaces a record in the Table with the supplied key-value pair
    /// 
    /// If the supplied `record_id` references an existing record, the existing record contents will
    /// be deleted.  If the supplied `record_id` references a record that has been deleted, this
    /// function will succeed, but is the `record_id` is invalid then this function will return an
    /// error.
    /// 
    /// If the key exactly matches the existing record's key, this function will be more efficient
    /// as it will not need to update the stored variants.
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn replace(&mut self, record_id : RecordID, key : &str, value : &T) -> Result<(), String> {
        self.replace_internal(record_id, key.as_bytes(), value)
    }

    /// Returns the key and value associated with the specified record
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_with_record_id(&self, record_id : RecordID) -> Result<(String, T), String> {
        self.get_with_record_id_internal(record_id).map(|(key, val)| (String::from_utf8(key.to_vec()).unwrap(), val))
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

// GOATGOAT, Provide a unicode=false implementation


impl <T : Serialize + serde::de::DeserializeOwned, const MAX_DELETES : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool>Drop for Table<T, MAX_DELETES, MEANINGFUL_KEY_LEN, UNICODE_KEYS> {
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

/// Returns a slice representing the characters of a String that has been encoded with bincode, using
/// [VarintEncoding](bincode::config::VarintEncoding) and [LittleEndian](bincode::config::LittleEndian) byte order.
fn bincode_string_varint(buf : &[u8]) -> &[u8] {

    //Interpret the length
    let mut skip_bytes = 0;
    let string_len = bincode_u64_le_varint(buf, &mut skip_bytes);

    //Split the slice to grab the string
    let (_len_chars, remainder) = buf.split_at(skip_bytes);
    let (string_slice, _remainder) = remainder.split_at(string_len as usize);
    string_slice
}

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
            record_id = table.insert(&geoname.name.to_lowercase(), &geoname.geonameid).unwrap();
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
        let london_results : Vec<(String, i32)> = table.lookup_exact("london").unwrap().map(|record_id| table.get_with_record_id(record_id).unwrap()).collect();
        assert!(london_results.contains(&("london".to_string(), 2643743)));

        //Close RocksDB connection by dropping the table object
        drop(table);
        drop(london_results);

        //Reopen the table and confirm that "London" is still there
        let table = Table::<i32, 2, 12, true>::new("geonames.rocks").unwrap();
        let london_results : Vec<(String, i32)> = table.lookup_exact("london").unwrap().map(|record_id| table.get_with_record_id(record_id).unwrap()).collect();
        assert!(london_results.contains(&("london".to_string(), 2643743)));
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
        let results : Vec<(String, String)> = table.lookup_exact("Friday").unwrap().map(|record_id| table.get_with_record_id(record_id).unwrap()).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Friday");
        assert_eq!(results[0].1, "Kinyoubi");

        //Test lookup_exact, with a query that should provide no results
        let results : Vec<(String, String)> = table.lookup_exact("friday").unwrap().map(|record_id| table.get_with_record_id(record_id).unwrap()).collect();
        assert_eq!(results.len(), 0);

        //Test lookup_best, using the supplied edit_distance function
        let result = table.lookup_best("Bonday", Table::<String, 2, 8, true>::edit_distance).unwrap();
        assert_eq!(result, mon);

        //Test lookup_best, when there is no acceptable match
        let result = table.lookup_best("Rahu", Table::<String, 2, 8, true>::edit_distance);
        assert!(result.is_err());

        //Test lookup_fuzzy with a perfect match, using the supplied edit_distance function
        //In this case, we should only get one match within edit-distance 2
        let results : Vec<(String, String, u64)> = table.lookup_fuzzy("Saturday", Table::<String, 2, 8, true>::edit_distance, 2)
            .unwrap().map(|(record_id, distance)| {
                let (key, val) = table.get_with_record_id(record_id).unwrap();
                (key, val, distance)
            }).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Saturday");
        assert_eq!(results[0].1, "Douyoubi");
        assert_eq!(results[0].2, 0);

        //Test lookup_fuzzy with a perfect match, but where we'll hit another imperfect match as well
        let results : Vec<(String, String, u64)> = table.lookup_fuzzy("Tuesday", Table::<String, 2, 8, true>::edit_distance, 2)
            .unwrap().map(|(record_id, distance)| {
                let (key, val) = table.get_with_record_id(record_id).unwrap();
                (key, val, distance)
            }).collect();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&("Tuesday".to_string(), "Kayoubi".to_string(), 0)));
        assert!(results.contains(&("Thursday".to_string(), "Mokuyoubi".to_string(), 2)));

        //Test lookup_fuzzy where we should get no match
        let results : Vec<(RecordID, u64)> = table.lookup_fuzzy("Rahu", Table::<String, 2, 8, true>::edit_distance, 2).unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test lookup_fuzzy_raw, to get all of the SymSpell Delete variants
        //We're testing the fact that characters beyond MEANINGFUL_KEY_LEN aren't used for the comparison
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Sunday. That's my fun day.").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sun);

        //Test deleting a record, and ensure we can't access it or any trace of its variants
        table.delete(tue).unwrap();
        assert!(table.get_with_record_id(tue).is_err());

        //Since "Tuesday" had one variant overlap with "Thursday", i.e. "Tusday", make sure we now find
        // "Thursday" when we attempt to lookup "Tuesday"
        let result = table.lookup_best("Tuesday", Table::<String, 2, 8, true>::edit_distance).unwrap();
        assert_eq!(result, thu);

        //Delete "Saturday" and make sure we see no matches when we try to search for it
        table.delete(sat).unwrap();
        assert!(table.get_with_record_id(sat).is_err());
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Saturday").unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test replacing a record with another one and ensure the right data is retained
        table.replace(wed, "Miercoles", &"Zhousan".to_string()).unwrap();
        let results : Vec<(String, String)> = table.lookup_exact("Miercoles").unwrap().map(|record_id| table.get_with_record_id(record_id).unwrap()).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Miercoles");
        assert_eq!(results[0].1, "Zhousan");
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Mercoledi").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], wed);

        //Test replacing a record that we deleted earlier
        table.replace(sat, "Sabado", &"Zhouliu".to_string()).unwrap();
        let results : Vec<(String, String)> = table.lookup_exact("Sabado").unwrap().map(|record_id| table.get_with_record_id(record_id).unwrap()).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Sabado");
        assert_eq!(results[0].1, "Zhouliu");
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Sabato").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sat);

        //Attempt to replace an invalid record and confirm we get a reasonable error
        assert!(table.replace(RecordID::NULL, "Nullday", &"Null".to_string()).is_err());

        //Test the fast-path of the replace method, when the keys are identical
        table.replace(fri, "Friday", &"Geumyoil".to_string()).unwrap();
        let (key, val) = table.get_with_record_id(fri).unwrap();
        assert_eq!(key, "Friday");
        assert_eq!(val, "Geumyoil");

    }
}