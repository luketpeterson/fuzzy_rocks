
/// A persistent key-value store backed by RocksDB with fuzzy lookup using an arbitrary distance function that is accelerated by the SymSpell algorithm.
///

// Copy-mode and reference-mode??  Discuss it, but only implement copy-mode
//

//GOATGOAT
// Main object will be called "Table".  Two parameters.  1.) MAX_SEARCH_DISTANCE 2.) KEY_IS_UNICODE?

//Optional score structure so deletes, inserts, transposes, and substitutions can be weighted differently

// lookup function will:
//1. decompose search key into all delete-based permutations
//2. Iterate over all permutations and perform lookup into the RocksDB
//3. If there is a value, iterate over all of the original keys represented...
//      I guess that means I need to have a separate namespace or table for original keys...
//4. See if the original key qualifies under the threshold and distance whatever criteria, and filter it out if it doesn't
//



use core::marker::PhantomData;

use serde::{Serialize, Deserialize};
use bincode::Options;

use std::collections::HashSet;
use std::convert::TryInto;

use rocksdb::{DB, DBWithThreadMode, ColumnFamily, ColumnFamilyDescriptor, MergeOperands};

///
/// 
/// -`MAX_SEARCH_DISTANCE` is the number of deletes to store in the database for variants created
/// by the SymSpell optimization.  If `MAX_SEARCH_DISTANCE` is too small, the variant will not be found
/// and therefore the `distance_function` will not have an opportunity to evaluate the match.  However,
/// if `MAX_SEARCH_DISTANCE` is too large, it will hurt performance by evaluating too many candidates.
/// 
/// Empirically, values between 2 and 3 are good in most situations I have found.
/// 
/// -`MEANINGFUL_KEY_LEN` is an optimization where only a subset of the key is used for creating
/// variants.  So, if `MEANINGFUL_KEY_LEN = 10` then only the first 10 characters of the key will be used
/// to generate and search for variants.
/// 
/// This optimization is predicated on the idea that long key strings will not be very similar to each
/// other.  For example the key *incomprehensibilities* will cause variants to be generated for
///  *incomprehe*, meaning that a search for *incomprehension* would find *incomprehensibilities*
///  and evauate it with the `distance_function` even though it is further than `MAX_SEARCH_DISTANCE`.
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
pub struct Table<T : Serialize + serde::de::DeserializeOwned, const MAX_SEARCH_DISTANCE : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool> {
    record_count : usize,
    db : DBWithThreadMode<rocksdb::SingleThreaded>,
    path : String,
    phantom: PhantomData<T>,
}

/// The largest key length considered by the algorithm.  Any additional bytes of the key will be ignored
pub const MAX_KEY_LENGTH : usize = 64;

//NOTE: We have two flavors of the Record struct so we don't need to make an extra copy of the data when
//serializing, but I'm not sure how to avoid the copy when deserializing
#[derive(Serialize)]
struct RecordSer<'a, T : Serialize, const UNICODE_KEYS : bool> {
    key : &'a [u8],
    value : &'a T
}

#[derive(Deserialize)]
struct RecordDeser<T : serde::de::DeserializeOwned, const UNICODE_KEYS : bool> {
    key : Box<[u8]>, //NOTE: we could avoid this allocation with a maximum_key_length, but currently we don't have one
    #[serde(bound(deserialize = "T: serde::de::DeserializeOwned"))]
    value : T
}

//GOATGOATGOAT, Make the RecordID be a special type, and add a function to get them directly, along with a function to scrub them out of the table.
#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, derive_more::Display, Serialize, Deserialize)]
pub struct RecordID(usize);

const RECORDS_CF_NAME : &str = "records";
const VARIANTS_CF_NAME : &str = "variants";

impl <T : 'static + Serialize + serde::de::DeserializeOwned, const MAX_SEARCH_DISTANCE : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool>Table<T, MAX_SEARCH_DISTANCE, MEANINGFUL_KEY_LEN, UNICODE_KEYS> {

    /// Creates a new Table, backed by the path provided
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn new(path : &str) -> Result<Self, String> {

        let records_cf = ColumnFamilyDescriptor::new(RECORDS_CF_NAME, rocksdb::Options::default());

        let mut variants_opts = rocksdb::Options::default();
        variants_opts.create_if_missing(true);
        variants_opts.set_merge_operator_associative("append to RecordID vec", Self::variant_append_merge);
        let variants_cf = ColumnFamilyDescriptor::new(VARIANTS_CF_NAME, variants_opts);

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors(&db_opts, path, vec![records_cf, variants_cf])?;

        let records_cf_handle = db.cf_handle(RECORDS_CF_NAME).unwrap();

        //GOATGOATGOAT, Dead test code
        // db.put_cf(records_cf_handle, usize::to_le_bytes(0), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(1), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(2), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(3), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(4), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(5), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(6), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(7), b"bobby").unwrap();
        // db.put_cf(records_cf_handle, usize::to_le_bytes(8), b"bobby").unwrap();

        let record_count = probe_for_max_sequential_key(&db, records_cf_handle, 255)?;

        //GOATGOATGOAT
        // println!("max = {}", record_count);

        Ok(Self {
            record_count : record_count,
            db : db,
            path : path.to_string(),
            phantom : PhantomData
        })
    }

    /// Inserts a record into the Table, called by insert(), which is implemented differently depending
    /// on the UNICODE_KEYS constant
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    fn insert_internal(&mut self, raw_key : &[u8], value : &T) -> Result<RecordID, String> {

//GOATGOATGOAT, THis below is wrong.  We want to support duplicate keys.  Two distinct places might have exactly the same name.
// This means we should create every time when we get here.

        //See if we already have the key.
        //If we do, we only want to update the records table with the new value.
        //if we don't then we need to create a new record and also create all of the variants
        let (new_record_id, need_to_create_variants) = match self.lookup_exact_internal(raw_key) {
            Ok(existing_record_id) => {
println!("use_existing {:?} - {}", existing_record_id, std::str::from_utf8(&raw_key).unwrap());
                (existing_record_id, false)
            },
            Err(_err_str) => {
                //We'll be creating a new record, so get the next unique record_id
                let new_record_id = RecordID(self.record_count);
                self.record_count += 1;
println!("make_new!! {:?} - {}", new_record_id, std::str::from_utf8(&raw_key).unwrap());
                (new_record_id, true)
            }
        };

        //Create the records structure, serialize it, and put in into the records table.
        //If we are updating an old record, we will overwrite it but the key and record_id will stay the same
        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        let record = RecordSer::<T, UNICODE_KEYS>{
            key : raw_key,
            value : value
        };
        let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
        let record_bytes = record_coder.serialize(&record).unwrap();
        self.db.put_cf(records_cf_handle, usize::to_le_bytes(new_record_id.0), record_bytes)?;

        //Now add the variants to the table if this is the first time we're encountering this key
        if need_to_create_variants {
            let variants = Self::variants(&raw_key);
            let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();

            //GOATGOAT DEBUG
            if new_record_id.0 % 500 == 0 {
                println!("bla {} turns into {}", std::str::from_utf8(&raw_key).unwrap(), variants.len());
                println!("{}", new_record_id.0);
            }
            // println!("bla {} turns into {}", std::str::from_utf8(&raw_key).unwrap(), variants.len());

            //Add the new_record_id to each variant
            for variant in variants {
                //TODO: Benchmark using merge_cf() against using a combination of get_pinned_cf() and put_cf()
                let val_bytes = Self::new_variant_vec(new_record_id);
                //GOATGOATGOAT DEBUG
                // println!("meerkat {:?}",new_record_id);
                self.db.merge_cf(variants_cf_handle, variant, val_bytes)?;
            }
        }

        Ok(new_record_id)
    }

//GOATGOATGOAT, This function should return an iterator because a key can have multiple valid entries
    /// Checks the table for a record whose key precisely matches the key supplied
    fn lookup_exact_internal(&self, raw_key : &[u8]) -> Result<RecordID, String> {

        let meaningful_key = Self::meaningful_key_substring(raw_key);

        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        let variants_cf_handle = self.db.cf_handle(VARIANTS_CF_NAME).unwrap();
        if let Some(variant_vec_bytes) = self.db.get_pinned_cf(variants_cf_handle, meaningful_key)? {

            // Loop over all of the RecordIDs in the vec we got from the DB and see if any of their keys match the key we are looking up
            for record_id_bytes in bincode_vec_iter::<RecordID>(&variant_vec_bytes) {

                if let Some(record_bytes) = self.db.get_pinned_cf(records_cf_handle, record_id_bytes)? {

                    //TODO: Make a way to read the key without doing a full deserialize and copying the data
                    //Get the key from the record we just looked up in the DB
                    let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
                    let record : RecordDeser::<T, UNICODE_KEYS> = record_coder.deserialize(&record_bytes).unwrap();
        
                    //If the full key in the DB matches the key we're checking return the RecordID
                    if *record.key == *raw_key {
                        return Ok(RecordID(usize::from_le_bytes(record_id_bytes.try_into().unwrap())));
                    }
                } else {
                    return Err("Internal Error: bad record_id in variant".to_string());
                }
            }

            Err("No Record Found Matching Key".to_string())
        } else {
            Err("No Record Found Matching Key".to_string())
        }
    }

    /// Returns the value at a specified record.  This function will be faster than doing a fuzzy lookup
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn get_with_record_id(&self, record_id : RecordID) -> Result<T, String> {

        //Get the Record structure by deserializing the bytes from the db
        let records_cf_handle = self.db.cf_handle(RECORDS_CF_NAME).unwrap();
        if let Some(record_bytes) = self.db.get_pinned_cf(records_cf_handle, record_id.0.to_le_bytes())? {
            let record_coder = bincode::DefaultOptions::new().with_varint_encoding().with_little_endian();
            let record : RecordDeser::<T, UNICODE_KEYS> = record_coder.deserialize(&record_bytes).unwrap();

            Ok(record.value)
        } else {
            Err("Invalid record_id".to_string())
        }
    }

    // Creates a Vec<RecordID> with one entry, serialized out as a string of bytes
    fn new_variant_vec(record_id : RecordID) -> Vec<u8> {

        //Create a new vec and Serialize it out
        let mut new_vec = Vec::with_capacity(1);
        new_vec.push(record_id);
        let vec_coder = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian();
        vec_coder.serialize(&new_vec).unwrap()
    }

    // The function to add a new entry for a variant in the database, formulated as a RocksDB callback
    fn variant_append_merge(_key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Option<Vec<u8>> {

// println!("Append-Called {:?}", std::str::from_utf8(key).unwrap());
        let vec_coder = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian();

        //Deserialize the existing database entry into a vec of RecordIDs
        let mut variant_vec = if let Some(existing_bytes) = existing_val {
            let new_vec : Vec<RecordID> = vec_coder.deserialize(&existing_bytes).unwrap();
            new_vec
        } else {

// println!("MERGE WITH NONE!!");
            Vec::with_capacity(1)
        };

        //Add the new RecordID(s)
        for op in operands {
            //Deserialize the vec on the operand, and merge its entries into the existing vec
            let operand_vec : Vec<RecordID> = vec_coder.deserialize(op).unwrap();
            variant_vec.extend(operand_vec);
        }

// println!("Appending {:?}", variant_vec);

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
    
                    if edit_distance < MAX_SEARCH_DISTANCE {
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


}

impl <T : 'static + Serialize + serde::de::DeserializeOwned, const MAX_SEARCH_DISTANCE : usize, const MEANINGFUL_KEY_LEN : usize>Table<T, MAX_SEARCH_DISTANCE, MEANINGFUL_KEY_LEN, true> {

    /// Inserts a new key-value pair into the table
    /// 
    /// NOTE: [rocksdb::Error] is a wrapper around a string, so if an error occurs it will be the
    /// unwrapped RocksDB error.
    pub fn insert(&mut self, key : &str, value : &T) -> Result<RecordID, String> {
        self.insert_internal(key.as_bytes(), value)
    }


}

impl <T : Serialize + serde::de::DeserializeOwned, const MAX_SEARCH_DISTANCE : usize, const MEANINGFUL_KEY_LEN : usize, const UNICODE_KEYS : bool>Drop for Table<T, MAX_SEARCH_DISTANCE, MEANINGFUL_KEY_LEN, UNICODE_KEYS> {
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
    } else {
        if starting_hint < 1 {
            1
        } else {
            starting_hint * starting_hint
        }
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
                guess_max = guess_max * 2;
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
/// TODO: Try with `with_varint_encoding` rather than `with_fixint_encoding`, and measure performance.
/// It's highly likely that the data compression completely makes up for the extra work and copy
/// deserializing the structure, and it's faster not to mess with the 
struct BinCodeVecIterator<'a, T : Sized + Copy> {
    remaining_buf : &'a [u8],
    phantom: PhantomData<&'a T>,
}

/// Returns the length of a Vec<T> that has been encoded with bincode
fn bincode_vec_len(buf : &[u8]) -> usize {

    let (len_chars, _remainder) = buf.split_at(8);
    usize::from_le_bytes(len_chars.try_into().unwrap())
}

/// Returns a [BinCodeVecIterator] to iterate over a Vec<T> that has been encoded with bincode,
/// without requiring an actual [Vec] to be recreated in memory
fn bincode_vec_iter<'a, T : Sized + Copy>(buf : &'a [u8]) -> BinCodeVecIterator<'a, T> {

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


//--///////////////////////////////////////////////////////////////////////////////////////////
// DEAD CODE, Failed Experiment in a complete Vec-like type over a &[u8] buffer
//--///////////////////////////////////////////////////////////////////////////////////////////


//I decided against this approach.  even though it would theoretically be more efficient, especially on
// the read-side because the memory of the bytes buffer could be treated as a vec in-place without any
// copies.  I have it all working, but ultimately shelved the approach in favor of serde and bincode,
// because of the dependency on the nightly toolchain and the complications around endian-ness and other
// memory-layout assumptions permeating the format in the database.

// I also realized I could create a safer and simpler iterator over the entries in bincode format.
// so when just reading is required, you can use that iterator, and when writing is also required, there
// is basically no performance to be gained by avoiding the bincode deserialization and reserialization.

//BlobVec test
// let forty_two : i32 = 42;
// let blob = BlobVec::new([forty_two]);
// let blob = blob.push_clone(22);
// let blob_buffer : &[u8] = blob.as_ref();
// println!("size = {}, {:?}", blob_buffer.len(), blob_buffer);

// /// A Vec-like type, where the entire contents as well as the header data are in a single blob in memory
// /// 
// /// **WARNING** No byte-order conversion is performed when types are serialized and deserialized into the
// /// database.  Therefore you may not build a database on a little-endian architecture and use it on a
// /// big-endian architecture or vice-versa.
// #[warn(dead_code)]
// struct BlobVec<T : Sized + Copy + Debug, const LEN : usize> {
//     len : usize, //len is here so it gets serialized out
//     contents : [T; LEN]
// }

// impl <T: Sized + Copy + Debug, const LEN : usize>BlobVec<T, LEN> {

//     pub fn new(contents : [T; LEN]) -> Self {
//         Self {
//             len : LEN,
//             contents : contents
//         }
//     }

//     pub fn len(&self) -> usize {
//         LEN
//     }

//     pub fn push_clone(&self, item : T) -> BlobVec<T, {LEN+1}> {
//         let mut new_contents = self.contents.to_vec();
//         new_contents.push(item);
//         BlobVec::new(new_contents.try_into().unwrap())
//     }
// }

// impl <T: Sized + Copy + Debug, const LEN : usize>AsRef<[u8]> for BlobVec<T, LEN>
// {
//     fn as_ref(&self) -> &[u8] {
//         unsafe {
//             ::std::slice::from_raw_parts(
//                 (self as *const BlobVec<T, LEN>) as *const u8,
//                 ::std::mem::size_of::<BlobVec<T, LEN>>(),
//             )
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use crate::{*};
    use sqlite::State;


    #[test]
    fn fuzzy_rocks_test() {

        //Open the SQLite connection and set up the query
        let connection = sqlite::open("data_store.sqlite").unwrap();
        let mut query = connection
            .prepare("SELECT * FROM geonames WHERE population > ?")
            .unwrap();
        query.bind(1, 5000).unwrap();

        //GOATGOATGOAT WTF!!!  Why does this do this???
        // for i in 0..10 {
        //     //Create the FuzzyRocks Table
        //     let mut table = Table::<i64, 3, 12, true>::new("goat.rocks").unwrap();
        //     table.db.flush();
        //     drop(table);
        // }

        //Create the FuzzyRocks Table
        let mut table = Table::<i64, 3, 12, true>::new("goat.rocks").unwrap();


//BinCode test
let forty_two : i32 = 42;
let mut my_vec = vec![forty_two, 22];
my_vec.push(987);


let bincode_serde = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian();

let bytes_buf = bincode_serde.serialize(&my_vec).unwrap();
println!("size = {}, {:?}", bytes_buf.len(), bytes_buf);

println!("len {}", bincode_vec_len(&bytes_buf));
for item in bincode_vec_iter::<i32>(&bytes_buf[..]) {
    println!("size = {}, {:?}", item.len(), item);
}

    // return;

        //Iterate over all of the rows returned by the sqlite query, and load them into Rocks
        while let State::Row = query.next().unwrap() {

            let geonameid = query.read::<i64>(0).unwrap();
            let name = query.read::<String>(1).unwrap();
            let population = query.read::<i64>(14).unwrap();

            let record_id = table.insert(&name, &geonameid).unwrap();
        }
        
    }
}