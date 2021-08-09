
//! # fuzzy_rocks Overview
//! 
//! A persistent datastore backed by [RocksDB](https://rocksdb.org) with fuzzy key lookup using an arbitrary
//! distance function accelerated by the [SymSpell](https://github.com/wolfgarbe/SymSpell) algorithm.
//! 
//! The reasons to use this crate over another SymSpell implementation are:
//! - You have non-character-based keys (e.g. DNA snippets, etc.)
//! - You want to use a custom distance function
//! - Startup time matters more than lookups-per-second
//! - You have millions of keys and care about memory footprint
//! 
//! ## Records
//! 
//! This crate manages records, each of which has a unique [RecordID].  Keys are used to perform fuzzy
//! lookups but keys are not guaranteed to be unique. [Insert](Table::insert)ing the same key into a [Table]
//! twice will result in two distinct records, and both records will be found by lookups of that key.
//! 
//! ## Usage Example
//! 
//! ```
//! use fuzzy_rocks::{*};
//! 
//! //Create and reset the FuzzyRocks Table
//! let mut table = Table::new("test.rocks", DEFAULT_UTF8_TABLE).unwrap();
//! table.reset().unwrap();
//!
//! //Insert some records
//! let thu = table.insert("Thursday", &"Mokuyoubi".to_string()).unwrap();
//! let wed = table.insert("Wednesday", &"Suiyoubi".to_string()).unwrap();
//! let tue = table.insert("Tuesday", &"Kayoubi".to_string()).unwrap();
//! let mon = table.insert("Monday", &"Getsuyoubi".to_string()).unwrap();
//! 
//! //Try out lookup_best, to get the closest fuzzy match
//! let result = table.lookup_best("Bonday")
//!     .unwrap().next().unwrap();
//! assert_eq!(result, mon);
//! 
//! //Try out lookup_fuzzy, to get all matches and their distances
//! let results : Vec<(RecordID, u8)> = table
//!     .lookup_fuzzy("Tuesday", 2)
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
//! words, you must be able to delete no more than [config.max_deletes] characters from each of the record's
//! key and the lookup key and arrive at identical key-variants.  If your distance function is incompatible
//! with this property then the SymSpell optimization won't work for you and you should use a different fuzzy
//! lookup technique and a different crate.
//! 
//! Distance functions may return any scalar type, so floating point distances will work.  However, the
//! [config.max_deletes] constant is an integer.  Records that can't be reached by deleting `config.max_deletes` characters
//! from both the record key and the lookup key will never be evaluated by the distance function and are
//! conceptually "too far away".  Once the distance function has been evaluated, its return value is
//! considered the authoritative distance and the delete distance is irrelevant.
//! 
//! ## Unicode and UTF-8 Support
//! 
//! GOATGOAT, More to say
//! 
//! A [Table] may allow for unicode keys or not, depending on the value of the `UTF8_KEYS` constant used
//! when the Table was created.
//! 
//! If `UTF8_KEYS` is `true`, keys may use unicode characters and multi-byte characters will still be
//! considered as single characters for the purpose of deleting characters to create key variants.
//! 
//! If `UTF8_KEYS` is `false`, keys are just strings of [u8] characters.
//! This option has better performance.
//! 
//! ## Algorithm Details
//! 
//! The authoritative description of SymSpell is the ReadMe for the [SymSpell project](https://github.com/wolfgarbe/SymSpell).
//! 
//! The fuzzy_rocks implementation has a few additional details to be aware of:
//! 
//! - fuzzy_rocks won't find keys that don't have at least one character in common, regardless of the value
//! of `config.max_deletes`.  For example the key `me` won't be found by the query string `hi`, even with a distance
//! of 2 (or any other value).  This decision was made because the variant space becomes very crowded
//! for short keys, and the extreme example of the empty-string variant was severely damaging performance with
//! short keys.
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
//! GOATGOAT, explain multi-key support
//! 
//! ### Tuning for Performance
//! 
//! GOATGOATGOAT, Write-up on perf_counters, how to enable, etc.
//! 
//! GOATGOATGOAT, Write-up on benchmarks.  How to edit, how to run.
//! 
//! A smaller `config.max_deletes` value will perform better but be able to find fewer results for a search.
//! 
//! A higher value for `config.meaningful_key_len` will result in fewer wasted evaluations of the distance function
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

//GOATGOATGOAT, write up:
// 0.) DNA snippet example.  Look at FAStA Wikipedia article
// 1.) How SymSpell works best with sparse key spaces, but a BK-Tree is better for dense key spaces
// 2.) How we have "Future Work" to add a BK tree to search within a key group
//      *consider adding a "k-nn query", in addition to or instead of the "best" query
//      *Tools to detect when a parameter is tuned badly for a data set, based on known optimal
//      ratios for certain performance counters.  Alternatively, tools to assist in performing a
//      config-parameter optimization process, to tune a table config to a data set.
//      *Save the Table config to the database (and the checksum of the distance function) to
//      detect an error when the config changes in a way that makes the database invalid

//TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
// #![feature(generic_associated_types)]

#[cfg(feature = "perf_counters")]
use core::cell::Cell;

mod unicode_string_helpers;
mod bincode_helpers;
mod database;
mod key;
pub use key::Key;
mod records;
pub use records::RecordID;
mod table_config;
pub use table_config::{TableConfig, MAX_KEY_LENGTH, DEFAULT_UTF8_TABLE};
mod key_groups;
mod sym_spell;
mod table;
pub use table::{Table};

/// Performance counters for optimizing [Table] parameters.
/// 
/// These counters don't reflect stats and totals across the whole database, rather they can
/// be reset and therefore used to measure individual operations or sequences of operations.
/// 
/// NOTE: Counters are being implemented on an as-needed basis, and many are still unimplimented
#[cfg(feature = "perf_counters")]
#[derive(Debug, Copy, Clone)]
struct PerfCounters {

    variant_load_count : usize, //The number of times we load a variant entry from the DB during a fuzzy lookup
    key_group_lookup_count : usize, //The number of time we load the keys from a key_group
    keys_found_count : usize, //The number of keys we find across all records we lookup the keys for
    max_variant_entry_refs : usize, // The number of RecordIDs in the variant with the most RecordIDs, among all variants loaded during lookups
}

#[cfg(feature = "perf_counters")]
impl PerfCounters {
    pub fn new() -> Self {
        Self {
            variant_load_count : 0,
            key_group_lookup_count : 0,
            keys_found_count : 0,
            max_variant_entry_refs : 0,
        }
    }
}



#[cfg(test)]
mod tests {
    use std::collections::{HashSet};
    use std::fs;
    use std::path::PathBuf;
    use csv::ReaderBuilder;
    use serde::{Serialize, Deserialize};

    use crate::{*};
    use crate::unicode_string_helpers::{*};

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
//GOATGOATGOAT
//let geonames_file_path = PathBuf::from("/Users/admin/Downloads/Geonames.org/cities500.txt");

    
        //Create the FuzzyRocks Table, and clear out any records that happen to be hanging out
        let config = TableConfig::<char, u8, i32, true>::default();
        let mut table = Table::new("geonames.rocks", config).unwrap();
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
            let mut names : HashSet<String> = HashSet::from_iter(geoname.alternatenames.split(',').map(|string| string.to_lowercase()));
            
            //Add the primary name for the place
            names.insert(geoname.name.to_lowercase());

            //Create a record in the table
            let names_vec : Vec<String> = names.into_iter()
                .map(|string| unicode_truncate(string.as_str(), MAX_KEY_LENGTH))
                .collect();
            record_id = table.create(&names_vec[..], &geoname.geonameid).unwrap();
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

        //Confirm we can find a known city with a longer key name (not on the fast-path)
        let rio_results : Vec<i32> = table.lookup_exact("rio de janeiro").unwrap().map(|record_id| table.get_value(record_id).unwrap()).collect();
        assert!(rio_results.contains(&3451190)); //3451190 is the geonames_id of "Rio de Janeiro"

        //Close RocksDB connection by dropping the table object
        drop(table);
        drop(london_results);

        //Reopen the table and confirm that "London" is still there
        let config = TableConfig::<char, u8, i32, true>::default();
        let table = Table::new("geonames.rocks", config).unwrap();
        let london_results : Vec<i32> = table.lookup_exact("london").unwrap().map(|record_id| table.get_value(record_id).unwrap()).collect();
        assert!(london_results.contains(&2643743)); //2643743 is the geonames_id of "London"
    }

    #[test]
    fn fuzzy_rocks_test() {

        //Create and reset the FuzzyRocks Table
        let mut config = DEFAULT_UTF8_TABLE;
        config.meaningful_key_len = 8;
        let mut table = Table::<char, u8, String, true>::new("test.rocks", config).unwrap();
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
        let results : Vec<RecordID> = table.lookup_best("Bonday").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&mon));

        //Test lookup_best, when there is no acceptable match
        let results : Vec<RecordID> = table.lookup_best("Rahu").unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test lookup_fuzzy with a perfect match, using the supplied edit_distance function
        //In this case, we should only get one match within edit-distance 2
        let results : Vec<(String, String, u8)> = table.lookup_fuzzy("Saturday", 2)
            .unwrap().map(|(record_id, distance)| {
                let (key, val) = table.get(record_id).unwrap();
                (key, val, distance)
            }).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "Saturday");
        assert_eq!(results[0].1, "Douyoubi");
        assert_eq!(results[0].2, 0);

        //Test lookup_fuzzy with a perfect match, but where we'll hit another imperfect match as well
        let results : Vec<(String, String, u8)> = table.lookup_fuzzy("Tuesday", 2)
            .unwrap().map(|(record_id, distance)| {
                let (key, val) = table.get(record_id).unwrap();
                (key, val, distance)
            }).collect();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&("Tuesday".to_string(), "Kayoubi".to_string(), 0)));
        assert!(results.contains(&("Thursday".to_string(), "Mokuyoubi".to_string(), 2)));

        //Test lookup_fuzzy where we should get no match
        let results : Vec<(RecordID, u8)> = table.lookup_fuzzy("Rahu", 2).unwrap().collect();
        assert_eq!(results.len(), 0);

        //Test lookup_fuzzy_raw, to get all of the SymSpell Delete variants
        //We're testing the fact that characters beyond `config.meaningful_key_len` aren't used for the comparison
        let results : Vec<RecordID> = table.lookup_fuzzy_raw("Sunday. That's my fun day.").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], sun);

        //Test deleting a record, and ensure we can't access it or any trace of its variants
        table.delete(tue).unwrap();
        assert!(table.get_one_key(tue).is_err());

        //Since "Tuesday" had one variant overlap with "Thursday", i.e. "Tusday", make sure we now find
        // "Thursday" when we attempt to lookup "Tuesday"
        let results : Vec<RecordID> = table.lookup_best("Tuesday").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&thu));

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
        table.add_keys(sat, &["Saturday", "Saturday"]).unwrap();
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

        let mut config = TableConfig::<u8, u8, f32, false>::default();
        config.max_deletes = 1;
        config.meaningful_key_len = 8;
        let mut table = Table::<u8, u8, f32, false>::new("test2.rocks", config).unwrap();
        table.reset().unwrap();

        let one = table.insert(b"One", &1.0).unwrap();
        let _two = table.insert(b"Dos", &2.0).unwrap();
        let _three = table.insert(b"San", &3.0).unwrap();
        let pi = table.insert(b"Pi", &3.1415926535).unwrap();

        let results : Vec<RecordID> = table.lookup_best(b"P").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&pi));
        
        let results : Vec<RecordID> = table.lookup_fuzzy_raw(b"ne").unwrap().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], one);
    }

    #[test]
    /// This tests the perf-counters
    fn perf_counters_test() {

        //Initialize the table with a very big database
        let config = TableConfig::<char, u8, i32, true>::default();
        let table = Table::new("all_cities.geonames.rocks", config).unwrap();

        //Make sure we have no pathological case of a variant for a zero-length string
        let iter = table.lookup_fuzzy_raw("").unwrap();
        assert_eq!(iter.count(), 0);

        #[cfg(feature = "perf_counters")]
        {
            //Make sure we are on the fast path that doesn't fetch the keys for the case when the key
            //length entirely fits within config.meaningful_key_len
            table.reset_perf_counters();
            let _iter = table.lookup_exact("london").unwrap();
            assert_eq!(table.perf_counters.get().key_group_lookup_count, 0);

            //Test that the other counters do something...
            table.reset_perf_counters();
            let iter = table.lookup_fuzzy("london", 3).unwrap();
            let _ = iter.count();
            assert!(table.perf_counters.get().variant_load_count > 0);
            assert!(table.perf_counters.get().key_group_lookup_count > 0);
            assert!(table.perf_counters.get().keys_found_count > 0);
            assert!(table.perf_counters.get().max_variant_entry_refs > 0);

            //Debug Prints
            println!("variant_load_count {}", table.perf_counters.get().variant_load_count);
            println!("key_group_lookup_count {}", table.perf_counters.get().key_group_lookup_count);
            println!("keys_found_count {}", table.perf_counters.get().keys_found_count);
            println!("max_variant_entry_refs {}", table.perf_counters.get().max_variant_entry_refs);
        }
        
        #[cfg(not(feature = "perf_counters"))]
        {
            println!("perf_counters feature not enabled");
        }


    }

}

//GOATGOATGOAT
//Features since last push to crates.io:
// Multi-key support
// lookup_best now returns an iterator instead of one arbitrarily-chosen record
// Support for a generic character type in key
// Adding micro-benchmarks using criterion
// Massive Perf optimizations for lookups
//  lookup_best checks lookup_exact first before more expensive lookup_fuzzy
//  key groups mingle similar keys for a record
//  optimizations to Levenstein distance function for 3x speedup
//  value table is separate from keys in DB
// 

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

//GOATGOATGOAT, Clippy, and update documentation

//GOAT Let Wolf Garbe know about my crate when I publish FuzzyRocks v0.2

