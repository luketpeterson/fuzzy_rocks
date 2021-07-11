# fuzzy_rocks Overview

A persistent datastore backed by [RocksDB](https://rocksdb.org) with fuzzy key lookup using an arbitrary
distance function accelerated by the [SymSpell](https://github.com/wolfgarbe/SymSpell) algorithm.

The reasons to use this crate over another SymSpell implementation are:
- You need to use a custom distance function
- Startup time matters more than lookups-per-second
- You care about resident memory footprint

## Records

This crate manages records, each of which has a unique [RecordID].  Keys are used to perform fuzzy
lookups but keys are not guaranteed to be unique. [Insert](Table::insert)ing the same key into a [Table] twice
will result in two distinct records.

## Usage Example

```Rust
use fuzzy_rocks::{*};

//Create and reset the FuzzyRocks Table
let mut table = Table::<String, 2, 8, true>::new("test.rocks").unwrap();
table.reset().unwrap();

//Insert some records
let thu = table.insert("Thursday", &"Mokuyoubi".to_string()).unwrap();
let wed = table.insert("Wednesday", &"Suiyoubi".to_string()).unwrap();
let tue = table.insert("Tuesday", &"Kayoubi".to_string()).unwrap();
let mon = table.insert("Monday", &"Getsuyoubi".to_string()).unwrap();

//Try out lookup_best, to get the closest fuzzy match
let result = table.lookup_best("Bonday", table.default_distance_func()).unwrap();
assert_eq!(result, mon);

//Try out lookup_fuzzy, to get all matches and their distances
let results : Vec<(RecordID, u64)> = table
    .lookup_fuzzy("Tuesday", table.default_distance_func(), 2)
    .unwrap().collect();
assert_eq!(results.len(), 2);
assert!(results.contains(&(tue, 0))); //Tuesday -> Tuesday with 0 edits
assert!(results.contains(&(thu, 2))); //Thursday -> Tuesday with 2 edits

//Retrieve the key and value from a record
let (key, val) = table.get_record(wed).unwrap();
assert_eq!(key, "Wednesday");
assert_eq!(val, "Suiyoubi");
```

Additional usage examples can be found in the tests, located at the bottom of the `src/lib.rs` file.

## Distance Functions

A distance function is any function that returns a scalar distance between two keys.  The smaller the
distance, the closer the match.  Two identical keys must have a distance of [zero](num_traits::Zero).  The `fuzzy` methods
in this crate, such as [lookup_fuzzy](Table::lookup_fuzzy), require a distance function to be supplied.

This crate includes a simple [Levenstein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) function
called [edit_distance](Table::edit_distance).  However, you may often want to use a different function.

One reason to use a custom distance function is to account for expected variation patterns. For example:
a distance function that considers likely [OCR](https://en.wikipedia.org/wiki/Optical_character_recognition)
errors might consider 'lo' to be very close to 'b', '0' to be extremely close to 'O', and 'A' to be
somewhat near to '^', while '#' would be much further from ',' even though the Levenstein distances
tell a different story with 'lo' being two edits away from 'b' and '#' being only one edit away from
',' (comma).

You may have a different distance function to catch common typos on a QWERTY keyboard, etc.

Another reason for a custom distance function is if your keys are not human-readable strings, in which
case you may need a different interpretation of variances between keys.  For example DNA snippets could
be used as keys.

Any distance function you choose must be compatible with SymSpell's delete-distance optimization.  In other
words, you must be able to delete no more than `MAX_DELETES` characters from each of the record's
key and the lookup key and arrive at identical key-variants.  If your distance function is incompatible
with this property then the SymSpell optimization won't work for you and you should use a different fuzzy
lookup technique and a different crate.

Distance functions may return any scalar type, so floating point distances will work.  However, the
`MAX_DELETES` constant is an integer.  Records that can't be reached by deleting `MAX_DELETES` characters
from both the record key and the lookup key will never be evaluated by the distance function and are
conceptually "too far away".  Once the distance function has been evaluated, its return value is
considered the authoritative distance and the delete distance is irrelevant.

## Unicode Support

A [Table] may allow for unicode keys or not, depending on the value of the `UNICODE_KEYS` constant used
when the Table was created.

If `UNICODE_KEYS` is `true`, keys may use unicode characters and multi-byte characters will still be
considered as single characters for the purpose of deleting characters to create key variants.

If `UNICODE_KEYS` is `false`, keys are just strings of [u8] characters.
This option has better performance.

## Performance Characteristics

This crate is designed for large databases where startup time and resident memory footprint are significant
considerations.  This create has been tested with 250,000 unique keys and about 10 million variants.

If your use-case can cope with a higher startup latency and you are ok with all of your keys and
variants being loaded into memory, then query performance will certainly be better using a solution
built on Rust's native collections, such as this [symspell](https://crates.io/crates/symspell)
crate on [crates.io](http://crates.io).

**NOTE**: The included `geonames_megacities.txt` file is a stub for the `geonames_test`, designed to stress-test
this crate.  The abriged file is included so the test will pass regardless, and to avoid bloating the
download.  The content of `geonames_megacities.txt` was derived from data on [geonames.org](http://geonames.org),
and licensed under a [Creative Commons Attribution 4.0 License](https://creativecommons.org/licenses/by/4.0/legalcode)
