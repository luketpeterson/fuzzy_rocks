# fuzzy_rocks Overview

A persistent datastore backed by [RocksDB](https://rocksdb.org) with fuzzy key lookup using an arbitrary
distance function accelerated by the [SymSpell](https://github.com/wolfgarbe/SymSpell) algorithm.

The reasons to use this crate over another SymSpell implementation are:
- You have non-standard key characters (e.g. DNA snippets, etc.)
- You want to use a custom distance function
- Startup time matters (You can't recompute key variants at load time)
- You have millions of keys and care about memory footprint

## Records & Keys

A [Table] contains records, each of which has a unique [RecordID], and each record is associated
with one or more [Key]s.  Keys are used to perform fuzzy lookups of records.  A [Key] is typically
a [String] or an [&str], but may be any number of collections of [KeyCharT](TableConfig#keychart), such
as a [Vec], [Array](array), or [Slice](slice).

Keys are not required to be unique in the Table and multiple records may have keys in common.  All
of the lookup methods may return multiple records if the lookup key and other criteria matches
more than one record in the Table.

## Usage Examples

A simple use case with a default [Table] configuration using `&str`s as keys.
```rust
use fuzzy_rocks::{*};

//Create and reset the FuzzyRocks Table
let mut table = Table::<DefaultTableConfig, true>::new("test.rocks", DefaultTableConfig()).unwrap();
table.reset().unwrap();

//Insert some records
let thu = table.insert("Thursday", &"Mokuyoubi".to_string()).unwrap();
let wed = table.insert("Wednesday", &"Suiyoubi".to_string()).unwrap();
let tue = table.insert("Tuesday", &"Kayoubi".to_string()).unwrap();
let mon = table.insert("Monday", &"Getsuyoubi".to_string()).unwrap();

//Use lookup_best, to get the closest fuzzy match
let result = table.lookup_best("Bonday")
    .unwrap().next().unwrap();
assert_eq!(result, mon);

//Use lookup_fuzzy, to get all matches and their distances
let results : Vec<(RecordID, u8)> = table
    .lookup_fuzzy("Tuesday", Some(2))
    .unwrap().collect();
assert_eq!(results.len(), 2);
assert!(results.contains(&(tue, 0))); //Tuesday -> Tuesday with 0 edits
assert!(results.contains(&(thu, 2))); //Thursday -> Tuesday with 2 edits

//Retrieve a key and the value from a record
assert_eq!(table.get_one_key(wed).unwrap(), "Wednesday");
assert_eq!(table.get_value(wed).unwrap(), "Suiyoubi");
```

Another use case with a [Table] that stores (simplified) DNA sequences.
For a more comprehensive representation of the format for biological molecules, look at the [FASTA format](https://en.wikipedia.org/wiki/FASTA_format).
```rust
use fuzzy_rocks::{*};

//A simplified data type that might represent a Nucleobase.
// https://en.wikipedia.org/wiki/Nucleobase
#[derive(Copy, Clone, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
enum Nucleobase {
    A, // adenine
    C, // cytosine
    G, // guanine
    T, // thymine
    U, // uracil
}

struct Config();
impl TableConfig for Config {
    type KeyCharT = Nucleobase;
    type DistanceT = u8;
    type ValueT = usize;
    const UTF8_KEYS : bool = false;
    const MAX_DELETES : usize = 2;
    const MEANINGFUL_KEY_LEN : usize = 24;
    const GROUP_VARIANT_OVERLAP_THRESHOLD : usize = 5;
    const DISTANCE_FUNCTION : DistanceFunction<Self::KeyCharT, Self::DistanceT> = Self::levenstein_distance;
}

//Create and reset the FuzzyRocks Table
let mut table = Table::<Config, false>::new("test.rocks", Config()).unwrap();
``` 

Additional usage examples can be found in the tests, located at the bottom of the [src/lib.rs](https://github.com/luketpeterson/fuzzy_rocks/blob/main/src/lib.rs) file.

## Table Configuration

A [TableConfig] object is passed as an argument to [Table::new].  The TableConfig specifies a number
of things about the table, including:

- Data Types that define the structure and representation of certain aspects of the [Table]
- Tuning Parameters to affect the performance of various operations
- A Distance Function to calculate the distance between two keys in a [Metric Space](https://en.wikipedia.org/wiki/Metric_space)

[DefaultTableConfig] is a zero-sized type that implements a default [TableConfig] for UTF-8 keys.  This will be sufficient for many situations.
If you need to customize the TableConfig, more details about the type parameters and fields can be found in the documentation
for [TableConfig].

### Unicode and UTF-8 Support

A [Table] may be configured to encode keys as [UTF-8](https://en.wikipedia.org/wiki/UTF-8) or not, depending on your requirements.
This is configured through the [TableConfig] object's [UTF8_KEYS](TableConfig::UTF8_KEYS) constant.

## Algorithm Details

The authoritative description of SymSpell is the ReadMe for the [SymSpell project](https://github.com/wolfgarbe/SymSpell).

The fuzzy_rocks implementation has a few additional details to be aware of:

- fuzzy_rocks won't find keys that don't have at least one character in common, regardless of the value
of [MAX_DELETES](TableConfig::MAX_DELETES).  For example the key `me` won't be found by the query string `hi`, even with a distance
of 2 (or any other value).  This decision was made because the variant space becomes very crowded
for short keys, and the extreme example of the empty-string variant was severely damaging performance with
short keys.

## Performance Characteristics

This crate is designed for large databases where startup time and resident memory footprint are significant
considerations.  This create has been tested with 200,000 records cumulatively having over 1 million keys,
and about 140 million key variants.  In this situation, a fuzzy lookup was between 500us (microseconds)
and 1ms, running on my laptop - which I consider to be very expensive in absolute terms, but acceptable
for many use cases.

The performance will also vary greatly depending on the key distribution and the table parameters.  Keys
that are distinct from eachother will lead to faster searches vs. keys that share many variants in common.

### Tuning for Performance

Performance is highly dependent on the values for the [TableConfig] used when creating the [Table], but
the values must match the characteristics of the keys in the data set.

Briefly, the tuning parameters are:

[MAX_DELETES](TableConfig::MAX_DELETES): A smaller `MAX_DELETES` value will perform exponentially better but be able to find
fewer results for a search.  `MAX_DELETES` should be tuned to be as small as you can make it, but no smaller. ;-)

[MEANINGFUL_KEY_LEN](TableConfig::MEANINGFUL_KEY_LEN): A higher value for `MEANINGFUL_KEY_LEN` will result in fewer wasted evaluations of the distance function
but will lead to more entries in the variants database and thus reduced database performance.

[GROUP_VARIANT_OVERLAP_THRESHOLD](TableConfig::GROUP_VARIANT_OVERLAP_THRESHOLD) controls the logic about when
a key is merged with an existing `key_group` vs. when a new `key_group` is created.

More detailed information on these tuning parameters can be found in the docs for [TableConfig].

If your use-case can cope with a higher startup latency and you are ok with all of your keys and
variants being loaded into memory, then query performance will certainly be better using a solution
built on Rust's native collections, such as this [symspell](https://crates.io/crates/symspell)
crate on [crates.io](http://crates.io).

### Performance Counters

You can use the [PerfCounterFields] to measure the number of internal operations being performed, and use
that information to adjust the [TableConfig] parameters.

First, you must enable the `perf_counters`
feature in the `Cargo.toml` file, with an entry similar to this:

```toml
[dependencies]
fuzzy_rocks = { version = "0.2.0", features = ["perf_counters"] }
```

Then, the performance counters may be reset by calling [Table::reset_perf_counters] and read by calling [Table::get_perf_counters].

### Benchmarks

Fuzzy_rocks contains a (small but growing) suite of benchmarks, implemented with [criterion](https://docs.rs/criterion).
These can be invoked with:

```sh
cargo bench -- --nocapture
```

## Database Format

DB contents are encoded using the [bincode] crate.  Currently the database contains 4 Column Families.

1. The "rec_data" CF uses a little-endian-encoded [RecordID] as its key, and stores a varint-encoded `Vec` of
    integers, which represent key_group indices, each of which can be combined with a `RecordID` to create a
    `KeyGroupID`.  Each referenced key_group contains at least one key associated with the record.
    The `rec_data` CF is the place to start when constructing the complete set of keys associated with a record.

2. The "keys" CF uses a little-endian-encoded `KeyGroupID` as its key, and stores a varint-encoded `Vec` of
    OwnedKeys (think Strings), each representing a key in a key_group.  In the present implementation,
    a given key_group only stores keys for a single record, and the `KeyGroupID` embeds the associated
    [RecordID] in its lower 44 bits.  This scheme is likely to change in the future.

3. The "variants" CF uses a serialized key variant as its key, and stores a fixint-encoded `Vec` of
    `KeyGroupID`s representing every key_group that holds a key that can be reduced to this variant.
    Complete key strings themselves are represented as variants in this CF.

4. The "values" CF uses a little-endian-encoded [RecordID] as its key, and stores the [bincode] serialized
    [ValueT](TableConfig::ValueT) associated with the record.

## Future Work

1. Optimization for crowded neighborhoods in the key metric-space.  The current design optimizes for the case
    where a single record has multiple keys with overlapping variants.  This is an efficient design in the
    case where many proximate keys belong to the same record, as is the case when multiple spelling variations
    for the same word are submitted to the Table.

    First, here is some background on how we got to this design.  In v0.2.0, multi-key
    support was added because many use cases involved creating multiple records that were conceptually the same
    record in order to represent key variations such as multiple valid spellings of the same thing. Becauase
    these keys are often similar to each other and shared many variants in the database, the key_groups feature
    was added to improve performance by reducing the number of keys in each variant entry.  Ideally
    a given variant entry would only contain one reference to a record even if that record had many similar keys.

    Unfortunately this meant all the record's keys would be loaded together and that lead to unnecessary
    invocations of the distance function to evaluate all of the keys for each record.  The solution in v0.2.0
    was to segment a record's keys into key_groups, so similar keys would be together in the same key group
    but other keys needn't be tested.  This solution works well for individual records with many keys that
    cluster together.

    However, a large performance problem still exists when many keys from *Different* records cluster together.
    To address this, I think that a different database layout is needed, and here is my thinking:

    - Create a separate CF to store full keys is needed, and each key entry would contain the [RecordID]s
        assiciated with that key. There are two possible implementations:

        - A more compact implementation for longer keys would be to have a "KeyID" to reference each key entry.
            Then the entry would contain the key itself, and the [RecordID]s associated with the key in the same
            entry or in another CF that is also indexed by `KeyID`.  In this implementation, the variant CF is
            still used to find the KeyID, and perhaps we could establish a convention where a variant keeps the
            KeyID for an exact match at the beginning of the list.
        - An implementation that requires less lookup indirection would be to use the exact keys themselves,
            (or perhaps meaningful keys. See [TableConfig::MEANINGFUL_KEY_LEN]) as the Rocks keys to the CF.
            Then we would separate the exact keys from the "variants" CF, and keys themselves wouldn't be placed
            into the "variants" CF.  Each variant entry would include all of the full keys that it is a variant of.

        The decision about which of these implementations is better depends on the average key length.  A 64-bit
        KeyID is the equivalent of an 8-byte key, but an average key is around 16 bytes.  On the other hand,
        eliminating the indirection associated with a second DB query might tip the scales in favor of embedding
        the keys directly.
    
    - The changes above address many records with the same keys, but we may still want to retain the optimization
        where many similar keys are implemented under only one `KeyGroupID` in a variant entry, instead of one
        reference for each individual key.  Especially if those keys are fetched together often.
    
        So, I believe the best path forward looks more like the first option above.  So we would:
        
        - Rework the `KeyGroupID` so it no longer implies a [RecordID] within its lower 44 bits, and is instead
            a completely different number, assigned as needed.

        - Rework the key_group entries so that each key is associated with a list of [RecordID]s.

        - Rework the "rec_data" CF, so that, instead of a list of key_group indices (which are then converted
            into `KeyGroupID`s), the "rec_data" entry would store the complete list of keys for the record.

        I believe this approach will improve lookup performance when there are many similar keys belonging to
        different records.  I can identify several downsides however:

        - The lookup_exact function no longer has the existing fast path because it must pull the key_group
            entry no matter what, in order to get the [RecordID]s.  This can be partially ameliorated by moving
            the exact match key_group to the beginning of a variant's list.  In addition, the current fast-path
            has a BUG! (see item 8. in this list.)

        - The key_group entry will be more expensive to parse on account of it containing a vector of [RecordID]s
            associated with each key.  However, this will be offset by needing to load many fewer key_group
            entries, so I think this may be a net win - or at least a wash.

        - The "rec_data" CF will bloat as each key group index (about 1 byte) is replaced by a whole key (avg 16
            bytes).  However, the rec_data is only loaded for bookkeeping operations, and doesn't figure into the
            fuzzy lookup speed.  Therefore the main impact will be DB size, and we can expect a 5-10% increase
            from this factor.

2. BK-Tree search within a key-group.  If we end up with many proximate keys, there may be an advantage to having
    a secondary search structure within the table.  In fact, I think a BL-Tree always outperforms a list, so
    the real question is whether the performance boost justifies the work and code complexity.

    SymSpell is very well optimized for sparse key spaces, but in dense key spaces (where many keys share many
    variants), the SymSpell optimization still results in the need to test hundreds of keys with the distance
    function.  In this case, a BK-Tree may speed up the fuzzy lookups substantially.

3. Multi-threading within a lookup. Rework internal plumbing so that variant entries can be fetched and processed
    in parallel, leading to parallel evaluation of key groups and parallel execution of distance functions.

4. Investigate adding a "k-nn lookup" method that would return up to k results, ordered by their distance from the
    lookup key.  Perhaps in addition to, or instead of, the [Table::lookup_best] method, because this new method is
    essentially a generalization of [Table::lookup_best]

    This idea is a little bit problematic because a function that returns exactly k results cannot be deterministic
    in cases where there are equal-distance results.  We will often find that k straddles a border and the only
    way to return a deterministic result set is to return more than k results. (or fewer than k, but that may
    mean a set with no results, which is probably not what the caller wants.)

5. Save the TableConfig to the database, in order to detect an error when the config changes in a way that makes
    the database invalid.  Also include a software version check, and create a function to represent which
    software versions contain database format compatibility breaks.  Open Question: Should we store a checksum
    of the distance function?  The function may be re-compiled or changed internally without changing behavior,
    but we can't know that.

6. Remove the `KeyUnsafe` trait as soon as "GenericAssociatedTypes" is stabilized in Rust.  
    <https://github.com/rust-lang/rust/issues/44265>.  As soon as I can implement an associated type with an
    internal lifetime that isn't a parameter to the Key trait itself, then I'll be able to return objects that
    have the same base type but a shorter associated lifetime, and thus eliminate the need to transmute lifetimes.

7. Investigate tools to detect when a [Table] parameter is tuned badly for a data set, based on known optimal
    ratios for certain performance counters.  Alternatively, build tools to assist in performing a parameter
    search optimization process, that could automatically sweep the parameters, searching for the optimal
    [TableConfig] for a given data set.

8. Fix BUG! in [Table::lookup_exact] caused by the optimization where we don't load the key_group associated with
    the exact variant, so we may return records whose keys are supersets of the lookup key.  NOTE: This will
    likely be fixed as a side-effect of the changes proposed in 1.

## Release History

### 0.2.0
- Multi-key support. Records may now be associated with more than one key
- Lookup_best now returns an iterator instead of one arbitrarily-chosen record
- Support for Keys composed of a generic KeyCharT type, vs. only ASCII or UTF-8
- Central TableConfig trait to centralize all tuning parameters
- Added micro-benchmarks using Criterion
- Added `perf_counters` feature
- Massive performance optimizations for lookups (10x-100x) in some cases
    - lookup_best checks lookup_exact first before more expensive lookup_fuzzy
    - key_groups mingle similar keys for a record
    - micro-optimizations to levenstein_distance function for 3x speedup
    - record value stored separately from keys in DB, so Value isn't parsed unnecessarily

### 0.1.1
- Initial Release

## Misc

**NOTE**: The included `geonames_megacities.txt` file is a stub for the `geonames_test`, designed to stress-test
this crate.  The abriged file is included so the test will pass regardless, and to avoid bloating the
download.  The content of `geonames_megacities.txt` was derived from data on [geonames.org](http://geonames.org),
and licensed under a [Creative Commons Attribution 4.0 License](https://creativecommons.org/licenses/by/4.0/legalcode)
