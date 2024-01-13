//!
//! The TableConfig module contains the parameters for configuring a Table.  The TableConfig struct
//! is re-exported.
//!

use core::hash::Hash;
use core::cmp::min;

use std::mem::MaybeUninit;

use num_traits::Zero;
use serde::Serialize;

/// The maximum number of characters allowable in a key.  Longer keys will cause an error
pub const MAX_KEY_LENGTH : usize = 95;

// NOTE: The #![feature(const_generics)] feature isn't stabilized and I don't want to depend on
// any unstable features.  So instead of taking the config structure as a const parameter to
// Table, the compile-time arguments will be passed individually using the capabilities of
// #![feature(min_const_generics)], and I'll hide that from novice API users by allowing the
// compiler to infer the values from phantoms in the config structure.

/// The TableConfig structure specifies all of the parameters for configuring a FuzzyRocks [Table](crate::Table)
///
/// ## An example creating a [Table](crate::Table) using a custom [TableConfig]
/// ```
/// use fuzzy_rocks::{*};
///
/// struct Config();
/// impl TableConfig for Config {
///     type KeyCharT = char;
///     type DistanceT = u8;
///     type ValueT = String;
///     type CoderT = BincodeCoder;
///     const UTF8_KEYS : bool = true;
///     const MAX_DELETES : usize = 2;
///     const MEANINGFUL_KEY_LEN : usize = 12;
///     const GROUP_VARIANT_OVERLAP_THRESHOLD : usize = 5;
///     const DISTANCE_FUNCTION : DistanceFunction<Self::KeyCharT, Self::DistanceT> = Self::levenstein_distance;
/// }
/// let mut table = Table::<Config, true>::new("config_example.rocks", Config()).unwrap();
/// ```
///
pub trait TableConfig {

    //TODO: Consider giving the associated types default values, as soon as the feature is stabilized in Rust.
    //https://github.com/rust-lang/rust/issues/29661

    /// A generic type that specifies the unit of deletion for the SymSpell algorithm.  
    /// Valid [Key](crate::Key) types for a [Table](crate::Table) must be losslessly convertible to and from `Vec<KeyCharT>`.
    ///
    /// In a typical implementation, `KeyCharT` is a [char] for unicode keys or a [u8] for simple [ASCII](https://en.wikipedia.org/wiki/ASCII) keys,
    /// although it could be a data type of another size.  `KeyCharT` must implement the [Copy] trait,
    /// so that keys will be contiguous in memory and may not include any references.
    type KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned;

    /// A generic type that represents a scalar distance in the [Metric Space](https://en.wikipedia.org/wiki/Metric_space) that contains
    /// all keys in the [Table](crate::Table).  A `DistanceT` is the return type of the [DISTANCE_FUNCTION](TableConfig::DISTANCE_FUNCTION).
    ///
    /// `DistanceT` may be any scalar type and is not necessarily required to be an integer.  It is often desireable to
    /// use fractional types to express more precision than integers, however, the use of floating point types for
    /// `DistanceT` is discouraged on account of rounding issues causing problems for equality comparison.  Therefore
    /// a fixed-point alternative type is superior.
    ///
    ///  Confusingly, the [MAX_DELETES](TableConfig::MAX_DELETES) const configuration parameter *is* an integer.  Conceptually,
    /// the SymSpell delete-distance pruning is a filter that reduces the number of candidate keys that must be
    /// tested using the [DISTANCE_FUNCTION](TableConfig::DISTANCE_FUNCTION).  Records that can't be reached
    /// by deleting [MAX_DELETES](TableConfig::MAX_DELETES) characters from both the record key and the lookup key
    /// will never be evaluated by the distance function and are conceptually "too far away" from the lookup key.
    type DistanceT : 'static + Copy + Zero + PartialOrd + PartialEq + From<u8>;

    /// A generic type that represents a payload value associated with a record.  `ValueT` must
    /// be able to be serialized and deserialized from the database but otherwise is not constrained.
    type ValueT : 'static + Serialize + serde::de::DeserializeOwned;

    //TODO document this
    type CoderT : 'static + crate::Coder + Send + Sync;

    /// A `const bool` that specifies whether the keys are [UTF-8](https://en.wikipedia.org/wiki/UTF-8) encoded [Unicode](https://en.wikipedia.org/wiki/Unicode) strings or not. 
    ///
    /// If `UTF8_KEYS = true`, common key characters will be encoded to consume only 8 bits, (as opposed to 24 bit
    /// unicode [char]s, which are often padded to 32 bits), thus improving database size and
    /// performance, but there is additional runtime overhead to encode and decode this format.  `UTF8_KEYS = true` is only allowed if `KeyCharT = char`.
    ///
    /// If `UTF8_KEYS = false`, all keys are stored as vectors of [KeyCharT](TableConfig::KeyCharT), meaning there is no cost to encoding
    /// and decoding them, but the database performance may suffer.
    ///
    /// In general, if your keys are unicode strings, `UTF8_KEYS = true` is advised, and if they aren't, then
    /// `UTF8_KEYS = false` is probably required.
    const UTF8_KEYS : bool = true;

    /// The number of deletes to store in the database for variants created
    /// by the SymSpell optimization.  If `MAX_DELETES` is too small, the variant will not be found
    /// and therefore the [DISTANCE_FUNCTION](TableConfig::DISTANCE_FUNCTION) will not have an opportunity to evaluate the match.  However,
    /// if `MAX_DELETES` is too large, it will hurt performance by finding and evaluating too many
    /// candidate keys.
    ///
    /// Empirically, values near 2 seem to be good in most situations I have found.  I.e. 1 and 3 might be
    /// appropriate sometimes.  4 ends up exploding in most cases I've seen so the SymSpell logic may not
    /// be a good fit if you need to find keys 4 edits away.  0 edits is an exact match.
    const MAX_DELETES : usize = 2;

    /// `MEANINGFUL_KEY_LEN` controls an optimization where only a subset of the key is used for creating
    /// variants.  For example, if `MEANINGFUL_KEY_LEN = 10` then only the first 10 characters of the key will be used
    /// to generate and search for variants.
    ///
    /// This optimization is predicated on the idea that long key strings will not be very similar to each
    /// other, and a certain number of characters is sufficient to substantially narrow down the search.
    ///
    /// For example the key *incomprehensibilities* will cause variants to be generated for *incomprehe*
    /// with a `MEANINGFUL_KEY_LEN` of 10, meaning that a search for *incomprehension* would find *incomprehensibilities*
    /// and evauate it with the [DISTANCE_FUNCTION](TableConfig::DISTANCE_FUNCTION) even though it is further than [MAX_DELETES](TableConfig::MAX_DELETES).
    ///
    /// In a dataset where many keys share a common prefix, or where keys are organized into a namespace by
    /// concatenating strings, this optimization will cause problems and you should either pass a high number
    /// to effectively disable it, or rework the code to use different logic to select a substring
    ///
    /// The distance function will always be invoked with the entire key, regardless of the value of
    /// `MEANINGFUL_KEY_LEN`.
    ///
    /// If this value is set too high, the number of variants in the database will increase.  If this value
    /// is set too low, the SymSpell filtering will be less effective and the distance function will be
    /// invoked unnecessarily, hurting performance.  However, the value of this field will not affect
    /// the correctness of the results.
    const MEANINGFUL_KEY_LEN : usize = 12;

    /// The number of variants a given key must share with the other keys in an existing key group, in
    /// order for the key to be added to the key group rather than being placed into a new separate key
    /// group.
    ///
    /// Setting this number to 0 puts every key for a record together in a group, while a large number
    /// results in each key being assigned to its own group.
    ///
    /// Creating excess groups when for keys that will likely be fetched together anyway leads to unnecessay
    /// database fetches and bloats the variant entries with additional `KeyGroupID`s.  On the other hand,
    /// combining distant keys into together in a key_group leads to unnecessary invocations of the
    /// [DistanceFunction].
    ///
    /// NOTE: We arrived at the default value (5) empirically by testing a number of different values and
    /// observing the effects on lookup speed, DB construction speed, and DB size.  The observed data
    /// points are checked in, in the file: `misc/perf_data.txt`
    const GROUP_VARIANT_OVERLAP_THRESHOLD : usize = 5;

    /// The `DISTANCE_FUNCTION` is a [DistanceFunction] associated with a [Table](crate::Table) and defines
    /// the [Metric Space](https://en.wikipedia.org/wiki/Metric_space) that contains all [Key](crate::Key)s in the Table.
    const DISTANCE_FUNCTION : DistanceFunction<Self::KeyCharT, Self::DistanceT> = Self::levenstein_distance;

    /// An implementation of the basic [Levenstein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) function, which is used by the [DefaultTableConfig],
    /// and may be used anywhere a distance function is required.
    ///
    /// This implementation uses the Wagner-Fischer Algorithm, as it's described [here](https://en.wikipedia.org/wiki/Wagner%E2%80%93Fischer_algorithm)
    fn levenstein_distance(key_a : &[Self::KeyCharT], key_b : &[Self::KeyCharT]) -> Self::DistanceT {

        let m = key_a.len()+1;
        let n = key_b.len()+1;

        //Allocate a 2-dimensional vec for the distances between the first i characters of key_a
        //and the first j characters of key_b
        #[allow(clippy::uninit_assumed_init)]
        let mut d : [[MaybeUninit<u8>; MAX_KEY_LENGTH + 1]; MAX_KEY_LENGTH + 1] = [[MaybeUninit::uninit(); MAX_KEY_LENGTH + 1]; MAX_KEY_LENGTH + 1];

        //NOTE: I personally find this (below) more readable, but clippy really like the other style.  -\_(..)_/-
        // for i in 1..m {
        //     d[i][0] = i;
        // }
        for (i, row) in d.iter_mut().enumerate().skip(1) {
            unsafe{ *(row[0].as_mut_ptr()) = i as u8; }
        }

        // for j in 1..n {
        //     d[0][j] = j as u8;
        // }
        for (j, element) in d[0].iter_mut().enumerate() {
            unsafe{ *element.as_mut_ptr() = j as u8; }
        }

        for j in 1..n {
            for i in 1..m {

                //TODO: There is one potential optimization left.  There is no reason to allcate a whole
                // square buffer of MAX_KEY_LENGTH on a side, because we never look back before the previous
                // line in the buffer.  A more cache-friendly approach might be to allocate a
                // MAX_KEY_LENGTH x 2 buffer, and then alternate between writing the values in one line
                // and reading them from the previous line.

                let substitution_cost = if key_a[i-1] == key_b[j-1] {
                    0
                } else {
                    1
                };

                //let deletion_distance = d[i-1][j] + 1;
                let deletion_distance = unsafe {d.get_unchecked(i-1).get_unchecked(j).assume_init()} + 1;
                //let insertion_distance = d[i][j-1] + 1;
                let insertion_distance = unsafe {d.get_unchecked(i).get_unchecked(j-1).assume_init()} + 1;
                //let substitution_distance = d[i-1][j-1] + substitution_cost;
                let substitution_distance = unsafe {d.get_unchecked(i-1).get_unchecked(j-1).assume_init()} + substitution_cost;

                let smallest_distance = min(min(deletion_distance, insertion_distance), substitution_distance);

                //d[i][j] = smallest_distance;
                let element = unsafe{ d.get_unchecked_mut(i).get_unchecked_mut(j) };
                unsafe{ *element.as_mut_ptr() = smallest_distance; }
            }
        }

        Self::DistanceT::from(unsafe{ d[m-1][n-1].assume_init() })
    }
}

/// A type for a function to compute the distance between two keys. Used in a [TableConfig]
///
/// A `DistanceFunction` can be any function that returns a scalar distance when given two keys.  The smaller the
/// distance, the closer the match.  Two identical keys must have a distance of [zero](num_traits::Zero).  The `fuzzy` methods
/// in this crate, such as [lookup_fuzzy](crate::Table::lookup_fuzzy), invoke the distance function to determine
/// if two keys adequately match.
///
/// This crate includes a simple [Levenstein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) function
/// called [levenstein_distance](TableConfig::levenstein_distance).  However, you may often want to use a different function.
///
/// One reason to use a custom distance function is to account for expected error patterns. For example:
/// a distance function that considers likely [OCR](https://en.wikipedia.org/wiki/Optical_character_recognition)
/// errors might consider 'lo' to be very close to 'b', '0' to be extremely close to 'O', and 'A' to be
/// somewhat near to '^', while '!' would be much further from '@' even though the Levenstein distances
/// tell a different story with 'lo' being two edits away from 'b' and '!' being only one edit away from
/// '@'.
///
/// In another situation, you may want to use a custom distance function that is aware of key positions on a QWERTY keyboard, and
/// thus able to identify likely typos.  In such a distance function, '!' and '@' are now very close
/// because they are adjacent keys.
///
/// In another example, a distance function may be used to identify words that are similar in pronunciation,
/// like the [Soundex](https://en.wikipedia.org/wiki/Soundex) algorithm, or you may have any number of
/// other application-specific requirements.
///
/// Another reason for a custom distance function is if your keys are not human-readable strings, in which
/// case you may need a different interpretation of variances between keys.  For example DNA snippets could
/// be used as keys to search for genetic mutations.
///
/// Distance functions must return a [DistanceT](TableConfig::DistanceT).
///
/// Any distance function you choose must be compatible with SymSpell's delete-distance optimization.  In other
/// words, you must be able to delete no more than [MAX_DELETES](TableConfig::MAX_DELETES) characters from both
/// a given record's key and the lookup key and arrive at identical key-variants.  If your distance function
/// is incompatible with this property then the SymSpell optimization won't work for you and you should use
/// a different fuzzy lookup technique and a different crate.
///
/// Here is more information on the [SymSpell algorithm](https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f).
///
/// Once the distance function has been evaluated, its return value is considered the authoritative distance
/// between the two keys, and the delete distance is irrelevant from that point onwards.
pub type DistanceFunction<KeyCharT, DistanceT> = fn(key_a : &[KeyCharT], key_b : &[KeyCharT]) -> DistanceT;

/// A struct that implements [TableConfig] with default values.  This can be passed as a convenience
/// when a default configuration for [Table](crate::Table) is acceptable
pub struct DefaultTableConfig();

impl TableConfig for DefaultTableConfig {
    type KeyCharT = char;
    type DistanceT =  u8;
    type ValueT = String;
    type CoderT = crate::BincodeCoder;
    const UTF8_KEYS : bool = true;
    const MAX_DELETES : usize = 2;
    const MEANINGFUL_KEY_LEN : usize = 12;
    const GROUP_VARIANT_OVERLAP_THRESHOLD : usize = 5;
    const DISTANCE_FUNCTION : DistanceFunction<Self::KeyCharT, Self::DistanceT> = Self::levenstein_distance;
}
