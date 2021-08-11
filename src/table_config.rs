//!
//! The TableConfig module contains the parameters for configuring a Table.  The TableConfig struct
//! is re-exported.
//! 

use core::marker::PhantomData;
use core::hash::Hash;
use core::cmp::{min};

use std::mem::{MaybeUninit};

use num_traits::Zero;
use serde::{Serialize};

/// The maximum number of characters allowable in a key.  Longer keys will cause an error
pub const MAX_KEY_LENGTH : usize = 95;

// NOTE: The #![feature(const_generics)] feature isn't stabilized and I don't want to depend on
// any unstable features.  So instead of taking the config structure as a const parameter to
// Table, the compile-time arguments will be passed individually using the capabilities of
// #![feature(min_const_generics)], and I'll hide that from novice API users by allowing the
// compiler to infer the values from phantoms in the config structure.

/// The TableConfig structure specifies all of the parameters for configuring a FuzzyRocks [Table](crate::Table)
/// 
/// ## Type Parameters
/// 
/// ### KeyCharT
/// **`KeyCharT`** is a generic type that specifies the unit of deletion for the SymSpell algorithm.  In
/// a typical implementation, this is a [char] for unicode keys or a [u8] for simple [ASCII](https://en.wikipedia.org/wiki/ASCII) keys,
/// although it could be a data type of another size.  KeyCharT must implement the [Copy] trait,
/// in other words, keys must be contiguous in memory and may not include any references.
/// 
/// ### DistanceT
/// **`DistanceT`** is a generic type that represents a scalar distance in the [Metric Space](https://en.wikipedia.org/wiki/Metric_space) that contains
/// all keys in the [Table](crate::Table).  While it is often desireable to use fractional types to express more
/// precision than integers, the use of floating point types is discouraged on account of their
/// inability to be reliably compared, so a fixed-point alternative type is superior.
/// 
/// ### ValueT
/// **`ValueT`** is a generic type that represents a payload value associated with a record.  ValueT must
/// be able to be serialized and deserialized from the database but otherwise is not constrained.
/// 
/// ### UTF8_KEYS
/// **`UTF8_KEYS`** is a `const bool` that specifies whether the keys are [UTF-8](https://en.wikipedia.org/wiki/UTF-8) encoded [Unicode](https://en.wikipedia.org/wiki/Unicode) strings or not. 
/// 
/// If `UTF8_KEYS = true`, common key characters will be encoded to consume only 8 bits, (as opposed to 24 bit
/// unicode [char]s, which are often padded to 32 bits), thus improving database size and
/// performance, but there is additional runtime overhead to encode and decode this format.  `UTF8_KEYS = true` is only allowed if `KeyCharT = char`.
/// 
/// If `UTF8_KEYS = false`, all keys are stored as vectors of [KeyCharT](#keychart), meaning there is no cost to encoding
/// and decoding them, but the database performance may suffer.
/// 
/// In general, if your keys are unicode strings, `UTF8_KEYS = true` is advised, and if they aren't, then
/// `UTF8_KEYS = false` is probably required.
/// 
/// ## Distance Function
/// 
/// A distance function is any function that returns a scalar distance between two keys.  The smaller the
/// distance, the closer the match.  Two identical keys must have a distance of [zero](num_traits::Zero).  The `fuzzy` methods
/// in this crate, such as [lookup_fuzzy](crate::Table::lookup_fuzzy), invoke the distance function to determine
/// if two keys adequately match.
/// 
/// This crate includes a simple [Levenstein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) function
/// called [edit_distance](TableConfig::edit_distance).  However, you may often want to use a different function.
/// 
/// One reason to use a custom distance function is to account for expected error patterns. For example:
/// a distance function that considers likely [OCR](https://en.wikipedia.org/wiki/Optical_character_recognition)
/// errors might consider 'lo' to be very close to 'b', '0' to be extremely close to 'O', and 'A' to be
/// somewhat near to '^', while '!' would be much further from '@' even though the Levenstein distances
/// tell a different story with 'lo' being two edits away from 'b' and '!' being only one edit away from
/// '@'.
/// 
/// You may want to use a custom distance function that is aware of key positions on a QWERTY keyboard, and
/// thus able to identify likely typos.  In such a distance function, '!' and '@' are now very close
/// because they are adjacent keys.
/// 
/// In another example, a distance function may be used to identify words that are similar in pronunciation,
/// like the [Soundex](https://en.wikipedia.org/wiki/Soundex) algorithm, or you may have any number of
/// other application-specific requirements.
/// 
/// Another reason for a custom distance function is if your keys are not human-readable strings, in which
/// case you may need a different interpretation of variances between keys.  For example DNA snippets could
/// be used as keys to search for mutations.
/// 
/// Any distance function you choose must be compatible with SymSpell's delete-distance optimization.  In other
/// words, you must be able to delete no more than [config.max_deletes] characters from both a given record's
/// key and the lookup key and arrive at identical key-variants.  If your distance function is incompatible
/// with this property then the SymSpell optimization won't work for you and you should use a different fuzzy
/// lookup technique and a different crate.  Here is more information on the [SymSpell algorithm](https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f).
/// 
/// Distance functions may return any scalar type, so floating point distances will work.  However, the
/// [config.max_deletes] constant is an integer.  Records that can't be reached by deleting `config.max_deletes` characters
/// from both the record key and the lookup key will never be evaluated by the distance function and are
/// conceptually "too far away".  Once the distance function has been evaluated, its return value is
/// considered the authoritative distance and the delete distance is irrelevant.
/// 
#[derive(Clone)]
pub struct TableConfig<KeyCharT, DistanceT, ValueT, const UTF8_KEYS : bool> {

    /// The number of deletes to store in the database for variants created
    /// by the SymSpell optimization.  If `max_deletes` is too small, the variant will not be found
    /// and therefore the `distance_function` will not have an opportunity to evaluate the match.  However,
    /// if `max_deletes` is too large, it will hurt performance by evaluating too many candidates.
    /// 
    /// Empirically, values near 2 seem to be good in most situations I have found.  I.e. 1 and 3 might be
    /// appropriate sometimes.  4 ends up exploding in most cases I've seen so the SymSpell logic may not
    /// be a good fit if you need to find keys 4 edits away.  0 edits is an exact match.
    pub max_deletes : usize,

    /// This controls an optimization where only a subset of the key is used for creating
    /// variants.  For example, if `meaningful_key_len = 10` then only the first 10 characters of the key will be used
    /// to generate and search for variants.
    /// 
    /// This optimization is predicated on the idea that long key strings will not be very similar to each
    /// other, and a certain number of characters is sufficient to substantially narrow down the search.
    /// 
    /// For example the key *incomprehensibilities* will cause variants to be generated for *incomprehe*
    /// with a `meaningful_key_len` of 10, meaning that a search for *incomprehension* would find *incomprehensibilities*
    /// and evauate it with the `distance_function` even though it is further than [config.max_deletes].
    /// 
    /// In a dataset where many keys share a common prefix, or where keys are organized into a namespace by
    /// concatenating strings, this optimization will cause problems and you should either pass a high number
    /// to effectively disable it, or rework this code to use different logic to select the substring
    /// 
    /// Lookup functions that invoke the distance function will always use the entire key, regardless of the
    /// value of `meaningful_key_len`.
    pub meaningful_key_len : usize,

    /// The number of variants a key must share with a keys in an existing key group in order for that key to
    /// be added to the group rather than being placed into a new separate key group.
    /// 
    /// NOTE: We arrived at the default value (5) empirically by testing a number of different values and
    /// observing the effects on lookup speed, DB construction speed, and DB size.  The observed data
    /// points are checked in, in the file: `misc/perf_data.txt`
    pub group_variant_overlap_threshold : usize,
    pub distance_function : fn(key_a : &[KeyCharT], key_b : &[KeyCharT]) -> DistanceT,
    phantom_key: PhantomData<KeyCharT>,
    phantom_distance: PhantomData<DistanceT>,
    phantom_value: PhantomData<ValueT>,
}

impl <KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned, DistanceT : 'static + Copy + Zero + PartialOrd + PartialEq + From<u8>, ValueT, const UTF8_KEYS : bool>TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS> {

    pub fn default() -> Self {
        Self {
            max_deletes : 2,
            meaningful_key_len : 12,
            group_variant_overlap_threshold : 5,
            distance_function : Self::edit_distance,
            phantom_key : PhantomData,
            phantom_distance : PhantomData,
            phantom_value : PhantomData,        
        }
    }

    /// An implementation of the basic Levenstein distance function, which is used by the [DEFAULT_UTF8_TABLE]
    /// [TableConfig], and may be used anywhere a distance function is required.
    /// 
    /// This implementation uses the Wagner-Fischer Algorithm, as it's described [here](https://en.wikipedia.org/wiki/Levenshtein_distance)
    pub fn edit_distance(key_a : &[KeyCharT], key_b : &[KeyCharT]) -> DistanceT {

        let m = key_a.len()+1;
        let n = key_b.len()+1;

        //Allocate a 2-dimensional vec for the distances between the first i characters of key_a
        //and the first j characters of key_b
        let mut d : [[u8; MAX_KEY_LENGTH + 1]; MAX_KEY_LENGTH + 1] = unsafe { MaybeUninit::uninit().assume_init() };

        //NOTE: I personally find this (below) more readable, but clippy really like the other style.  -\_(..)_/-
        // for i in 1..m {
        //     d[i][0] = i;
        // }
        for (i, row) in d.iter_mut().enumerate().skip(1) {
            //row[0] = i as u8;
            let element = unsafe{ row.get_unchecked_mut(0) };
            *element = i as u8;
        }

        // for j in 1..n {
        //     d[0][j] = j as u8;
        // }
        for (j, element) in d[0].iter_mut().enumerate() {
            *element = j as u8;
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
                let deletion_distance = unsafe {d.get_unchecked(i-1).get_unchecked(j)} + 1;
                //let insertion_distance = d[i][j-1] + 1;
                let insertion_distance = unsafe {d.get_unchecked(i).get_unchecked(j-1)} + 1;
                //let substitution_distance = d[i-1][j-1] + substitution_cost;
                let substitution_distance = unsafe {d.get_unchecked(i-1).get_unchecked(j-1)} + substitution_cost;

                let smallest_distance = min(min(deletion_distance, insertion_distance), substitution_distance);
                
                //d[i][j] = smallest_distance;  
                let element = unsafe{ d.get_unchecked_mut(i).get_unchecked_mut(j) };
                *element = smallest_distance;
            }
        }

        DistanceT::from(d[m-1][n-1])
    }
}

pub const DEFAULT_UTF8_TABLE : TableConfig<char, u8, String, true> = TableConfig {
    max_deletes : 2,
    meaningful_key_len : 12,
    group_variant_overlap_threshold : 5,
    distance_function : TableConfig::<char, u8, String, true>::edit_distance,
    phantom_key : PhantomData,
    phantom_distance : PhantomData,
    phantom_value : PhantomData,
};
