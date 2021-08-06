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

    /// An implementation of the basic Levenstein distance function, which may be passed to
    /// [lookup_fuzzy](Table::lookup_fuzzy), [lookup_best](Table::lookup_best), or used anywhere
    /// else a distance function is needed.
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
