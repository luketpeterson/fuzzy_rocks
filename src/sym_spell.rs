//!
//! The SymSpell module contains the logic for computing and searching for key variants
//! 

use core::marker::PhantomData;
use std::collections::{HashSet};

use super::unicode_string_helpers::{*};
use super::key::{*};
use super::table_config::{*};

pub struct SymSpell<OwnedKeyT, const UTF8_KEYS : bool> {
    phantom_key: PhantomData<OwnedKeyT>
}

impl <OwnedKeyT, const UTF8_KEYS : bool>SymSpell<OwnedKeyT, UTF8_KEYS> {

    /// Returns all of the variants of a key, for querying or adding to the variants database
    pub fn variants<KeyCharT : Clone, DistanceT, ValueT, K : Key<KeyCharT>>(key: &K, config : &TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS>) -> HashSet<Vec<u8>>
        where OwnedKeyT : OwnedKey<KeyCharT>
    {

        let mut variants_set : HashSet<Vec<u8>> = HashSet::new();
        
        //We shouldn't make any variants for empty keys
        if key.num_chars() > 0 {

            //We'll only build variants from the meaningful portion of the key
            let meaningful_key = Self::meaningful_key_substring(key, config);

            if 0 < config.max_deletes {
                Self::variants_recursive(&meaningful_key, 0, &mut variants_set, config);
            }
            variants_set.insert(meaningful_key.into_bytes());    
        }

        variants_set
    }

    // The recursive part of the variants() function
    pub fn variants_recursive<KeyCharT, DistanceT, ValueT, K : Key<KeyCharT>>(key: &K, edit_distance: usize, variants_set: &mut HashSet<Vec<u8>>, config : &TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS>)
        where OwnedKeyT : OwnedKey<KeyCharT>
    {

        let edit_distance = edit_distance + 1;

        let key_len = key.num_chars();

        if key_len > 1 {
            for i in 0..key_len {
                let variant = Self::remove_char_from_key(key, i);

                if !variants_set.contains(variant.as_bytes()) {

                    if edit_distance < config.max_deletes {
                        Self::variants_recursive(&variant, edit_distance, variants_set, config);
                    }

                    variants_set.insert(variant.into_bytes());
                }
            }
        }
    }

    // Returns the "meaningful" part of a key, that is used as the starting point to generate the variants
    pub fn meaningful_key_substring<KeyCharT : Clone, DistanceT, ValueT, K : Key<KeyCharT>>(key: &K, config : &TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS>) -> OwnedKeyT
        where OwnedKeyT : OwnedKey<KeyCharT>
    {
        if UTF8_KEYS {
            let result_string = unicode_truncate(key.borrow_key_str().unwrap(), config.meaningful_key_len);
            OwnedKeyT::from_string(result_string)
        } else {
            let result_vec = if key.num_chars() > config.meaningful_key_len {
                let (prefix, _remainder) = key.borrow_key_chars().unwrap().split_at(config.meaningful_key_len);
                prefix.to_vec()
            } else {
                key.get_key_chars()
            };
            OwnedKeyT::from_vec(result_vec)
        }
    }

    // Returns a new owned key, that is a variant of the supplied key, without the character at the
    // specified index
    pub fn remove_char_from_key<KeyCharT, K : Key<KeyCharT>>(key: &K, idx : usize) -> OwnedKeyT
        where OwnedKeyT : OwnedKey<KeyCharT>
    {
        if UTF8_KEYS {
            let result_string = unicode_remove_char(key.borrow_key_str().unwrap(), idx);
            OwnedKeyT::from_string(result_string)
        } else {
            let mut result_vec = key.get_key_chars();
            result_vec.remove(idx);
            OwnedKeyT::from_vec(result_vec)
        }
    }

}
