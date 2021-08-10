//!
//! The KeyGroups module contains all of the logic for working with key groups.  Nothing
//! from here should be re-exported
//! 

use core::cmp::{Ordering};

use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

use super::key::{*};
use super::database::{*};
use super::records::{*};
use super::sym_spell::{*};
use super::table_config::{*};

/// A unique identifier for a key group, which includes its RecordID
/// 
/// Lower 44 bits are the RecordID, upper 20 bits are the GroupID
#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, derive_more::Display, Serialize, Deserialize)]
pub struct KeyGroupID(usize);
impl KeyGroupID {
    pub fn from(id : usize) -> Self {
        KeyGroupID(id)
    }
    pub fn from_record_and_idx(record_id : RecordID, group_idx : usize) -> Self {
        //Panic if we have more than the allowed number of records
        if record_id.0 > 0xFFFFFFFFFFF {
            panic!("too many records!");
        }
        let record_component = record_id.0 & 0xFFFFFFFFFFF;
        let group_component = group_idx << 44;
        Self(record_component + group_component)
    }
    pub fn record_id(&self) -> RecordID {
        RecordID::from(self.0 & 0xFFFFFFFFFFF)
    }
    pub fn group_idx(&self) -> usize {
        self.0 >> 44
    }
    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

/// A transient struct to keep track of the multiple key groups, reflecting what's in the DB.
/// Used when assembling key groups or adding new keys to existing groups
#[derive(Clone)]
pub struct KeyGroups<OwnedKeyT, const UTF8_KEYS : bool> {
    variant_reverse_lookup_map : HashMap<Vec<u8>, usize>,
    pub key_group_variants : Vec<HashSet<Vec<u8>>>,
    pub key_group_keys : Vec<HashSet<OwnedKeyT>>,
    pub group_ids : Vec<usize> //The contents of this vec correspond to the KeyGroupID
}

impl <OwnedKeyT, const UTF8_KEYS : bool>KeyGroups<OwnedKeyT, UTF8_KEYS> {
    
    fn new() -> Self {
        Self{
            variant_reverse_lookup_map : HashMap::new(),
            key_group_variants : vec![],
            key_group_keys : vec![],
            group_ids : vec![]
        }
    }

    fn next_available_group_id(&self) -> usize {
        //It doesn't matter if we leave some holes, but we must not collide, therefore we'll
        //start at the length of the vec, and search forward from there
        let mut group_id = self.group_ids.len();
        while self.group_ids.contains(&group_id) {
            group_id += 1;
        }
        group_id
    }

    /// Adds a new key to a KeyGroups transient structure.  Doesn't touch the DB
    /// 
    /// This function is the owner of the decision whether or not to add a key to an existing
    /// group or to create a new group for a key
    pub fn add_key_to_groups<KeyCharT : Clone, DistanceT, ValueT, K>(&mut self, key : &K, update_reverse_map : bool, config : &TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS>) -> Result<(), String>
        where
        OwnedKeyT : OwnedKey<KeyCharT = KeyCharT>,
        K : Key<KeyCharT = KeyCharT>
    {
        
        //Make sure the key is within the maximum allowable MAX_KEY_LENGTH
        if key.num_chars() > MAX_KEY_LENGTH {
            return Err("key length exceeds MAX_KEY_LENGTH".to_string());
        }

        //Compute the variants for the key
        let key_variants = SymSpell::<OwnedKeyT, UTF8_KEYS>::variants(key, config);

        //Variables that determine which group we merge into, or whether we create a new key group
        let mut group_idx; //The index of the key group we'll merge this key into
        let create_new_group;

        //If we already have exactly this key as a variant, then we will add the key to that
        // key group
        if let Some(existing_group) = self.variant_reverse_lookup_map.get(key.as_bytes()) {
            group_idx = *existing_group;
            create_new_group = false;
        } else {

            if config.group_variant_overlap_threshold > 0 {

                //Count the number of overlapping variants the key has with each existing group
                // NOTE: It's possible the variant_reverse_lookup_map doesn't capture all of the
                // different groups containing a given variant.  This could happen if we chose not
                // to merge variants for any reason, like exceeding a max number of keys in a key group,
                // It could also happen if a variant set ends up overlapping two previously disjoint
                // variant sets.  The only way to avoid that would be to merge the two existing key
                // groups into a single key group, but we don't have logic to merge existing key groups,
                // only to append new keys and the key's variants to a group.
                // Since the whole key groups logic is just an optimization, this edge case will not
                // affect the correctness of the results.
                let mut overlap_counts : Vec<usize> = vec![0; self.key_group_keys.len()];
                for variant in key_variants.iter() {
                    //See if it's already part of another key's variant list
                    if let Some(existing_group) = self.variant_reverse_lookup_map.get(&variant[..]) {
                        overlap_counts[*existing_group] += 1;
                    }
                }

                let (max_group_idx, max_overlaps) = overlap_counts.into_iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .unwrap_or((0, 0));
                group_idx = max_group_idx;
                create_new_group = max_overlaps < config.group_variant_overlap_threshold; //Unless we have at least group_variant_overlap_threshold variant overlaps we'll make a new key group.

            } else {
                group_idx = 0;
                create_new_group = self.key_group_keys.len() == 0;
            }
        }

        //Make a decision about whether to:
        //A.) Use the key as the start of a new key group, or
        //B.) Combine the key and its variant into an existing group
        if create_new_group {
            //A. We have no overlap with any existing group, so we will create a new group for this key
            group_idx = self.key_group_keys.len();
            let mut new_set = HashSet::with_capacity(1);
            new_set.insert(OwnedKeyT::from_key(key));
            self.key_group_keys.push(new_set);
            self.key_group_variants.push(key_variants.clone());
            //We can't count on the KeyGroupIDs not having holes so we need to use a function to
            //find a unique ID.
            let new_group_id = self.next_available_group_id();
            self.group_ids.push(new_group_id);
        } else {
            //B. We will append the key to the existing group at group_index, and merge the variants
            self.key_group_keys[group_idx].insert(OwnedKeyT::from_key(key));
            self.key_group_variants[group_idx].extend(key_variants.clone());
        }

        //If we're not at the last key in the list, add the variants to the variant_reverse_lookup_map
        if update_reverse_map {
            for variant in key_variants {
                self.variant_reverse_lookup_map.insert(variant, group_idx);
            }
        }

        Ok(())
    }

    /// Divides a list of keys up into one or more key groups based on some criteria; the primary
    /// of which is the overlap between key variants.  Keys with more overlapping variants are more
    /// likely to belong in the same group and keys with fewer or none are less likely.
    pub fn make_groups_from_keys<'a, KeyCharT : Clone, DistanceT, ValueT, K, KeysIterT : Iterator<Item=&'a K>>(keys_iter : KeysIterT, num_keys : usize, config : &TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS>) -> Result<Self, String>
        where
        OwnedKeyT : OwnedKey<KeyCharT = KeyCharT>,
        K : Key<KeyCharT = KeyCharT> + 'a
    {

        //Start with empty key groups, and add the keys one at a time
        let mut groups = KeyGroups::new();
        for (key_idx, key) in keys_iter.enumerate() {
            let update_reverse_map = key_idx < num_keys-1;
            groups.add_key_to_groups(key, update_reverse_map, config)?;
        }

        Ok(groups)
    }

    /// Loads the existing key groups for a record in the [Table]
    /// 
    /// This function is used when adding new keys to a record, and figuring out which groups to
    /// merge the keys into
    pub fn load_key_groups<KeyCharT : Clone, DistanceT, ValueT>(db : &DBConnection, record_id : RecordID, config : &TableConfig<KeyCharT, DistanceT, ValueT, UTF8_KEYS>) -> Result<Self, String> 
        where
        OwnedKeyT : OwnedKey<KeyCharT = KeyCharT>,
    {

        let mut groups = KeyGroups::new();

        //Load the group indices from the rec_data table and loop over each key group
        for (group_idx, key_group) in db.get_record_key_groups(record_id)?.enumerate() {

            let mut group_keys = HashSet::new();
            let mut group_variants = HashSet::new();

            //Load the group's keys and loop over each one
            for key in db.get_keys_in_group::<OwnedKeyT>(key_group)? {

                //Compute the variants for the key, and merge them into the group variants
                let key_variants = SymSpell::<OwnedKeyT, UTF8_KEYS>::variants(&key, config);

                //Update the reverse_lookup_map with every variant
                for variant in key_variants.iter() {
                    groups.variant_reverse_lookup_map.insert(variant.clone(), group_idx);
                }

                //Push this key into the group's key list
                group_keys.insert(key);

                //Merge this key's variants with the other variants in this group
                group_variants.extend(key_variants);
            }

            groups.key_group_variants.push(group_variants);
            groups.key_group_keys.push(group_keys);
            groups.group_ids.push(key_group.group_idx());
        }

        Ok(groups)
    }
}

