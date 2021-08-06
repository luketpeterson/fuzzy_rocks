//!
//! The KeyGroups module contains all of the logic for working with key groups.  Nothing
//! from here should be re-exported
//! 

use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

use super::database::{*};
use super::records::{*};

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
//GOAT, get rid of these field pubs when I move the key_groups snip
#[derive(Clone)]
pub struct KeyGroups<OwnedKeyT> {
    pub variant_reverse_lookup_map : HashMap<Vec<u8>, usize>,
    pub key_group_variants : Vec<HashSet<Vec<u8>>>,
    pub key_group_keys : Vec<HashSet<OwnedKeyT>>,
    pub group_ids : Vec<usize> //The contents of this vec correspond to the KeyGroupID
}

impl <OwnedKeyT>KeyGroups<OwnedKeyT> {
    //GOAT, make private after snip
    pub fn new() -> Self {
        Self{
            variant_reverse_lookup_map : HashMap::new(),
            key_group_variants : vec![],
            key_group_keys : vec![],
            group_ids : vec![]
        }
    }
    //GOAT, make private after snip
    pub fn next_available_group_id(&self) -> usize {
        //It doesn't matter if we leave some holes, but we must not collide, therefore we'll
        //start at the length of the vec, and search forward from there
        let mut group_id = self.group_ids.len();
        while self.group_ids.contains(&group_id) {
            group_id += 1;
        }
        group_id
    }
}

