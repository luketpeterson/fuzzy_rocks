//!
//! The Records module contains the logic for working with records.  RecordIDs are re-exported
//! to the public interface.
//! 

use serde::{Serialize, Deserialize};


/// A unique identifier for a record within a [Table]
/// 
/// NOTE: although the RecordID is 64 bits, only 44 bits can be used to address
/// records, giving a theoretical maximum of 17.6 trillion records although
/// this crate has only been tested with about 10 million unique records, so far
#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, derive_more::Display, Serialize, Deserialize)]
pub struct RecordID(pub usize);
impl RecordID {
    pub const NULL : RecordID = RecordID(usize::MAX);
}

impl RecordID {
    pub fn from(id : usize) -> Self {
        RecordID(id)
    }
    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

/// Some meta-data associated with each record.  There is one of these for each record, and it
/// points to each KeyGroup that might have keys associated with the record
#[derive(Serialize, Deserialize)]
pub struct RecordData {
    pub key_groups : Vec<usize>,
    //DANGER: If any additional fields are added here, we must update `put_record_key_groups` to preserve
    // other fields before just overwriting it.
}

impl RecordData {
    pub fn new(key_groups : &[usize]) -> Self {
        Self{
            key_groups : key_groups.to_vec()
        }
    }
}