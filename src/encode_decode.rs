//!
//! Contains wrappers around the logic to encode and decode data into bytes, abstracting away the format.
//!

/// Wraps an interface to an encode / decode format
///
/// NOTE: It's unlikely you will want to implement this trait.  Instead use one of the existing
/// implementations: [BincodeCoder] and [MessagePackCoder]
pub trait Coder: Clone + Send + Sync + 'static {
    type FixintListIter<'a, I: Copy + Sized + 'static>: Iterator<Item=&'a [u8]>;

    /// Create a new coder
    fn new() -> Self;

    /// Encodes an arbitrary structure to bytes using a variable integer size encoding
    fn encode_varint_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String>;

    /// Decodes an arbitrary structure from bytes using a variable integer size encoding
    fn decode_varint_owned_from_bytes<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String>;

    /// Encodes a list of items to a buffer using a variable integer size encoding
    ///
    /// NOTE: it is OK to decode the returned buffer using [decode_owned_from_bytes]
    /// WARNING: This method requires the Serializer for T to encode the length as the first thing in
    /// the serialized output
    fn encode_varint_list_to_buf<T: serde::ser::Serialize + IntoIterator>(&self, list: &T) -> Result<Vec<u8>, String>;

    /// Returns the number of elements in an encoded varint list without decoding them
    ///
    /// WARNING: This method assumes the bytes were encoded with [encode_varint_list_to_buf]
    fn varint_list_len(&self, bytes: &[u8]) -> Result<usize, String>;

    /// Encodes an arbitrary structure to bytes using a fixed integer size encoding
    fn encode_fixint_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String>;

    /// Decodes an arbitrary structure from bytes using a variable integer size encoding
    fn decode_fixint_owned_from_bytes<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String>;

    /// Encodes a list of items to a buffer using a fixed integer size encoding
    ///
    /// WARNING: a list can't be encoded with this method and then decoded using [decode_owned_from_bytes]
    /// because the encoding format may be different.  Also, list elements **MUST** be a fixed size when encoded
    fn encode_fixint_list_to_buf<T: serde::ser::Serialize + IntoIterator<Item=I>, I: Sized + Copy>(&self, list: &T) -> Result<Vec<u8>, String>;

    /// Returns the number of elements in an encoded fixint list without decoding them
    ///
    /// WARNING: This method assumes the bytes were encoded with [encode_fixint_list_to_buf]
    fn fixint_list_len(&self, bytes: &[u8]) -> Result<usize, String>;

    /// Returns an iterator over the sub-slices within `bytes` that correspond to each item in the list
    ///
    /// WARNING: This method assumes the bytes were encoded with [encode_fixint_list_to_buf]
    fn fixint_list_iter<'a, I: Copy + Sized + 'static>(&self, bytes: &'a [u8]) -> Result<Self::FixintListIter<'a, I>, String>;

}

//TODO: Consider removing fixint stuff from the interface altogether, as well as the fixint_list_iter
// since it isn't very heavily used

#[cfg(feature = "bincode")]
pub(crate) mod bincode_interface {
    use super::*;
    use bincode::Options;
    use bincode::config::*;
    use crate::bincode_helpers::*;

    #[derive(Clone)]
    pub struct BincodeCoder {
        varint_coder: WithOtherEndian<WithOtherIntEncoding<DefaultOptions, VarintEncoding>, LittleEndian>,
        fixint_coder: WithOtherEndian<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, LittleEndian>,
    }

    impl Coder for BincodeCoder {
        type FixintListIter<'a, I: Copy + Sized + 'static> = BinCodeVecIterator<'a, I>;

        fn new() -> Self {
            Self {
                varint_coder: bincode::DefaultOptions::new().with_varint_encoding().with_little_endian(),
                fixint_coder: bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian(),
            }
        }
        fn encode_varint_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            self.varint_coder.serialize(obj).map_err(|e| format!("Encode error: {e}"))
        }
        fn decode_varint_owned_from_bytes<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
            self.varint_coder.deserialize(&bytes).map_err(|e| format!("Decode error: {e}"))
        }
        fn encode_varint_list_to_buf<T: serde::ser::Serialize + IntoIterator>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.encode_varint_to_buf(list)
        }
        fn varint_list_len(&self, bytes: &[u8]) -> Result<usize, String> {

            //The vector element count should be the first encoded usize
            let mut skip_bytes = 0;
            let keys_count = bincode_u64_le_varint(bytes, &mut skip_bytes);
            Ok(keys_count as usize)
        }
        fn encode_fixint_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            self.fixint_coder.serialize(obj).map_err(|e| format!("Encode error: {e}"))
        }
        fn decode_fixint_owned_from_bytes<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
            self.fixint_coder.deserialize(&bytes).map_err(|e| format!("Decode error: {e}"))
        }
        fn encode_fixint_list_to_buf<T: serde::ser::Serialize + IntoIterator<Item=I>, I: Sized + Copy>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.fixint_coder.serialize(list).map_err(|e| format!("Encode error: {e}"))
        }
        fn fixint_list_len(&self, bytes: &[u8]) -> Result<usize, String> {
            Ok(bincode_vec_fixint_len(bytes))
        }
        fn fixint_list_iter<'a, I: Copy + Sized + 'static>(&self, bytes: &'a [u8]) -> Result<Self::FixintListIter<'a, I>, String> {
            Ok(bincode_vec_iter::<I>(bytes))
        }
    }

}
