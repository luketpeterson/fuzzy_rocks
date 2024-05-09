//!
//! Contains wrappers around the logic to encode and decode data into bytes, abstracting away the format.
//!

/// Wraps an interface to an encode / decode format
///
/// NOTE: It's unlikely you will want to implement this trait.  Instead use one of the existing
/// implementations: [BincodeCoder](crate::BincodeCoder) and [MsgPackCoder](crate::MsgPackCoder),
/// or use [DefaultCoder](crate::DefaultCoder)
///
/// NOTE: `fmt1` and `fmt2` allow the a coder to use two different formats.  Originally this was
/// done for BinCode's `varint` and `fixint` formats, where `fmt1` was the more compact, while
/// `fmt2` is the more regular and faster to parse.
pub trait Coder: Clone + Send + Sync + 'static {

    /// Create a new coder
    fn new() -> Self;

    /// The name of the format, to check for compatibility
    fn format_name(&self) -> &'static str;

    /// Encodes an arbitrary structure to bytes using fmt1 encoding
    fn encode_fmt1_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String>;

    /// Decodes an arbitrary structure from bytes using a fmt1 encoding
    fn decode_fmt1_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String>;

    /// Encodes a list of items to a buffer using a fmt1 encoding
    ///
    /// NOTE: it is OK to decode the returned buffer using [decode_fmt1_from_bytes](Self::decode_fmt1_from_bytes)
    /// WARNING: This method requires the Serializer for T to encode the length as the first thing in
    /// the serialized output
    fn encode_fmt1_list_to_buf<T: serde::ser::Serialize + IntoIterator>(&self, list: &T) -> Result<Vec<u8>, String>;

    /// Returns the number of elements in an encoded fmt1 list without decoding them
    ///
    /// WARNING: This method assumes the bytes were encoded with [encode_fmt1_list_to_buf](Self::encode_fmt1_list_to_buf)
    fn fmt1_list_len(&self, bytes: &[u8]) -> Result<usize, String>;

    /// Encodes an arbitrary structure to bytes using fmt2 encoding
    fn encode_fmt2_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String>;

    /// Decodes an arbitrary structure from bytes using fmt2 encoding
    fn decode_fmt2_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String>;

    /// Encodes a list of items to a buffer using fmt2 encoding
    ///
    /// WARNING: a list can't be encoded with this method and then decoded using [decode_fmt1_from_bytes](Self::decode_fmt1_from_bytes)
    /// because the encoding format may be different, but it's ok to use [decode_fmt2_from_bytes](Self::decode_fmt2_from_bytes)
    fn encode_fmt2_list_to_buf<T: serde::ser::Serialize + IntoIterator<Item=I>, I: Sized + Copy>(&self, list: &T) -> Result<Vec<u8>, String>;

    /// Returns the number of elements in an encoded fmt2 list without decoding them
    ///
    /// WARNING: This method assumes the bytes were encoded with [encode_fmt2_list_to_buf](Self::encode_fmt2_list_to_buf)
    fn fmt2_list_len(&self, bytes: &[u8]) -> Result<usize, String>;

}

#[cfg(feature = "bitcode")]
macro_rules! internal_coder {
    () => { crate::BitcodeCoder };
}

#[cfg(all(feature = "bincode", not(feature = "bitcode")))]
pub(crate) static INTERNAL_CODER: std::sync::OnceLock<crate::BincodeCoder> = std::sync::OnceLock::new();

#[cfg(all(feature = "bincode", not(feature = "bitcode")))]
macro_rules! internal_coder {
    () => { crate::encode_decode::INTERNAL_CODER.get_or_init(|| crate::BincodeCoder::new()) };
}

#[cfg(all(feature = "msgpack", not(feature = "bitcode"), not(feature = "bincode")))]
macro_rules! internal_coder {
    () => { crate::MsgPackCoder };
}

pub(crate) use internal_coder;

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

        fn new() -> Self {
            Self {
                varint_coder: bincode::DefaultOptions::new().with_varint_encoding().with_little_endian(),
                fixint_coder: bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian(),
            }
        }
        fn format_name(&self) -> &'static str {
            "bincode"
        }
        fn encode_fmt1_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            self.varint_coder.serialize(obj).map_err(|e| format!("Encode error: {e}"))
        }
        fn decode_fmt1_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String> {
            self.varint_coder.deserialize(bytes).map_err(|e| format!("Decode error: {e}"))
        }
        fn encode_fmt1_list_to_buf<T: serde::ser::Serialize + IntoIterator>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_to_buf(list)
        }
        fn fmt1_list_len(&self, bytes: &[u8]) -> Result<usize, String> {

            //The vector element count should be the first encoded usize
            let mut skip_bytes = 0;
            let element_cnt = bincode_u64_le_varint(bytes, &mut skip_bytes);
            Ok(element_cnt as usize)
        }
        fn encode_fmt2_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            self.fixint_coder.serialize(obj).map_err(|e| format!("Encode error: {e}"))
        }
        fn decode_fmt2_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String> {
            self.fixint_coder.deserialize(bytes).map_err(|e| format!("Decode error: {e}"))
        }
        fn encode_fmt2_list_to_buf<T: serde::ser::Serialize + IntoIterator<Item=I>, I: Sized + Copy>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.fixint_coder.serialize(list).map_err(|e| format!("Encode error: {e}"))
        }
        fn fmt2_list_len(&self, bytes: &[u8]) -> Result<usize, String> {
            Ok(bincode_vec_fixint_len(bytes))
        }
    }
}

#[cfg(feature = "msgpack")]
pub(crate) mod msgpack_interface {
    use super::*;

    #[derive(Clone)]
    pub struct MsgPackCoder;

    impl Coder for MsgPackCoder {
        fn new() -> Self {
            Self
        }
        fn format_name(&self) -> &'static str {
            "msgpack"
        }
        fn encode_fmt1_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            rmp_serde::encode::to_vec(obj).map_err(|e| format!("Encode error: {e}"))
        }
        fn decode_fmt1_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String> {
            rmp_serde::decode::from_slice(bytes).map_err(|e| format!("Decode error: {e}"))
        }
        fn encode_fmt1_list_to_buf<T: serde::ser::Serialize + IntoIterator>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_to_buf(list)
        }
        fn fmt1_list_len(&self, bytes: &[u8]) -> Result<usize, String> {
            let element_cnt = rmp::decode::read_array_len(&mut &bytes[..]).map_err(|e| format!("Decode error: {e}"))?;
            Ok(element_cnt as usize)
        }
        fn encode_fmt2_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_to_buf(obj)
        }
        fn decode_fmt2_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String> {
            self.decode_fmt1_from_bytes(bytes)
        }
        fn encode_fmt2_list_to_buf<T: serde::ser::Serialize + IntoIterator<Item=I>, I: Sized + Copy>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_list_to_buf(list)
        }
        fn fmt2_list_len(&self, bytes: &[u8]) -> Result<usize, String> {
            self.fmt1_list_len(bytes)
        }
    }
}

#[cfg(feature = "bitcode")]
pub(crate) mod bitcode_interface {
    use super::*;

    #[derive(Debug, Default, Clone, Copy)]
    pub struct BitcodeCoder;

    impl Coder for BitcodeCoder {
        fn new() -> Self {
            Self
        }
        fn format_name(&self) -> &'static str {
            "bitcode v.0.6.0"
        }
        fn encode_fmt1_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            bitcode::serialize(obj).map_err(|e| format!("Encode error: {e}"))
        }
        fn decode_fmt1_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String> {
            bitcode::deserialize(bytes).map_err(|e| format!("Decode error: {e}"))
        }
        fn encode_fmt1_list_to_buf<T: serde::ser::Serialize + IntoIterator>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_to_buf(list)
        }
        fn fmt1_list_len(&self, bytes: &[u8]) -> Result<usize, String> {
            if bytes[0] == 255 && bytes[1..].len() > 255 {
              match bytes[1] {
                4 => {
                  let len_chars = &bytes[2..=3];
                  let value = u16::from_le_bytes(
                    len_chars
                      .try_into()
                      .map_err(|e| format!("Failed converting size bytes to u16: {e}"))?
                  );
                  Ok(value as usize)
                }
                2 => {
                  let len_chars = &bytes[2..=5];
                  let value = u32::from_le_bytes(
                    len_chars
                      .try_into()
                      .map_err(|e| format!("Failed converting size bytes to u32: {e}"))?
                  );
                  Ok(value as usize)
                }
                1 => {
                  let len_chars = &bytes[2..=10];
                  let value = u64::from_le_bytes(
                    len_chars
                      .try_into()
                      .map_err(|e| format!("Failed converting size bytes to u64: {e}"))?
                  );
                  Ok(value as usize)
                }
                0 => {
                  let len_chars = &bytes[2..=17];
                  let value = u128::from_le_bytes(
                    len_chars
                      .try_into()
                      .map_err(|e| format!("Failed converting size bytes to u128: {e}"))?
                  );
                  Ok(value as usize)
                }
                _ => Ok(bytes[0] as usize)
              }
            } else {
              Ok(bytes[0] as usize)
            }
        }
        fn encode_fmt2_to_buf<T: serde::ser::Serialize>(&self, obj: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_to_buf(obj)
        }
        fn decode_fmt2_from_bytes<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, String> {
            self.decode_fmt1_from_bytes(bytes)
        }
        fn encode_fmt2_list_to_buf<T: serde::ser::Serialize + IntoIterator<Item=I>, I: Sized + Copy>(&self, list: &T) -> Result<Vec<u8>, String> {
            self.encode_fmt1_list_to_buf(list)
        }
        fn fmt2_list_len(&self, bytes: &[u8]) -> Result<usize, String> {
            self.fmt1_list_len(bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Coder;

  #[test]
  fn test_encode_len() {
    let coder = crate::DefaultCoder::new();

    let data = coder.encode_fmt1_to_buf::<String>(&"test".to_string()).unwrap();

    let len = coder.fmt1_list_len(&data).unwrap();

    assert_eq!(len , 4);
  }
}