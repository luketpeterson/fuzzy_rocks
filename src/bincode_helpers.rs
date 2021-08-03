
use core::marker::PhantomData;
use std::convert::{TryInto};

/// This Iterator object is designed to iterate over the entries in a bincode-encoded Vec<T>
/// without needing to actually deserialize the Vec into a temporary memory object
/// 
/// **NOTE** This type assumes bincode is configured with [FixintEncoding](bincode::config::FixintEncoding),
/// i.e. 64-bit usize types and [LittleEndian](bincode::config::LittleEndian) byte order.  Any other
/// configuration and your data may be corrupt.
/// 
/// TODO: Try using `with_varint_encoding` rather than `with_fixint_encoding`, and measure performance.
/// It's highly likely that the data size reduction completely makes up for the extra work and memcpy incurred
/// deserializing the structure, and it's faster not to mess with trying to read the buffer without fully
/// deserializing it.
pub struct BinCodeVecIterator<'a, T : Sized + Copy> {
    remaining_buf : &'a [u8],
    phantom: PhantomData<&'a T>,
}

/// Returns the length of a Vec<T> that has been encoded with bincode, using
/// [FixintEncoding](bincode::config::FixintEncoding) and
/// [LittleEndian](bincode::config::LittleEndian) byte order.
pub fn bincode_vec_fixint_len(buf : &[u8]) -> usize {

    let (len_chars, _remainder) = buf.split_at(8);
    usize::from_le_bytes(len_chars.try_into().unwrap())
}

/// Returns a [BinCodeVecIterator] to iterate over a Vec<T> that has been encoded with bincode,
/// without requiring an actual [Vec] to be recreated in memory
pub fn bincode_vec_iter<T : Sized + Copy>(buf : &[u8]) -> BinCodeVecIterator<'_, T> {

    //Skip over the length at the beginning (8 bytes = 64bit usize), because we can infer
    // the Vec length from the buffer size
    let (_len_chars, remainder) = buf.split_at(8);

    BinCodeVecIterator{remaining_buf: remainder, phantom : PhantomData}
}

impl <'a, T : Sized + Copy>Iterator for BinCodeVecIterator<'a, T> {
    type Item = &'a [u8];

    //NOTE: type Item = &T; would be better.
    // Ideally we'd decode T inside of next(), but we would need a way to decode it without making
    // a copy because making a copy would defeat the whole purpose of the in-place access

    fn next(&mut self) -> Option<&'a [u8]> {
        let t_size_bytes = ::std::mem::size_of::<T>();
        
        if self.remaining_buf.len() >= t_size_bytes {
            let (t_chars, remainder) = self.remaining_buf.split_at(t_size_bytes);
            self.remaining_buf = remainder;
            Some(t_chars)  
        } else {
            None
        }
    }
}

/// Interprets the bytes at the start of `buf` as an encoded 64-bit unsigned number that has been
/// encoded with bincode, using [VarintEncoding](bincode::config::VarintEncoding) and
/// [LittleEndian](bincode::config::LittleEndian) byte order.
/// 
/// Returns the encoded value, and sets `num_bytes` to the number of bytes in the buffer used to encode
/// the value.
pub fn bincode_u64_le_varint(buf : &[u8], num_bytes : &mut usize) -> u64 {

    match buf[0] {
        251 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(2);
            let value = u16::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 3;
            value as u64
        },
        252 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(4);
            let value = u32::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 5;
            value as u64
        },
        253 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(8);
            let value = u64::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 9;
            value
        },
        254 => {
            let (_junk_char, remainder) = buf.split_at(1);
            let (len_chars, _remainder) = remainder.split_at(16);
            let value = u128::from_le_bytes(len_chars.try_into().unwrap());
            *num_bytes = 17;
            value as u64
        },
        _ => {
            *num_bytes = 1;
            buf[0] as u64
        }
    }
}

//NOTE: Currently Unused
// /// Returns a slice representing the characters of a String that has been encoded with bincode, using
// /// [VarintEncoding](bincode::config::VarintEncoding) and [LittleEndian](bincode::config::LittleEndian) byte order.
// fn bincode_string_varint(buf : &[u8]) -> &[u8] {

//     //Interpret the length
//     let mut skip_bytes = 0;
//     let string_len = bincode_u64_le_varint(buf, &mut skip_bytes);

//     //Split the slice to grab the string
//     let (_len_chars, remainder) = buf.split_at(skip_bytes);
//     let (string_slice, _remainder) = remainder.split_at(string_len as usize);
//     string_slice
// }