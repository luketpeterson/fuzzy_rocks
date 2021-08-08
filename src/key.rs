//!
//! The Key module contains all of the implementation for the abstract Key trait and all
//! helper traits associated with it. Only the [Key] trait should be publicly re-exported.
//! 

use std::{slice};
use core::hash::Hash;
use std::mem::{forget, size_of, transmute};

use serde::{Serialize};

use super::unicode_string_helpers::{*};

/// A private trait representing the subset of key types that are owned and therefore 'static
pub trait OwnedKey<KeyCharT> : 'static + Sized + Serialize + serde::de::DeserializeOwned + Key<KeyCharT> {
    fn as_string(&self) -> Option<String>;
    fn borrow_str(&self) -> Option<&str>;
    fn as_vec(&self) -> Option<Vec<KeyCharT>>;
    fn into_vec(self) -> Vec<KeyCharT>;
    /// WARNING: will stomp memory if the allocated buf is smaller than self.num_chars
    fn move_into_buf<'a>(&'a self, buf : &'a mut Vec<KeyCharT>) -> &'a Vec<KeyCharT>; //NOTE: These lifetimes are like this because this method May copy the data into the supplied buffer, or just return self 
    fn borrow_vec(&self) -> Option<&[KeyCharT]>;

    fn from_key<K : Key<KeyCharT>>(k : &K) -> Self;
    fn from_string(s : String) -> Self;
    fn from_vec(v : Vec<KeyCharT>) -> Self;
}

impl OwnedKey<char> for String {
    fn as_string(&self) -> Option<String> {
        Some(self.clone())
    }
    fn borrow_str(&self) -> Option<&str> {
        Some(self)
    }
    fn as_vec(&self) -> Option<Vec<char>> {
        Some(self.chars().collect())
    }
    fn into_vec(self) -> Vec<char> {
        //NOTE: 15% of the performance on the fuzzy lookups was taken with this function before I
        // started optimizing and utimately switched over to owned_key_into_buf. It appeared that
        // the buffer was being reallocated for each additional char for the collect() implementation.

        //NOTE: Old implementation
        // key.chars().collect()

        //NOTE: It appears that this implementation is twice as fast as the collect() implementation,
        // but we're still losing 7% of overall perf allocating and freeing this Vec, so I'm switching
        // to owned_key_into_buf().
        let num_chars = self.num_chars();
        let mut result_vec = Vec::with_capacity(num_chars);
        for the_char in self.chars() {
            result_vec.push(the_char);
        }
        result_vec
    }
    fn move_into_buf<'a>(&'a self, buf : &'a mut Vec<char>) -> &'a Vec<char> {
        let mut num_chars = 0;
        for (i, the_char) in self.chars().enumerate() {
            let element = unsafe{ buf.get_unchecked_mut(i) };
            *element = the_char;
            num_chars = i;
        }
        unsafe{ buf.set_len(num_chars+1) };

        buf
    }
    fn borrow_vec(&self) -> Option<&[char]> {
        None
    }
    fn from_key<K : Key<char>>(k : &K) -> Self {
        k.borrow_key_str().unwrap().to_string() //NOTE: the unwrap() will panic if called with the wrong kind of key
    }
    fn from_string(s : String) -> Self {
        s
    }
    fn from_vec(_v : Vec<char>) -> Self {
        panic!() //NOTE: Should never be called when the OwnedKeyT isn't a Vec
        //NOTE: This could be made to work if there was a good reason for it, but if it's called
        // it's an indication we might not be on the code path we intended.
    }
}

impl <KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned>OwnedKey<KeyCharT> for Vec<KeyCharT> {
    fn as_string(&self) -> Option<String> {
        None
    }
    fn borrow_str(&self) -> Option<&str> {
        None
    }
    fn as_vec(&self) -> Option<Vec<KeyCharT>> {
        Some(self.clone())
    }
    fn into_vec(self) -> Vec<KeyCharT> {
        self
    }
    fn move_into_buf<'a>(&'a self, _buf : &'a mut Vec<KeyCharT>) -> &'a Vec<KeyCharT> {
        self
    }
    fn borrow_vec(&self) -> Option<&[KeyCharT]> {
        Some(&self[..])
    }
    fn from_key<K : Key<KeyCharT>>(k : &K) -> Self {
        k.get_key_chars()
    }
    fn from_string(_s : String) -> Self {
        panic!() //NOTE: Should never be called when the OwnedKeyT isn't a String
    }
    fn from_vec(v : Vec<KeyCharT>) -> Self {
        v
    }
}

/// Implemented by all types that can be used as keys, whether they are UTF-8 encoded
/// strings or arrays of KeyCharT
pub trait Key<KeyCharT> : KeyUnsafe<KeyCharT> {

    fn num_chars(&self) -> usize;
    fn as_bytes(&self) -> &[u8];
    fn into_bytes(self) -> Vec<u8>;
    fn borrow_key_chars(&self) -> Option<&[KeyCharT]>;
    fn get_key_chars(&self) -> Vec<KeyCharT>;
    fn borrow_key_str(&self) -> Option<&str>;
}

/// The private unsafe accessors for the Key trait
pub trait KeyUnsafe<KeyCharT> : Eq + Hash + Clone + Serialize {
    /// This function may return a result that borrows the owned_key parameter, but the
    /// returned result may have a longer lifetime on account of the type it's called with
    unsafe fn from_owned_unsafe<'b, OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &'b OwnedKeyT) -> Self;
}

impl <KeyCharT>Key<KeyCharT> for &[KeyCharT]
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned,
{
    fn num_chars(&self) -> usize {
        self.len()
    }

    fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        unsafe { slice::from_raw_parts(self.as_ptr() as *const u8, size_of::<KeyCharT>() * len) }
    }

    fn into_bytes(self) -> Vec<u8> {

        let mut owned_vec = self.to_vec();
        let len = owned_vec.len();
        let cap = owned_vec.capacity();

        //Now transmute the vec into a vec of bytes
        let result = unsafe { Vec::<u8>::from_raw_parts(owned_vec.as_mut_ptr() as *mut u8, size_of::<KeyCharT>() * len, size_of::<KeyCharT>() * cap) };
        forget(owned_vec); //So we don't get a double-free
        result
    }

    fn borrow_key_chars(&self) -> Option<&[KeyCharT]> {
        Some(self)
    }

    fn get_key_chars(&self) -> Vec<KeyCharT> {
        self.to_vec()
    }

    fn borrow_key_str(&self) -> Option<&str> {
        None
    }
}

//GOATGOAT, Get rid of this KeyUnsafe trait, because I think we can use the 2e7 pattern
impl <'a, KeyCharT>KeyUnsafe<KeyCharT> for &'a [KeyCharT]
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned
{
    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &OwnedKeyT) -> Self {
        let result = owned_key.borrow_vec().unwrap();
        transmute::<&[KeyCharT], &'a [KeyCharT]>(result)
    }
}

impl <KeyCharT>Key<KeyCharT> for Vec<KeyCharT>
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned,
{
    fn num_chars(&self) -> usize {
        self.len()
    }

    fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        unsafe { slice::from_raw_parts(self.as_ptr() as *const u8, size_of::<KeyCharT>() * len) }
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut mut_self = self;
        let len = mut_self.len();
        let cap = mut_self.capacity();

        let result = unsafe { Vec::<u8>::from_raw_parts(mut_self.as_mut_ptr() as *mut u8, size_of::<KeyCharT>() * len, size_of::<KeyCharT>() * cap) };
        forget(mut_self); //So we don't get a double-free

        result
    }

    fn borrow_key_chars(&self) -> Option<&[KeyCharT]> {
        Some(&self[..])
    }

    fn get_key_chars(&self) -> Vec<KeyCharT> {
        self.clone()
    }

    fn borrow_key_str(&self) -> Option<&str> {
        None
    }
}

impl <KeyCharT>KeyUnsafe<KeyCharT> for Vec<KeyCharT>
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned,
{
    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &OwnedKeyT) -> Self {
        //This implementation is actually safe, but the fn prototype is unsafe
        owned_key.as_vec().unwrap()
    }
}

impl Key<char> for &str
{
    fn num_chars(&self) -> usize {
        unicode_len(self)
    }

    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }

    fn into_bytes(self) -> Vec<u8> {
        let bytes_slice = str::as_bytes(self);
        bytes_slice.to_vec()
    }

    fn borrow_key_chars(&self) -> Option<&[char]> {
        None
    }

    fn get_key_chars(&self) -> Vec<char> {
        self.chars().collect()
    }

    fn borrow_key_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl <'a>KeyUnsafe<char> for &'a str {

    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey<char>>(owned_key : &OwnedKeyT) -> Self {
        let result = owned_key.borrow_str().unwrap();
        transmute::<&str, &'a str>(result)
    }
}

impl Key<char> for String
{
    fn num_chars(&self) -> usize {
        unicode_len(self)
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }

    fn borrow_key_chars(&self) -> Option<&[char]> {
        None
    }

    fn get_key_chars(&self) -> Vec<char> {
        self.chars().collect()
    }

    fn borrow_key_str(&self) -> Option<&str> {
        Some(&self)
    }
}

impl KeyUnsafe<char> for String {

    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey<char>>(owned_key : &OwnedKeyT) -> Self {
        //This implementation is actually safe, but the fn prototype is unsafe
        owned_key.as_string().unwrap()
    }
}
