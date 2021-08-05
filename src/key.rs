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
    fn borrow_vec(&self) -> Option<&[KeyCharT]>;
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
    fn borrow_vec(&self) -> Option<&[char]> {
        None
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
    fn borrow_vec(&self) -> Option<&[KeyCharT]> {
        Some(&self[..])
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
