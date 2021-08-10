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
pub trait OwnedKey : 'static + Sized + Serialize + serde::de::DeserializeOwned + Key {
    fn as_string(&self) -> Option<String>;
    fn borrow_str(&self) -> Option<&str>;
    fn as_vec(&self) -> Option<Vec<Self::KeyCharT>>;
    fn into_vec(self) -> Vec<Self::KeyCharT>;
    /// WARNING: will stomp memory if the allocated buf is smaller than self.num_chars
    fn move_into_buf<'a>(&'a self, buf : &'a mut Vec<Self::KeyCharT>) -> &'a Vec<Self::KeyCharT>; //NOTE: These lifetimes are like this because this method May copy the data into the supplied buffer, or just return self 
    fn borrow_vec(&self) -> Option<&[Self::KeyCharT]>;

    fn from_key<K : Key + KeyUnsafe<KeyCharT = Self::KeyCharT>>(k : &K) -> Self; //TODO: Get rid of the KeyUnsafe trait when When GenericAssociatedTypes is stabilized
    fn from_string(s : String) -> Self;
    fn from_vec(v : Vec<Self::KeyCharT>) -> Self;
}

impl OwnedKey for String {
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
    fn from_key<K : Key>(k : &K) -> Self {
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

impl <KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned>OwnedKey for Vec<KeyCharT> 
{
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
    fn from_key<K : Key + KeyUnsafe<KeyCharT = KeyCharT>>(k : &K) -> Self { //TODO: Get rid of the KeyUnsafe trait when When GenericAssociatedTypes is stabilized
        k.get_key_chars()
    }
    fn from_string(_s : String) -> Self {
        panic!() //NOTE: Should never be called when the OwnedKeyT isn't a String
    }
    fn from_vec(v : Vec<KeyCharT>) -> Self {
        v
    }
}

/// A convenience trait to automatically convert the passed argument into one of the acceptable [Key] types,
/// if possible
pub trait IntoKey {
    type Key: Key;

    fn into_key(self) -> Self::Key;
}

impl<K> IntoKey for K
    where
    K : Key
{
    type Key = Self;

    #[inline(always)]
    fn into_key(self) -> Self::Key {
        self
    }
}

impl<'a> IntoKey for &'a String {
    type Key = &'a str;

    #[inline(always)]
    fn into_key(self) -> Self::Key {
        self as &str
    }
}

impl<'a, KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned, const SIZE: usize> IntoKey for &'a [KeyCharT; SIZE] {
    type Key = &'a [KeyCharT];

    #[inline(always)]
    fn into_key(self) -> Self::Key {
        &self[..]
    }
}

/// Implemented by all types that can be used as keys, whether they are UTF-8 encoded
/// strings or arrays of KeyCharT
pub trait Key : Eq + Hash + Clone + KeyUnsafe {
    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    //type BorrowedKey<'a> : Key<KeyCharT>;

    fn num_chars(&self) -> usize;
    fn as_bytes(&self) -> &[u8];
    fn into_bytes(self) -> Vec<u8>;
    fn borrow_key_chars(&self) -> Option<&[Self::KeyCharT]>;
    fn get_key_chars(&self) -> Vec<Self::KeyCharT>;
    fn borrow_key_str(&self) -> Option<&str>;

    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // fn new_borrowed_from_owned<'a, OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &'a OwnedKeyT) -> Self::BorrowedKey<'a>;
}

/// The private unsafe accessors for the Key trait
pub trait KeyUnsafe : Eq + Hash + Clone {
    type KeyCharT : Clone;
    /// This function may return a result that borrows the owned_key parameter, but the
    /// returned result may have a longer lifetime on account of the type it's called with
    unsafe fn from_owned_unsafe<'b, OwnedKeyT : OwnedKey + KeyUnsafe<KeyCharT = Self::KeyCharT>>(owned_key : &'b OwnedKeyT) -> Self;
}

impl <KeyCharT>Key for &[KeyCharT]
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned,
{
    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // type BorrowedKey<'a> = &'a [KeyCharT];

    #[inline(always)]
    fn num_chars(&self) -> usize {
        self.len()
    }

    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        unsafe { slice::from_raw_parts(self.as_ptr() as *const u8, size_of::<KeyCharT>() * len) }
    }

    #[inline(always)]
    fn into_bytes(self) -> Vec<u8> {

        let mut owned_vec = self.to_vec();
        let len = owned_vec.len();
        let cap = owned_vec.capacity();

        //Now transmute the vec into a vec of bytes
        let result = unsafe { Vec::<u8>::from_raw_parts(owned_vec.as_mut_ptr() as *mut u8, size_of::<KeyCharT>() * len, size_of::<KeyCharT>() * cap) };
        forget(owned_vec); //So we don't get a double-free
        result
    }

    #[inline(always)]
    fn borrow_key_chars(&self) -> Option<&[KeyCharT]> {
        Some(self)
    }

    #[inline(always)]
    fn get_key_chars(&self) -> Vec<KeyCharT> {
        self.to_vec()
    }

    #[inline(always)]
    fn borrow_key_str(&self) -> Option<&str> {
        None
    }

    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // fn new_borrowed_from_owned<'a, OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &'a OwnedKeyT) -> &'a [KeyCharT] {
    //     owned_key.borrow_vec().unwrap()
    // }
}

//TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
impl <'a, KeyCharT>KeyUnsafe for &'a [KeyCharT]
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned
{
    type KeyCharT = KeyCharT;

    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey + KeyUnsafe<KeyCharT = KeyCharT>>(owned_key : &OwnedKeyT) -> Self {
        let result = owned_key.borrow_vec().unwrap();
        transmute::<&[KeyCharT], &'a [KeyCharT]>(result)
    }
}

impl <KeyCharT>Key for Vec<KeyCharT>
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned,
{
    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    //type BorrowedKey<'a> = Vec<KeyCharT>;

    #[inline(always)]
    fn num_chars(&self) -> usize {
        self.len()
    }

    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        unsafe { slice::from_raw_parts(self.as_ptr() as *const u8, size_of::<KeyCharT>() * len) }
    }

    #[inline(always)]
    fn into_bytes(self) -> Vec<u8> {
        let mut mut_self = self;
        let len = mut_self.len();
        let cap = mut_self.capacity();

        let result = unsafe { Vec::<u8>::from_raw_parts(mut_self.as_mut_ptr() as *mut u8, size_of::<KeyCharT>() * len, size_of::<KeyCharT>() * cap) };
        forget(mut_self); //So we don't get a double-free

        result
    }

    #[inline(always)]
    fn borrow_key_chars(&self) -> Option<&[KeyCharT]> {
        Some(&self[..])
    }

    #[inline(always)]
    fn get_key_chars(&self) -> Vec<KeyCharT> {
        self.clone()
    }

    #[inline(always)]
    fn borrow_key_str(&self) -> Option<&str> {
        None
    }

    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // fn new_borrowed_from_owned<OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &OwnedKeyT) -> Vec<KeyCharT> {
    //     owned_key.as_vec().unwrap()
    // }
}

impl <KeyCharT>KeyUnsafe for Vec<KeyCharT>
    where
    KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned,
{
    type KeyCharT = KeyCharT;

    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey + KeyUnsafe<KeyCharT = KeyCharT>>(owned_key : &OwnedKeyT) -> Self {
        //This implementation is actually safe, but the fn prototype is unsafe
        owned_key.as_vec().unwrap()
    }
}

impl Key for &str
{
    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // type BorrowedKey<'a> = &'a str;

    #[inline(always)]
    fn num_chars(&self) -> usize {
        unicode_len(self)
    }

    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }

    #[inline(always)]
    fn into_bytes(self) -> Vec<u8> {
        let bytes_slice = str::as_bytes(self);
        bytes_slice.to_vec()
    }

    #[inline(always)]
    fn borrow_key_chars(&self) -> Option<&[char]> {
        None
    }

    #[inline(always)]
    fn get_key_chars(&self) -> Vec<char> {
        self.chars().collect()
    }

    #[inline(always)]
    fn borrow_key_str(&self) -> Option<&str> {
        Some(self)
    }

    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // fn new_borrowed_from_owned<'a, OwnedKeyT : OwnedKey<char>>(owned_key : &'a OwnedKeyT) -> &'a str {
    //     owned_key.borrow_str().unwrap()
    // }
}

impl <'a>KeyUnsafe for &'a str {
    type KeyCharT = char;

    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey>(owned_key : &OwnedKeyT) -> Self {
        let result = owned_key.borrow_str().unwrap();
        transmute::<&str, &'a str>(result)
    }
}

impl Key for String
{
    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // type BorrowedKey<'a> = String;

    #[inline(always)]
    fn num_chars(&self) -> usize {
        unicode_len(self)
    }

    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    #[inline(always)]
    fn into_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }

    #[inline(always)]
    fn borrow_key_chars(&self) -> Option<&[char]> {
        None
    }

    #[inline(always)]
    fn get_key_chars(&self) -> Vec<char> {
        self.chars().collect()
    }

    #[inline(always)]
    fn borrow_key_str(&self) -> Option<&str> {
        Some(&self)
    }

    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // fn new_borrowed_from_owned<OwnedKeyT : OwnedKey<char>>(owned_key : &OwnedKeyT) -> String {
    //     owned_key.as_string().unwrap()
    // }
}

impl KeyUnsafe for String {
    type KeyCharT = char;

    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey>(owned_key : &OwnedKeyT) -> Self {
        //This implementation is actually safe, but the fn prototype is unsafe
        owned_key.as_string().unwrap()
    }
}
