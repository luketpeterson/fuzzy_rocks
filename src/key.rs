//!
//! The Key module contains all of the implementation for the abstract Key trait and all
//! helper traits associated with it. Only the [Key] trait should be publicly re-exported.
//! 

use std::slice;
use core::hash::Hash;
use std::mem::{forget, size_of, transmute};

use serde::Serialize;

use super::unicode_string_helpers::*;

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

    #[inline(always)]
    fn as_string(&self) -> Option<String> {
        Some(self.clone())
    }

    #[inline(always)]
    fn borrow_str(&self) -> Option<&str> {
        Some(self)
    }

    #[inline(always)]
    fn as_vec(&self) -> Option<Vec<char>> {
        Some(self.chars().collect())
    }

    #[inline(always)]
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

    #[inline(always)]
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

    #[inline(always)]
    fn borrow_vec(&self) -> Option<&[char]> {
        None
    }

    #[inline(always)]
    fn from_key<K : Key>(k : &K) -> Self {
        k.get_key_string()
    }

    #[inline(always)]
    fn from_string(s : String) -> Self {
        s
    }

    #[inline(always)]
    fn from_vec(v : Vec<char>) -> Self {
        v.into_iter().collect()
    }
}

impl <KeyCharT : 'static + Copy + Eq + Hash + Serialize + serde::de::DeserializeOwned>OwnedKey for Vec<KeyCharT> 
{
    #[inline(always)]
    fn as_string(&self) -> Option<String> {
        None
    }

    #[inline(always)]
    fn borrow_str(&self) -> Option<&str> {
        None
    }

    #[inline(always)]
    fn as_vec(&self) -> Option<Vec<KeyCharT>> {
        Some(self.clone())
    }

    #[inline(always)]
    fn into_vec(self) -> Vec<KeyCharT> {
        self
    }

    #[inline(always)]
    fn move_into_buf<'a>(&'a self, _buf : &'a mut Vec<KeyCharT>) -> &'a Vec<KeyCharT> {
        self
    }

    #[inline(always)]
    fn borrow_vec(&self) -> Option<&[KeyCharT]> {
        Some(&self[..])
    }

    #[inline(always)]
    fn from_key<K : Key + KeyUnsafe<KeyCharT = KeyCharT>>(k : &K) -> Self { //TODO: Get rid of the KeyUnsafe trait when When GenericAssociatedTypes is stabilized
        k.get_key_chars()
    }

    #[inline(always)]
    fn from_string(_s : String) -> Self {
        panic!() //NOTE: Should never be called when the OwnedKeyT isn't a String
        //NOTE: We could implement this when KeyCharT = char, but so far a reason to do that hasn't come up
    }

    #[inline(always)]
    fn from_vec(v : Vec<KeyCharT>) -> Self {
        v
    }
}

/// A convenience trait to automatically convert the passed argument into one of the acceptable [Key] types,
/// as long as the conversion can be done with no runtime cost.
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

/// Implemented by all types that can be used as keys to access records in a [Table](crate::Table), whether they are UTF-8 encoded
/// &[str], [String]s, or arrays of [KeyCharT](crate::TableConfig::KeyCharT)
pub trait Key : Eq + Hash + Clone + KeyUnsafe {
    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // https://github.com/rust-lang/rust/issues/44265
    //type BorrowedKey<'a> : Key;

    fn num_chars(&self) -> usize;
    fn as_bytes(&self) -> &[u8];
    fn into_bytes(self) -> Vec<u8>;
    fn borrow_key_chars(&self) -> Option<&[Self::KeyCharT]>;
    fn get_key_chars(&self) -> Vec<Self::KeyCharT>;
    fn borrow_key_str(&self) -> Option<&str>;
    fn get_key_string(&self) -> String; //WARNING: This will panic if the key isn't representable as a UTF-8 String

    //TODO: When GenericAssociatedTypes is stabilized, I will remove the KeyUnsafe trait in favor of an associated type
    // fn new_borrowed_from_owned<'a, OwnedKeyT : OwnedKey<KeyCharT>>(owned_key : &'a OwnedKeyT) -> Self::BorrowedKey<'a>;
}

/// The private unsafe accessors for the Key trait
pub trait KeyUnsafe : Eq + Hash + Clone {
    type KeyCharT : Clone;
    /// This function may return a result that borrows the owned_key parameter, but the
    /// returned result may have a longer lifetime on account of the type it's called with
    unsafe fn from_owned_unsafe<OwnedKeyT : OwnedKey + KeyUnsafe<KeyCharT = Self::KeyCharT>>(owned_key : &OwnedKeyT) -> Self;
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

    #[inline(always)]
    fn get_key_string(&self) -> String {

        //This function only makes sense when KeyCharT = char, but it may be harmless as long as KeyCharT
        // is the same size as char.  If they are different sizes, we must panic!
        if size_of::<KeyCharT>() == size_of::<char>() {
            self.iter().map(|key_char| unsafe{ transmute::<&KeyCharT, &char>(key_char) }).collect()
        } else {
            panic!();
        }
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

    #[inline(always)]
    fn get_key_string(&self) -> String {

        //This function only makes sense when KeyCharT = char, but it may be harmless as long as KeyCharT
        // is the same size as char.  If they are different sizes, we must panic!
        if size_of::<KeyCharT>() == size_of::<char>() {
            self.iter().map(|key_char| unsafe{ transmute::<&KeyCharT, &char>(key_char) }).collect()
        } else {
            panic!();
        }
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

    #[inline(always)]
    fn get_key_string(&self) -> String {
        self.to_string()
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
        Some(self)
    }

    #[inline(always)]
    fn get_key_string(&self) -> String {
        self.clone()
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
