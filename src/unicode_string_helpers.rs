

/// Returns the number of chars in a utf-8 string
pub fn unicode_len(s: &str) -> usize {
    s.chars().count()
}

/// Returns the first n characters up to len from a utf-8 string
pub fn unicode_truncate(s: &str, len: usize) -> String {
    let new_string : String = s.chars()
        .enumerate()
        .filter(|(i, _)| *i < len)
        .map(|(_, the_char)| the_char)
        .collect();
    new_string
}

/// Removes a single unicode character at the specified index from a utf-8 string stored
pub fn unicode_remove_char(s: &str, idx: usize) -> String {
    let new_str : String = s.chars()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, the_char)| the_char)
        .collect();
    new_str
}

//NOTE: Currently unneeded
// /// Returns the unicode character at the idx, counting through each character in the string
// /// Will panic if idx is greater than the number of characters in the parsed string
// pub fn unicode_char_at_index(s: &str, idx : usize) -> char {
//     s.chars().nth(idx).unwrap()
// }