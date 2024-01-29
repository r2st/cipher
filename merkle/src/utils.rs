use sha2::{Digest, Sha256};
use std::{error::Error, fmt};

pub const HASH_SIZE: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Hash([u8; HASH_SIZE]);

impl Hash {
	pub fn new(data: [u8; HASH_SIZE]) -> Hash {
		Hash(data)
	}
}

impl fmt::Display for Hash {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		for byte in &self.0 {
			write!(f, "{:02x}", byte)?;
		}
		Ok(())
	}
}

pub fn calculate_hash(data: &[u8]) -> Hash {
	let h = from_bytes(&hash256(data)).unwrap();
	h
}

pub fn from_bytes(data: &[u8]) -> Result<Hash, Box<dyn Error>> {
	if data.len() != HASH_SIZE {
		return Err(
			format!("Hash should be {} bytes, but it is {} bytes", HASH_SIZE, data.len()).into()
		);
	}
	let mut h = [0u8; HASH_SIZE];
	h.copy_from_slice(&data[..HASH_SIZE]);
	Ok(Hash::new(h))
}

pub fn hash256(data: &[u8]) -> [u8; HASH_SIZE] {
	let mut h = [0u8; HASH_SIZE];
	h.copy_from_slice(&sha2::Sha256::digest(data));
	h
}

pub fn hash_merkle_branches(left: Hash, right: Hash) -> Hash {
	let mut h = [0u8; HASH_SIZE * 2];
	h[..HASH_SIZE].copy_from_slice(&left.0);
	h[HASH_SIZE..].copy_from_slice(&right.0);

	let new_hash = calculate_hash(&h);
	new_hash
}

pub fn next_power_of_two(n: usize) -> usize {
	if n.is_power_of_two() {
		n
	} else {
		2usize.pow((n as f64).log2() as u32 + 1)
	}
}
