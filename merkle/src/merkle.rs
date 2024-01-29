use crate::utils;
use bincode::serialize;
use std::time::{Duration, Instant};
use system::transaction::{Transaction, TransactionType};
use utils::{calculate_hash, hash256, hash_merkle_branches, next_power_of_two, Hash, HASH_SIZE};
#[derive(Clone, Debug)]
struct MerkleTree {
	nodes: Vec<Option<Hash>>,
}

impl MerkleTree {
	fn new_from_slices(slices: &[&[u8]]) -> MerkleTree {
		let mut hashes = Vec::with_capacity(slices.len());
		for &slice in slices {
			hashes.push(calculate_hash(slice));
		}
		MerkleTree::new_from_hashes(&hashes)
	}

	fn new_from_hashes(hashes: &[Hash]) -> MerkleTree {
		if hashes.is_empty() {
			return MerkleTree { nodes: Vec::new() };
		}
		let next_pot = next_power_of_two(hashes.len());
		let array_size = next_pot * 2 - 1;
		let mut nodes = vec![None; array_size];

		for (i, hash) in hashes.iter().enumerate() {
			nodes[i] = Some(*hash);
		}

		let mut offset = next_pot;
		for i in (0..array_size - 1).step_by(2) {
			match (nodes[i], nodes[i + 1]) {
				(None, _) => nodes[offset] = None,
				(Some(h), None) => {
					// If there is only one child, hash it with itself
					let new_hash = hash_merkle_branches(h, h);
					nodes[offset] = Some(new_hash);
				},
				(Some(h1), Some(h2)) => {
					let new_hash = hash_merkle_branches(h1, h2);
					nodes[offset] = Some(new_hash);
				},
				_ => {},
			}
			offset += 1;
		}

		MerkleTree { nodes }
	}

	fn root(&self) -> Hash {
		self.nodes.last().unwrap_or(&None).unwrap_or(Hash::new([0u8; HASH_SIZE]))
	}

	fn depth(&self) -> usize {
		self.nodes.len().next_power_of_two().trailing_zeros() as usize
	}
}

#[derive(Clone, Debug)]
struct TransactionSPV {
	sender: String,
	receiver: String,
	amount: u32,
}

impl TransactionSPV {
	// A simple hashing function for transaction
	fn hash(&self) -> Hash {
		let data = format!("{}{}{}", self.sender, self.receiver, self.amount);
		let data = data.as_bytes();
		let hash = calculate_hash(data);
		hash
	}
}

pub trait TransactionMethods {
	fn as_slice(&self) -> Vec<u8>;
	fn hash(&self) -> Hash;
}

impl TransactionMethods for Transaction {
	fn as_slice(&self) -> Vec<u8> {
		bincode::serialize(self).expect("Failed to serialize transaction")
	}

	fn hash(&self) -> Hash {
		let data = self.as_slice();
		calculate_hash(&data)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn test_calc_hash() {
		let data = b"hello world";
		let expected_hash = Hash::new([
			0xb9, 0x4d, 0x27, 0xb9, 0x93, 0x4d, 0x3e, 0x08, 0xa5, 0x2e, 0x52, 0xd7, 0xda, 0x7d,
			0xab, 0xfa, 0xc4, 0x84, 0xef, 0xe3, 0x7a, 0x53, 0x80, 0xee, 0x90, 0x88, 0xf7, 0xac,
			0xe2, 0xef, 0xcd, 0xe9,
		]);

		let result = calculate_hash(data);
		assert_eq!(result, expected_hash);
	}

	#[test]
	fn test_tree_root_empty() {
		let tree = MerkleTree::new_from_hashes(&[]);
		assert_eq!(tree.root(), Hash::new([0u8; HASH_SIZE]));
	}

	#[test]
	fn test_tree_depth_empty() {
		let tree = MerkleTree::new_from_hashes(&[]);
		assert_eq!(tree.depth(), 0);
	}

	#[test]
	fn test_tree_depth_non_empty() {
		let hashes = vec![
			Hash::new([0u8; HASH_SIZE]),
			Hash::new([1u8; HASH_SIZE]),
			Hash::new([2u8; HASH_SIZE]),
		];
		let tree = MerkleTree::new_from_hashes(&hashes);
		assert_eq!(tree.depth(), 3);
	}

	#[test]
	fn test_spv_transaction() {
		let transaction_hash = Hash::new([2u8; HASH_SIZE]);
		let other_hashes = vec![
			Hash::new([0u8; HASH_SIZE]),
			Hash::new([1u8; HASH_SIZE]),
			transaction_hash, // assuming this is the hash of our transaction
		];
		let tree = MerkleTree::new_from_hashes(&other_hashes);

		// Now we would typically use the SPV client to verify the transaction.
		// Since we don't have an SPV client, we just check that the transaction's hash is in the
		// tree. This is a very simplified version and doesn't really constitute SPV, but it's a
		// start.
		let is_in_tree = tree.nodes.iter().any(|&node_hash_option| match node_hash_option {
			Some(node_hash) => node_hash == transaction_hash,
			None => false,
		});

		assert!(is_in_tree, "Transaction hash not found in Merkle tree");
	}

	#[test]
	fn test_merkle_tree_structure() {
		let hashes = vec![
			Hash::new([0u8; HASH_SIZE]),
			Hash::new([1u8; HASH_SIZE]),
			Hash::new([2u8; HASH_SIZE]),
		];
		let tree = MerkleTree::new_from_hashes(&hashes);

		// Print out the nodes of the tree
		for (i, node) in tree.nodes.iter().enumerate() {
			match node {
				Some(hash) => println!("Node {}: {}", i, hash),
				None => println!("Node {}: None", i),
			}
		}

		// The structure of the tree should now be as follows:
		//
		//       6
		//     /   \
		//    4     5
		//   / \   / \
		//  0   1 2   2
		//
		// The tree is a complete binary tree, and all non-leaf nodes are hashes of their children's
		// hashes.

		assert_eq!(tree.nodes.len(), 7); // root + 2 level-1 nodes + 4 level-2 nodes
		assert_eq!(tree.depth(), 3); // root at depth 0, 2 levels below root

		// Check that the root node is a hash of the child nodes
		let left_child = tree.nodes[4].unwrap();
		let right_child = tree.nodes[5].unwrap();
		let root_hash = hash_merkle_branches(left_child, right_child);
		assert_eq!(tree.root(), root_hash);

		// Similarly, you can add checks for the other nodes of the tree if needed.
	}

	#[test]
	fn test_leaf_exists() {
		let hashes = vec![
			Hash::new([0u8; HASH_SIZE]),
			Hash::new([1u8; HASH_SIZE]),
			Hash::new([2u8; HASH_SIZE]),
		];
		let tree = MerkleTree::new_from_hashes(&hashes);

		// Check that each hash in `hashes` is a leaf node in the tree
		for &hash in &hashes {
			let hash_exists_in_tree =
				tree.nodes.iter().any(|&node_hash_option| match node_hash_option {
					Some(node_hash) => node_hash == hash,
					None => false,
				});
			assert!(hash_exists_in_tree, "Hash not found in Merkle tree");
		}
	}

	#[test]
	fn test_average_time_leaf_exists() {
		let hashes = vec![
			Hash::new([0u8; HASH_SIZE]),
			Hash::new([1u8; HASH_SIZE]),
			Hash::new([2u8; HASH_SIZE]),
		];
		let tree = MerkleTree::new_from_hashes(&hashes);

		let mut total_duration = Duration::new(0, 0);

		// Check that each hash in `hashes` is a leaf node in the tree
		for &hash in &hashes {
			let start = Instant::now();
			let hash_exists_in_tree =
				tree.nodes.iter().any(|&node_hash_option| match node_hash_option {
					Some(node_hash) => node_hash == hash,
					None => false,
				});
			let duration = start.elapsed();
			total_duration += duration;

			assert!(hash_exists_in_tree, "Hash not found in Merkle tree");
		}

		let average_duration = total_duration / hashes.len() as u32;
		println!("Average time elapsed in search is: {:?}", average_duration);
	}

	#[test]
	fn test_transaction_exists() {
		// Create some transactions
		let transactions = vec![
			TransactionSPV { sender: "Alice".to_string(), receiver: "Bob".to_string(), amount: 50 },
			TransactionSPV {
				sender: "Bob".to_string(),
				receiver: "Charlie".to_string(),
				amount: 25,
			},
			TransactionSPV {
				sender: "Charlie".to_string(),
				receiver: "Alice".to_string(),
				amount: 30,
			},
		];

		// Get the hashes of the transactions and create the Merkle tree
		let hashes: Vec<_> = transactions.iter().map(|t| t.hash()).collect();
		let tree = MerkleTree::new_from_hashes(&hashes);

		// Check that each transaction's hash exists in the Merkle tree
		for transaction in &transactions {
			let hash = transaction.hash();
			let hash_exists_in_tree =
				tree.nodes.iter().any(|&node_hash_option| match node_hash_option {
					Some(node_hash) => node_hash == hash,
					None => false,
				});
			assert!(hash_exists_in_tree, "Transaction hash found in Merkle tree");
		}
	}

	#[test]
	fn test_l1x_transaction_exists() {
		let pool_address: [u8; 20] = [
			0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];
		// Create some transactions
		let transactions = vec![
			Transaction {
				nonce: 0,
				transaction_type: TransactionType::Stake { pool_address, amount: 50 },
				fee_limit: 10,
				signature: vec![0u8; HASH_SIZE],
				verifying_key: vec![0u8; HASH_SIZE],
			},
			Transaction {
				nonce: 1,
				transaction_type: TransactionType::Stake { pool_address, amount: 75 },
				fee_limit: 20,
				signature: vec![1u8; HASH_SIZE],
				verifying_key: vec![1u8; HASH_SIZE],
			},
			Transaction {
				nonce: 2,
				transaction_type: TransactionType::Stake { pool_address, amount: 100 },
				fee_limit: 30,
				signature: vec![2u8; HASH_SIZE],
				verifying_key: vec![2u8; HASH_SIZE],
			},
		];

		// Get the hashes of the transactions and create the Merkle tree
		let hashes: Vec<_> = transactions.iter().map(|t| t.hash()).collect();
		let tree = MerkleTree::new_from_hashes(&hashes);

		// Check that each transaction's hash exists in the Merkle tree
		for transaction in &transactions {
			let hash = transaction.hash();
			let hash_exists_in_tree =
				tree.nodes.iter().any(|&node_hash_option| match node_hash_option {
					Some(node_hash) => node_hash == hash,
					None => false,
				});
			assert!(hash_exists_in_tree, "Transaction hash not found in Merkle tree");
		}
	}

	#[test]
	fn test_root_hash_exists() {
		let pool_address: [u8; 20] = [
			0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];
		// Create some transactions
		let mut transactions = vec![
			Transaction {
				nonce: 0,
				transaction_type: TransactionType::Stake { pool_address, amount: 50 },
				fee_limit: 10,
				signature: vec![0u8; HASH_SIZE],
				verifying_key: vec![0u8; HASH_SIZE],
			},
			Transaction {
				nonce: 1,
				transaction_type: TransactionType::Stake { pool_address, amount: 75 },
				fee_limit: 20,
				signature: vec![1u8; HASH_SIZE],
				verifying_key: vec![1u8; HASH_SIZE],
			},
			Transaction {
				nonce: 2,
				transaction_type: TransactionType::Stake { pool_address, amount: 100 },
				fee_limit: 30,
				signature: vec![2u8; HASH_SIZE],
				verifying_key: vec![2u8; HASH_SIZE],
			},
		];
		// Get the byte slices of the transactions and create the Merkle tree
		let slices: Vec<Vec<u8>> = transactions.iter().map(|t| t.as_slice()).collect();
		let slices_refs: Vec<&[u8]> = slices.iter().map(|slice| slice.as_slice()).collect();
		let tree = MerkleTree::new_from_slices(&slices_refs);

		// Get the root hash
		let root_hash = tree.root();

		// Check if the root hash exists in the Merkle tree
		let root_hash_exists_in_tree =
			tree.nodes.iter().any(|&node_hash_option| match node_hash_option {
				Some(node_hash) => node_hash == root_hash,
				None => false,
			});

		assert!(root_hash_exists_in_tree, "Root hash not found in Merkle tree");
	}

	#[test]
	fn test_compare_root_hash() {
		let pool_address: [u8; 20] = [
			0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];
		// Create some transactions
		let mut transactions = vec![
			Transaction {
				nonce: 0,
				transaction_type: TransactionType::Stake { pool_address, amount: 50 },
				fee_limit: 10,
				signature: vec![0u8; HASH_SIZE],
				verifying_key: vec![0u8; HASH_SIZE],
			},
			Transaction {
				nonce: 1,
				transaction_type: TransactionType::Stake { pool_address, amount: 75 },
				fee_limit: 20,
				signature: vec![1u8; HASH_SIZE],
				verifying_key: vec![1u8; HASH_SIZE],
			},
			Transaction {
				nonce: 2,
				transaction_type: TransactionType::Stake { pool_address, amount: 100 },
				fee_limit: 30,
				signature: vec![2u8; HASH_SIZE],
				verifying_key: vec![2u8; HASH_SIZE],
			},
		];

		// Get the byte slices of the transactions and create the first Merkle tree
		let slices: Vec<Vec<u8>> = transactions.iter().map(|t| t.as_slice()).collect();
		let slices_refs: Vec<&[u8]> = slices.iter().map(|slice| slice.as_slice()).collect();
		let tree1 = MerkleTree::new_from_slices(&slices_refs);

		// Create the second Merkle tree with the same transactions
		let slices2: Vec<Vec<u8>> = transactions.iter().map(|t| t.as_slice()).collect();
		let slices_refs2: Vec<&[u8]> = slices2.iter().map(|slice| slice.as_slice()).collect();
		let tree2 = MerkleTree::new_from_slices(&slices_refs2);

		// Compare the root hashes of the two trees
		let root_hash1 = tree1.root();
		let root_hash2 = tree2.root();

		assert_eq!(root_hash1, root_hash2, "Root hashes do not match");
	}

	#[test]
	fn test_leaf_contains_transaction() {
		let pool_address: [u8; 20] = [
			0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];
		// Create some transactions
		let mut transactions = vec![
			Transaction {
				nonce: 0,
				transaction_type: TransactionType::Stake { pool_address, amount: 50 },
				fee_limit: 10,
				signature: vec![0u8; HASH_SIZE],
				verifying_key: vec![0u8; HASH_SIZE],
			},
			Transaction {
				nonce: 1,
				transaction_type: TransactionType::Stake { pool_address, amount: 75 },
				fee_limit: 20,
				signature: vec![1u8; HASH_SIZE],
				verifying_key: vec![1u8; HASH_SIZE],
			},
			Transaction {
				nonce: 2,
				transaction_type: TransactionType::Stake { pool_address, amount: 100 },
				fee_limit: 30,
				signature: vec![2u8; HASH_SIZE],
				verifying_key: vec![2u8; HASH_SIZE],
			},
		];
		// Get the byte slices of the transactions and create the Merkle tree
		let slices: Vec<Vec<u8>> = transactions.iter().map(|t| t.as_slice()).collect();
		let slices_refs: Vec<&[u8]> = slices.iter().map(|slice| slice.as_slice()).collect();
		let tree = MerkleTree::new_from_slices(&slices_refs);

		// Pick a transaction to check
		let transaction = &transactions[0];
		let hash = transaction.hash();

		// Check if this transaction's hash is present in the tree's leaf nodes
		let leaf_nodes = &tree.nodes[0..slices_refs.len()];
		let hash_exists_in_leaves =
			leaf_nodes.iter().any(|&node_hash_option| match node_hash_option {
				Some(node_hash) => node_hash == hash,
				None => false,
			});

		assert!(hash_exists_in_leaves, "Transaction hash not found in Merkle tree leaves");
	}
}
