## Merkle Tree Implementation in Rust

The MerkleTree struct represents a Merkle Tree. It contains a list of nodes, where each node is an optional Hash. It provides methods to create a new Merkle Tree from slices (new_from_slices) or from existing hashes (new_from_hashes), compute the root hash (root), and calculate the depth of the tree (depth).

### Transversal and Time complexity:

- Transversal : In the given Merkle tree implementation, the type of traversal used is a form of Depth-First Traversal. The tree is built from the bottom up, from leaves to the root, computing hashes for each pair of nodes. This is performed by iterating over the array of nodes and computing hashes for pairs of nodes.

- Building the Merkle tree: The time complexity of building the Merkle tree is O(n)
  , where `n` is the number of transactions, or more generally, the number of leaves in the tree. This is because each node (including non-leaf nodes) in the tree is visited exactly once when the tree is constructed.

- Checking if a leaf exists in the tree: The time complexity of checking if a certain leaf (transaction) exists in the tree is O(n), where `n` is the number of nodes in the tree. In the worst-case scenario, we would have to check every node in the tree.

- Computing the root of the tree: The time complexity of computing the root of the tree is
  O(1), as the root hash is stored and can be returned directly.

- Computing the depth of the tree: The time complexity of computing the depth of the tree is O(1) as the depth can be computed directly from the number of nodes using the formula
  `log(n+1)` where n is the number of nodes in the tree.It's important to note that these time complexities assume that the hash computation time is constant, which may not be the case in practice depending on the hash function and the length of the input.

## Transaction construcution

- Consider test case `test_root_hash_exists` in merkle.rs which contains three transaction (T0,T1,T2). The hash of the transaction

  `T0 is denoted as A = H(T0)`

  `T1 is denoted as B = H(T1)`

  `T2 is denoted as C = H(T2)`

  `Node is denoted as P = (H(A∣∣B))`

  `Node is denoted as Q= (H(C∣∣C))` (since there's no fourth transaction to pair with C,C is hashed with itself).

  `Root (R) is denoted as R= (H(P∣∣Q))`.

                    ┌─┐
                    │R│
                    └─┘
                   /   \
               ┌─┐     ┌─┐
               │P│     │Q│
               └─┘     └─┘
              /   \      |
          ┌─┐     ┌─┐ ┌─┐
          │A│     │B│ │C│
          └─┘     └─┘ └─┘