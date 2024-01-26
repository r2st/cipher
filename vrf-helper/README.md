# CIPHER VRF Module

* Provides Secp256K1 operations

## Modules

```rust
// Provides signature verification operations.
pub mod common;

// Provides Secret and Public Key generation operations.
pub mod secp_vrf;
```

## Usage

* Import following modules

```rust
use vrf_helper::common::{verify, sign, verify_with_pubkey, sign_with_secret};
use vrf_helper::secp_vrf::{SecretKey, PublicKey};
```
* For detailed usage please refer to doctests in the source code.