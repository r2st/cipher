use directories::UserDirs;
use std::{fs::create_dir_all, path::Path};
use system::config::Config as SystemConfig;

#[derive(Debug)]
pub struct DatabaseManager;

impl DatabaseManager {
	pub(crate) fn new(config: &SystemConfig) -> String {
		let user_dirs = UserDirs::new().expect("Couldn't fetch home directory");
		let home_dir = user_dirs.home_dir().to_path_buf();
		//println!("Working directory: {:?}", home_dir);

		// Create 'cipher' directory inside the working directory
		let cipher_dir = home_dir.join(config.rocksdb_name.clone());

		// Check if directory already exists
		if Path::new(&cipher_dir).exists() {
			//println!("Directory already exists");
		} else {
			create_dir_all(&cipher_dir).expect("Couldn't create cipher directory");
			//println!("Created cipher directory");
		}

		// Convert cipher_dir to a String and return it
		cipher_dir.to_string_lossy().to_string()
	}
}
