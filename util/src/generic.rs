use super::convert::address_from_str;
use anyhow::{anyhow, Error};
use chrono::NaiveDateTime;
use log::error;
use primitives::{Address, BlockTimeStamp, TimeStamp as PriTimeStamp};
use scylla::frame::value::CqlTimestamp;
use std::{
	env::var,
	fs, io,
	path::Path,
	time::{SystemTime, UNIX_EPOCH},
};

pub fn get_cassandra_timestamp() -> Result<CqlTimestamp, Error> {
	let milliseconds: i64 = SystemTime::now()
		.duration_since(UNIX_EPOCH)?
		.as_millis()
		.try_into()
		.unwrap_or(i64::MAX);
	Ok(CqlTimestamp(milliseconds))
}

pub fn get_rocks_db_timestamp() -> Result<PriTimeStamp, Error> {
	let milliseconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
	Ok(milliseconds)
}

pub fn get_postgres_timestamp() -> Result<PriTimeStamp, Error> {
	let milliseconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
	Ok(milliseconds)
}

fn remove_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
	for entry in fs::read_dir(&path)? {
		let entry = entry?;
		let path = entry.path();
		if path.is_dir() {
			remove_dir_all(&path)?;
		} else {
			fs::remove_file(&path)?;
		}
	}
	fs::remove_dir(&path)
}

pub fn convert_to_naive_datetime(ts: PriTimeStamp) -> NaiveDateTime {
	/*let duration_in_chrono = ts.0;
	let seconds = duration_in_chrono.num_seconds();
	let nanos = (duration_in_chrono - Duration::seconds(seconds)).num_nanoseconds().unwrap_or(0);*/
	let x = NaiveDateTime::from_timestamp_millis(ts as i64).unwrap_or(NaiveDateTime::default());
	x
}

pub fn block_timestamp_to_naive_datetime(ts: BlockTimeStamp) -> NaiveDateTime {
	/*let duration_in_chrono = ts.0;
	let seconds = duration_in_chrono.num_seconds();
	let nanos = (duration_in_chrono - Duration::seconds(seconds)).num_nanoseconds().unwrap_or(0);*/
	let x = NaiveDateTime::from_timestamp_millis(ts as i64).unwrap_or(NaiveDateTime::default());
	x
}

pub async fn whitelist_check(from_address: &Address, to_address: &Address) -> Result<(), Error> {
	// Whitelisted Receiver Address
	let env_whitelisted_sender_addresses = var("WHITELISTED_SENDER_ADDRESSES")
		.map_err(|_| anyhow!("WHITELISTED_SENDER_ADDRESSES not set"))?;

	println!("env_whitelisted_sender_addresses {:?}", env_whitelisted_sender_addresses);

	// Split the string by commas, convert each to lowercase, and collect into a vector
	let whitelisted_sender_addresses_result: Result<Vec<Address>, anyhow::Error> =
		env_whitelisted_sender_addresses
			.split(',')
			.map(|s| address_from_str(&s)) // Trim whitespace and convert to lowercase
			.collect();

	let whitelisted_sender_addresses = match whitelisted_sender_addresses_result {
		Ok(addresses) => addresses,
		Err(e) => {
			error!("Failed to get whitelisted receiver addresses: {}", e);
			return Err(e);
		},
	};
	println!("whitelisted_sender_addresses {:?}", whitelisted_sender_addresses);

	// Whitelisted Receiver Address
	let env_whitelisted_receiver_addresses = var("WHITELISTED_RECEIVER_ADDRESSES")
		.map_err(|_| anyhow!("WHITELISTED_RECEIVER_ADDRESSES not set"))?;

	// Split the string by commas, convert each to lowercase, and collect into a vector
	let whitelisted_receiver_addresses_result: Result<Vec<Address>, anyhow::Error> =
		env_whitelisted_receiver_addresses
			.split(',')
			.map(|s| address_from_str(&s)) // Trim whitespace and convert to lowercase
			.collect();

	let whitelisted_receiver_addresses = match whitelisted_receiver_addresses_result {
		Ok(addresses) => addresses,
		Err(e) => {
			error!("Failed to get whitelisted receiver addresses: {}", e);
			return Err(e);
		},
	};
	println!("whitelisted_receiver_addresses {:?}", whitelisted_receiver_addresses);

	let mut is_whitelisted_sender = false;
	let mut is_whitelisted_receiver = false;

	let mut allow_transfer = false;

	println!("Before Check:is_whitelisted_sender {:?}", is_whitelisted_sender);
	println!("Before Check:is_whitelisted_receiver {:?}", is_whitelisted_receiver);
	println!("Before Check:allow_transfer {:?}", allow_transfer);

	println!("Before Check:whitelisted_sender_addresses {:?}", whitelisted_sender_addresses);
	println!("Before Check:whitelisted_receiver_addresses {:?}", whitelisted_receiver_addresses);

	println!("Before Check:from_address {:?}", from_address);
	println!("Before Check:to_address {:?}", to_address);

	if whitelisted_sender_addresses.contains(&from_address) {
		is_whitelisted_sender = true;
	}

	if whitelisted_receiver_addresses.contains(&to_address) {
		is_whitelisted_receiver = true;
	}

	if is_whitelisted_sender {
		allow_transfer = true;
	} else if is_whitelisted_receiver {
		allow_transfer = true;
	}

	println!("After Check:is_whitelisted_sender {:?}", is_whitelisted_sender);
	println!("After Check:is_whitelisted_receiver {:?}", is_whitelisted_receiver);
	println!("After Check:allow_transfer {:?}", allow_transfer);

	if !allow_transfer {
		let message = format!(
			r#"
			❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌
			👮👮Insufficient privileges 👮👮
			❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌
			"#
		);
		error!("{}", message);
		return Err(anyhow!(message))
	} else {
		return Ok(())
	}
}
