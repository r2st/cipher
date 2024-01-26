pub mod init;
pub mod start;
use crate::commands::{init::InitCmd, start::StartCmd};
use async_trait::async_trait;
use structopt::StructOpt;

#[async_trait]
pub trait CIPHERCommand {
	/// Returns the result of the command execution.
	async fn execute(self);
}

#[derive(Debug, StructOpt)]
pub enum Command {
	///Initialize the cipher-core genesis file, config and node
	#[structopt(name = "init")]
	Init(InitCmd),
	///Start the cipher-core
	#[structopt(name = "start")]
	Start(StartCmd),
}

impl Command {
	/// Wrapper around `StructOpt::from_args` method.
	pub fn from_args() -> Self {
		<Self as StructOpt>::from_args()
	}
}

#[async_trait]
impl CIPHERCommand for Command {
	async fn execute(self) {
		match self {
			Self::Init(command) => command.execute().await,
			Self::Start(command) => command.execute().await,
		}
	}
}
