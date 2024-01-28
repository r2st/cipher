use libp2p::futures::StreamExt;
use libp2p_gossipsub::{self as gossipsub, MessageId};

use crate::config::*;

use either::Either;
use libp2p::{
	core::upgrade,
	identify,
	kad::{
		store::MemoryStore, Behaviour as Kademlia, Config as KademliaConfig,
		Event as KademliaEvent, QueryResult,
	},
};
//::{record::store::MemoryStore, Kademlia, KademliaConfig, KademliaEvent, QueryResult},
use libp2p::{
	identity,
	identity::Keypair,
	noise,
	swarm::{NetworkBehaviour, Swarm, SwarmEvent},
	tcp, yamux, Multiaddr, PeerId, SwarmBuilder, Transport,
};

use libp2p::multiaddr::Protocol;
use std::collections::{hash_map, hash_map::DefaultHasher, HashMap};

use itertools::Itertools;
use std::{
	error::Error,
	hash::{Hash, Hasher},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use log::{debug, error, info, warn};

use std::time::Duration;
use system::{
	block::{BlockBroadcast, BlockPayload},
	block_header::{BlockHeaderBroadcast, BlockHeaderPayload},
	block_proposer::{BlockProposerBroadcast, BlockProposerPayload},
	node_info::{NodeInfo, NodeInfoBroadcast},
	transaction::{Transaction, TransactionBroadcast},
	vote::{Vote, VoteBroadcast},
	vote_result::{VoteResult, VoteResultBroadcast},
};
use tokio::{
	io,
	sync::{mpsc, oneshot},
};

/// Creates the network components, namely:
///
/// - The network client to interact with the network layer from anywhere within your application.
///
/// - The network event stream, e.g. for incoming requests.
///
/// - The network task driving the network itself.
pub async fn new(
	local_key: Keypair,
	bootnodes: &[&str],
) -> Result<(Client, mpsc::Receiver<Event>, EventLoop), Box<dyn Error>> {
	// Create a public/private key pair, either random or based on a seed.
	let local_peer_id = local_key.public().to_peer_id();

	//let _tcp_transport = libp2p::tokio_development_transport(local_key.clone())?;

	let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
		.upgrade(upgrade::Version::V1)
		.authenticate(noise::Config::new(&local_key).expect("signing libp2p-noise static keypair"))
		.multiplex(yamux::Config::default())
		.timeout(std::time::Duration::from_secs(20))
		.boxed();

	// Create new topic for transactions
	let node_join_topic = get_topic_hash(NODE_INFO_TOPIC);
	let tx_topic = get_topic_hash(TRANSACTIONS_TOPIC);
	let block_validate_topic = get_topic_hash(BLOCKS_VALIDATE_TOPIC);
	let block_topic = get_topic_hash(BLOCKS_TOPIC);
	let block_header_topic = get_topic_hash(BLOCK_HEADERS_TOPIC);
	let block_proposer_topic = get_topic_hash(BLOCK_PROPOSER_TOPIC);
	let vote_topic = get_topic_hash(VOTE_TOPIC);
	let vote_result_topic = get_topic_hash(VOTE_RESULT_TOPIC);

	let topics = vec![
		node_join_topic,
		tx_topic,
		block_topic,
		block_header_topic,
		vote_result_topic,
		block_validate_topic,
		block_proposer_topic,
		vote_topic,
	];

	// Only the very first node should start subscribed to these validator only topics
	// as it is hardcoded to be a validator from genesis
	// if bootnodes.is_empty() {
	// 	topics.push(block_validate_topic);
	// 	topics.push(block_proposer_topic);
	// 	topics.push(vote_topic);
	// }

	// Build the Swarm, connecting the lower layer transport logic with the
	// higher layer network behaviour logic.
	/*let swarm = {
		let mut behaviour = Behaviour {
			identify: identify::Behaviour::new(identify::Config::new(
				"/ipfs/id/1.0.0".to_string(),
				local_keys.public(),
			)),
			// mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?,
			kademlia: kademlia_behaviour(local_peer_id),
			gossipsub: gossipsub_behaviour(local_keys.clone(), topics)?,
		};

		// If provided, bootstrap routing table with bootnode(s)
		if !bootnodes.is_empty() {
			for bootnode in bootnodes {
				info!("Bootstrapping to: {}", bootnode);
				let (peer, _, address) = bootnode.rsplitn(3, "/").into_iter().tuples().next().ok_or(anyhow!("Invalid bootnode address, expecting format /ip4/<address>/tcp/<port>/p2p/<peer_id>, got {bootnode}"))?;
				behaviour.kademlia.add_address(&peer.parse()?, address.parse()?);
			}

			behaviour.kademlia.bootstrap()?;
		}

		SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build()
	};*/

	let mut swarm = SwarmBuilder::with_new_identity()
		.with_tokio()
		.with_tcp(
			tcp::Config::default(),
			noise::Config::new,
			yamux::Config::default,
		)?
		.with_quic()
		.with_behaviour(|key| {
			let mut behaviour = Behaviour {
				identify: identify::Behaviour::new(identify::Config::new(
					"/ipfs/id/1.0.0".to_string(),
					local_key.public(),
				)),
				// mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?,
				kademlia: kademlia_behaviour(local_peer_id),
				gossipsub: gossipsub_behaviour(local_key.clone(), topics).expect("error gossipsub_behaviour"),
			};
			// If provided, bootstrap routing table with bootnode(s)
			if !bootnodes.is_empty() {
				for bootnode in bootnodes {
					info!("Bootstrapping to: {}", bootnode);
					let (peer, _, address) = bootnode.rsplitn(3, "/").into_iter().tuples().next().ok_or(anyhow!("Invalid bootnode address, expecting format /ip4/<address>/tcp/<port>/p2p/<peer_id>, got {bootnode}"))?;
					behaviour.kademlia.add_address(&peer.parse()?, address.parse()?);
				}

				behaviour.kademlia.bootstrap()?;
			}
			Ok(behaviour)
		})?
		.with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
		.build();

	let (command_sender, command_receiver) = mpsc::channel(1000);
	let (event_sender, event_receiver) = mpsc::channel(1000);

	Ok((
		Client { sender: command_sender },
		event_receiver,
		EventLoop::new(swarm, command_receiver, event_sender),
	))
}

#[derive(Clone)]
pub struct Client {
	sender: mpsc::Sender<Command>,
}

impl Client {
	/// Listen for incoming connections on the given address.
	pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::StartListening { addr, sender })
			.await
			.expect("Command receiver not to be dropped.");
		receiver.await.expect("Sender not to be dropped.")
	}

	/// Command the node to subscribe to gossipsub messages published under the given `topic`
	pub async fn subscribe_to_topic(&mut self, topic: String) -> Result<(), Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::TopicSubscribe { topic_string: topic, sender })
			.await
			.expect("Command receiver not to be dropped.");
		receiver.await.expect("Sender not to be dropped.")
	}

	/// Command the node to unsubscribe from the gossipsub messages published under the given
	/// `topic`
	pub async fn unsubscribe_from_topic(
		&mut self,
		topic: String,
	) -> Result<(), Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::TopicUnsubscribe { topic_string: topic, sender })
			.await
			.expect("Command receiver not to be dropped.");
		receiver.await.expect("Sender not to be dropped.")
	}
}

#[async_trait]
impl NodeInfoBroadcast for Client {
	/// Command the node to broadcast a transaction to the p2p network.
	async fn node_info_broadcast(
		&self,
		node_info: NodeInfo,
	) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastNodeInfo { node_info, sender })
			.await
			// .expect("Command receiver not to be dropped.");
			.unwrap_or_else(|e| {
				error!(
					"Failed to send BroadcastNodeInfo command down command_sender channel: {:?}",
					e
				)
			});
		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}
}

#[async_trait]
impl TransactionBroadcast for Client {
	/// Command the node to broadcast a transaction to the p2p network.
	async fn transaction_broadcast(&self, transaction: Transaction) -> Result<MessageId> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastTransaction { transaction, sender })
			.await
			// .expect("Command receiver not to be dropped.");
			.unwrap_or_else(|e| {
				error!(
					"Failed to send BroadcastTransaction command down command_sender channel: {:?}",
					e
				)
			});
		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(anyhow!("Failed to broadcast transaction: {:?}", e)),
			},
			Err(e) => Err(anyhow::Error::from(e)),
		}
	}
}

#[async_trait]
impl BlockBroadcast for Client {
	async fn block_broadcast(
		&self,
		block_payload: BlockPayload,
	) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastBlock { block_payload, sender })
			.await
			.unwrap_or_else(|e| {
				error!("Failed to send BroadcastBlock command down command_sender channel: {e:?}");
			});

		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}

	async fn block_validate_broadcast(
		&self,
		block_payload: BlockPayload,
	) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastValidateBlock { block_payload, sender })
			.await
			.unwrap_or_else(|e| {
				error!("Failed to send BroadcastBlock command down command_sender channel: {e:?}");
			});

		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}
}

#[async_trait]
impl BlockHeaderBroadcast for Client {
	async fn block_header_broadcast(
		&self,
		block_header_payload: BlockHeaderPayload,
	) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastBlockHeader { block_header_payload, sender })
			.await
			.unwrap_or_else(|e| {
				error!("Failed to send BroadcastBlockHeader command down command_sender channel: {e:?}");
			});

		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}
}

#[async_trait]
impl BlockProposerBroadcast for Client {
	async fn block_proposer_broadcast(
		&self,
		block_proposer_payload: BlockProposerPayload,
	) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastBlockProposer { block_proposer_payload, sender })
			.await
			.unwrap_or_else(|e| {
				error!("Failed to send BroadcastBlockProposer command down command_sender channel: {e:?}");
			});

		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}
}

#[async_trait]
impl VoteBroadcast for Client {
	async fn vote_broadcast(&self, vote: Vote) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastVote { vote, sender })
			.await
			.unwrap_or_else(|e| {
				error!("Failed to send BroadcastVote command down command_sender channel: {e:?}");
			});

		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}
}

#[async_trait]
impl VoteResultBroadcast for Client {
	async fn vote_result_broadcast(
		&self,
		vote_result: VoteResult,
	) -> Result<MessageId, Box<dyn Error + Send>> {
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(Command::BroadcastVoteResult { vote_result, sender })
			.await
			.unwrap_or_else(|e| {
				error!(
					"Failed to send BroadcastVoteResult command down command_sender channel: {e:?}"
				);
			});

		match receiver.await {
			Ok(res) => match res {
				Ok(msg_id) => Ok(msg_id),
				Err(e) => Err(e),
			},
			Err(e) => Err(Box::new(e)),
		}
	}
}

pub struct EventLoop {
	swarm: Swarm<Behaviour>,
	command_receiver: mpsc::Receiver<Command>,
	event_sender: mpsc::Sender<Event>,
	pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
}

impl EventLoop {
	fn new(
		swarm: Swarm<Behaviour>,
		command_receiver: mpsc::Receiver<Command>,
		event_sender: mpsc::Sender<Event>,
	) -> Self {
		Self { swarm, command_receiver, event_sender, pending_dial: Default::default() }
	}

	/// Start the event loop. This will listen for commands from the node (itself) and events from
	/// p2p network,
	pub async fn run(mut self) {
		loop {
			tokio::select! {
				event = self.swarm.select_next_some() => self.handle_event(event/*/.expect("Swarm stream to be infinite.")*/).await  ,
				command = self.command_receiver.recv() => match command {
					Some(c) => self.handle_command(c).await,
					// Command channel closed, thus shutting down the network event loop.
					None=>  return,
				},
			}
		}
	}

	/// Handles events received from the p2p network. This can result in anything from logging some
	/// info to sending a transaction for mempool validation.
	async fn handle_event(
		&mut self,
		event: SwarmEvent<BehaviourEvent>, //, Either<Either<io::Error, io::Error>, void::Void>
	) {
		match event {
			SwarmEvent::NewListenAddr { address, .. } => {
				let local_peer_id = *self.swarm.local_peer_id();
				info!(
					"Local node is listening on {:?}",
					address.with(Protocol::P2p(local_peer_id.into()))
				);
			},
			SwarmEvent::IncomingConnection { .. } => {},
			SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } =>
				if endpoint.is_dialer() {
					if let Some(sender) = self.pending_dial.remove(&peer_id) {
						let _ = sender.send(Ok(()));
					}
				},
			SwarmEvent::ConnectionClosed { .. } => {},
			SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
				if let Some(peer_id) = peer_id {
					if let Some(sender) = self.pending_dial.remove(&peer_id) {
						let _ = sender.send(Err(Box::new(error)));
					}
				}
			},
			SwarmEvent::IncomingConnectionError { .. } => {},
			SwarmEvent::Dialing { peer_id: Some(peer_id), .. } => info!("Dialing {peer_id}"),
			SwarmEvent::Behaviour(event) => match event {
				BehaviourEvent::Identify(event) => match event {
					// Prints peer id identify info is being sent to.
					identify::Event::Sent { peer_id, .. } => {
						info!("Sent identify info to {peer_id:?}")
					},
					// Prints out the info received via the identify event
					identify::Event::Received { info, .. } => {
						info!("Received {info:?}")
					},
					_ => info!("Some Identify event received: {event:?}"),
				},
				BehaviourEvent::Kademlia(event) => match event {
					KademliaEvent::OutboundQueryProgressed { result, .. } => match result {
						QueryResult::Bootstrap(result) => match result {
							Ok(res) => {
								info!("BOOTSTRAP SUCCESS: {res:?}");
							},
							Err(e) => {
								error!("BOOTSTRAP FAILURE: {e:?}");
							},
						},
						_ => info!("Some OutboundQueryProgressed event received: {result:?}"),
					},
					KademliaEvent::RoutingUpdated { peer, .. } => {
						info!("Kademlia routing updated: {peer:?}")
					},
					_ => info!("Some Kademlia event received: {event:?}"),
				},
				BehaviourEvent::Gossipsub(event) => match event {
					gossipsub::Event::Subscribed { peer_id, topic } => {
						info!("Peer: {peer_id:?} subscribed to '{topic:?}'",)
					},
					gossipsub::Event::Unsubscribed { peer_id, topic } => {
						info!("Peer: {peer_id:?} unsubscribed from '{topic:?}'",)
					},
					gossipsub::Event::Message {
						propagation_source: peer_id,
						message_id: id,
						message,
					} => {
						// Are we recieving a transaction/block_payload/etc
						match message.topic.as_str() {
							NODE_INFO_TOPIC => {
								if let Ok(node_info) =
									serde_json::from_slice::<NodeInfo>(&message.data)
								{
									let _ = self
										.event_sender
										.send(Event::InboundNodeInfo { node_info })
										.await
										.unwrap_or_else(|e| {
											error!(
												"Failed to send incoming node_info to receiver: {:?}",
												e
											)
										});
								}
							},
							TRANSACTIONS_TOPIC => {
								if let Ok(tx) = serde_json::from_slice::<Transaction>(&message.data)
								{
									let _ = self
										.event_sender
										.send(Event::InboundTransaction { transaction: tx })
										.await
										.unwrap_or_else(|e| {
											error!(
												"Failed to send incoming tx to receiver: {:?}",
												e
											)
										});
								}
							},
							BLOCKS_VALIDATE_TOPIC => {
								// let peer_id = message.source.unwrap_or_else(|| {
								//     error!("Failed to unwrap peer ID");
								// });
								if let Ok(block_payload) =
									serde_json::from_slice::<BlockPayload>(&message.data)
								{
									let _ = self
                                        .event_sender
                                        .send(Event::InboundValidateBlock { block_payload })
                                        .await
                                        .unwrap_or_else(|e| {
                                            error!(
                                                "Failed to send incoming block_payload to receiver: {:?}",
                                                e
                                            )
                                        });
								}
							},
							BLOCKS_TOPIC => {
								// let peer_id = message.source.unwrap_or_else(|| {
								//     error!("Failed to unwrap peer ID");
								// });
								if let Ok(block_payload) =
									serde_json::from_slice::<BlockPayload>(&message.data)
								{
									let _ = self
                                        .event_sender
                                        .send(Event::InboundBlock { block_payload })
                                        .await
                                        .unwrap_or_else(|e| {
                                            error!(
                                                "Failed to send incoming block_payload to receiver: {:?}",
                                                e
                                            )
                                        });
								}
							},
							BLOCK_HEADERS_TOPIC => {
								if let Ok(block_header_payload) =
									serde_json::from_slice::<BlockHeaderPayload>(&message.data)
								{
									let _ = self
                                        .event_sender
                                        .send(Event::InboundBlockHeader { block_header_payload })
                                        .await
                                        .unwrap_or_else(|e| {
                                            error!(
                                                "Failed to send incoming block_payload header to receiver: {:?}",
                                                e
                                            )
                                        });
								}
							},
							BLOCK_PROPOSER_TOPIC => {
								if let Ok(block_proposer_payload) =
									serde_json::from_slice::<BlockProposerPayload>(&message.data)
								{
									let _ = self
                                        .event_sender
                                        .send(Event::InboundBlockProposer { block_proposer_payload })
                                        .await
                                        .unwrap_or_else(|e| {
                                            error!(
                                                "Failed to send incoming block_proposer_payload to receiver: {:?}",
                                                e
                                            )
                                        });
								}
							},
							VOTE_TOPIC => {
								if let Ok(vote) = serde_json::from_slice::<Vote>(&message.data) {
									let _ = self
										.event_sender
										.send(Event::InboundVote { vote })
										.await
										.unwrap_or_else(|e| {
											error!(
												"Failed to send incoming vote to receiver: {:?}",
												e
											)
										});
								}
							},
							VOTE_RESULT_TOPIC => {
								if let Ok(vote_result) =
									serde_json::from_slice::<VoteResult>(&message.data)
								{
									let _ = self
										.event_sender
										.send(Event::InboundVoteResult { vote_result })
										.await
										.unwrap_or_else(|e| {
											error!(
												"Failed to send incoming vote_result to receiver: {:?}",
												e
											)
										});
								}
							},
							_ => {
								warn!("Topic {} not supported. Shouldn't recieve an unknown or un-subscribed from topic.", message.topic.as_str());
							},
						}

						debug!(
							"New p2p message received: '{}' with id: {id} from peer: {peer_id}",
							String::from_utf8_lossy(&message.data),
						)
					},
					_ => info!("Some Gossipsub event received: {event:?}"),
				},
			},
			e => warn!("{e:?}"),
		}
	}

	/// Given a vallid `Command`, execute the proper underlying libp2p calls
	async fn handle_command(&mut self, command: Command) {
		debug!("MADE IT TO HANDLE_COMMAND()");
		match command {
			Command::StartListening { addr, sender } => {
				let _ = match self.swarm.listen_on(addr) {
					Ok(_) => sender.send(Ok(())),
					Err(e) => sender.send(Err(Box::new(e))),
				};
			},
			Command::Dial { peer_id, peer_addr, sender } => {
				if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
					match self.swarm.dial(peer_addr.clone().with(Protocol::P2p(peer_id.into()))) {
						Ok(()) => {
							e.insert(sender);

							self.swarm
								.behaviour_mut()
								.kademlia
								// .add_address(&peer_id, "/dnsaddr/bootstrap.libp2p.io".parse()?);
								.add_address(&peer_id, peer_addr);
						},
						Err(e) => {
							let _ = sender.send(Err(Box::new(e)));
						},
					}
				} else {
					todo!("Already dialing peer.");
				}
			},
			Command::BroadcastNodeInfo { node_info, sender } => match node_info.as_bytes() {
				Ok(tx_bytes) => match self
					.swarm
					.behaviour_mut()
					.gossipsub
					.publish(gossipsub::IdentTopic::new(NODE_INFO_TOPIC), tx_bytes)
				{
					Ok(msg_id) => {
						info!("NodeInfo MESSAGE PUBLISHED SUCCESSFULLY");
						let _ = sender.send(Ok(msg_id));
					},
					Err(e) => {
						// error!("FAILED TO PUBLISH NodeInfo MESSAGE ");
						let _ = sender.send(Err(Box::new(e)));
					},
				},
				Err(e) => {
					error!("FAILED TO SERIALIZE TRANSACTION TO BYTES");
					let _ = sender.send(Err(e.into()));
				},
			},
			Command::BroadcastTransaction { transaction, sender } => match transaction.as_bytes() {
				Ok(tx_bytes) => match self
					.swarm
					.behaviour_mut()
					.gossipsub
					.publish(gossipsub::IdentTopic::new(TRANSACTIONS_TOPIC), tx_bytes)
				{
					Ok(msg_id) => {
						info!("Transaction published successfully");
						let _ = sender.send(Ok(msg_id));
					},
					Err(e) => {
						// error!("FAILED TO PUBLISH MESSAGE");
						let _ = sender.send(Err(Box::new(e)));
					},
				},
				Err(e) => {
					error!("FAILED TO SERIALIZE TRANSACTION TO BYTES");
					let _ = sender.send(Err(e.into()));
				},
			},
			Command::BroadcastValidateBlock { block_payload, sender } => {
				match block_payload.as_bytes() {
					Ok(block_bytes) => match self
						.swarm
						.behaviour_mut()
						.gossipsub
						.publish(gossipsub::IdentTopic::new(BLOCKS_VALIDATE_TOPIC), block_bytes)
					{
						Ok(msg_id) => {
							info!("BLOCK PUBLISHED SUCCESSFULLY");
							let _ = sender.send(Ok(msg_id));
						},
						Err(e) => {
							// error!("FAILED TO PUBLISH BLOCK");
							let _ = sender.send(Err(Box::new(e)));
						},
					},
					Err(e) => {
						error!("FAILED TO SERIALIZE BLOCK TO BYTES");
						let _ = sender.send(Err(e.into()));
					},
				}
			},
			Command::BroadcastBlock { block_payload, sender } => match block_payload.as_bytes() {
				Ok(block_bytes) => match self
					.swarm
					.behaviour_mut()
					.gossipsub
					.publish(gossipsub::IdentTopic::new(BLOCKS_TOPIC), block_bytes)
				{
					Ok(msg_id) => {
						info!("BLOCK PUBLISHED SUCCESSFULLY");
						let _ = sender.send(Ok(msg_id));
					},
					Err(e) => {
						// error!("FAILED TO PUBLISH BLOCK");
						let _ = sender.send(Err(Box::new(e)));
					},
				},
				Err(e) => {
					error!("FAILED TO SERIALIZE BLOCK TO BYTES");
					let _ = sender.send(Err(e.into()));
				},
			},
			Command::BroadcastBlockHeader { block_header_payload, sender } =>
				match block_header_payload.as_bytes() {
					Ok(block_header_bytes) => match self.swarm.behaviour_mut().gossipsub.publish(
						gossipsub::IdentTopic::new(BLOCK_HEADERS_TOPIC),
						block_header_bytes,
					) {
						Ok(msg_id) => {
							info!("HEADER PUBLISHED SUCCESSFULLY");
							let _ = sender.send(Ok(msg_id));
						},
						Err(e) => {
							// error!("FAILED TO PUBLISH HEADER");
							let _ = sender.send(Err(Box::new(e)));
						},
					},
					Err(e) => {
						error!("FAILED TO SERIALIZE HEADER TO BYTES");
						let _ = sender.send(Err(e.into()));
					},
				},
			Command::BroadcastBlockProposer { block_proposer_payload, sender } =>
				match block_proposer_payload.as_bytes() {
					Ok(cluster_block_proposers_bytes) => {
						match self.swarm.behaviour_mut().gossipsub.publish(
							gossipsub::IdentTopic::new(BLOCK_PROPOSER_TOPIC),
							cluster_block_proposers_bytes,
						) {
							Ok(msg_id) => {
								info!("BLOCK PROPOSER PUBLISHED SUCCESSFULLY");
								let _ = sender.send(Ok(msg_id));
							},
							Err(e) => {
								// error!("FAILED TO PUBLISH BLOCK PROPOSER");
								let _ = sender.send(Err(Box::new(e)));
							},
						}
					},
					Err(e) => {
						error!("FAILED TO SERIALIZE BLOCK PROPOSER TO BYTES");
						let _ = sender.send(Err(e.into()));
					},
				},
			Command::BroadcastVote { vote, sender } => match vote.as_bytes() {
				Ok(cluster_vote_bytes) => {
					match self
						.swarm
						.behaviour_mut()
						.gossipsub
						.publish(gossipsub::IdentTopic::new(VOTE_TOPIC), cluster_vote_bytes)
					{
						Ok(msg_id) => {
							info!("VOTE PUBLISHED SUCCESSFULLY");
							let _ = sender.send(Ok(msg_id));
						},
						Err(e) => {
							// error!("FAILED TO PUBLISH VOTE");
							let _ = sender.send(Err(Box::new(e)));
						},
					}
				},
				Err(e) => {
					error!("FAILED TO SERIALIZE VOTE TO BYTES");
					let _ = sender.send(Err(e.into()));
				},
			},
			Command::BroadcastVoteResult { vote_result, sender } => match vote_result.as_bytes() {
				Ok(cluster_vote_result_bytes) => {
					match self.swarm.behaviour_mut().gossipsub.publish(
						gossipsub::IdentTopic::new(VOTE_RESULT_TOPIC),
						cluster_vote_result_bytes,
					) {
						Ok(msg_id) => {
							info!("VOTE RESULT PUBLISHED SUCCESSFULLY");
							let _ = sender.send(Ok(msg_id));
						},
						Err(e) => {
							// error!("FAILED TO PUBLISH VOTE RESULT");
							let _ = sender.send(Err(Box::new(e)));
						},
					}
				},
				Err(e) => {
					error!("FAILED TO SERIALIZE VOTE RESULT TO BYTES");
					let _ = sender.send(Err(e.into()));
				},
			},
			Command::TopicSubscribe { topic_string, sender } => {
				let topic = gossipsub::IdentTopic::new(topic_string.clone());
				match self.swarm.behaviour_mut().gossipsub.subscribe(&topic) {
					Ok(_) => {
						let _ = sender.send(Ok(()));
					},
					Err(e) => {
						error!("Failed to subscribe to topic {topic_string:?}: {e:?}");
						let _ = sender.send(Err(Box::new(e)));
					},
				}
			},
			Command::TopicUnsubscribe { topic_string, sender } => {
				let topic = gossipsub::IdentTopic::new(topic_string.clone());
				match self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic) {
					Ok(_) => {
						let _ = sender.send(Ok(()));
					},
					Err(e) => {
						error!("Failed to unsubscribe from topic {topic_string:?}: {e:?}");
						let _ = sender.send(Err(Box::new(e)));
					},
				}
			},
		}
	}
}

/// Our network behaviour.
#[derive(NetworkBehaviour)]
//#[behaviour(to_swarm = "BehaviourEvent")]
struct Behaviour {
	identify: identify::Behaviour,
	// mdns: mdns::tokio::Behaviour,
	kademlia: Kademlia<MemoryStore>,
	gossipsub: gossipsub::Behaviour,
}

fn kademlia_behaviour(local_peer_id: PeerId) -> Kademlia<MemoryStore> {
	let mut config = KademliaConfig::default();
	config.set_query_timeout(Duration::from_secs(5 * 60));
	let store = MemoryStore::new(local_peer_id);
	Kademlia::with_config(local_peer_id, store, config)
}

fn gossipsub_behaviour(
	local_key: identity::Keypair,
	topics: Vec<gossipsub::IdentTopic>,
) -> Result<gossipsub::Behaviour, Box<dyn Error>> {
	// To content-address message, we can take the hash of message and use it as an ID.
	let message_id_fn = |message: &gossipsub::Message| {
		let mut s = DefaultHasher::new();
		message.data.hash(&mut s);
		gossipsub::MessageId::from(s.finish().to_string())
	};

	let gossipsub_config = gossipsub::ConfigBuilder::default()
		.heartbeat_interval(Duration::from_secs(4))
		.validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
		.message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated. NOTE:
		// Not sure if we want this method or a different method
		.max_transmit_size(100_000_000)
		.build()
		.expect("Valid config");

	let mut gossipsub = gossipsub::Behaviour::new(
		gossipsub::MessageAuthenticity::Signed(local_key),
		gossipsub_config,
	)
	.expect("Correct configuration");

	// Subscribe to the topics
	for topic in topics {
		gossipsub.subscribe(&topic)?;
	}

	Ok(gossipsub)
}

/// Valid commands that can be sent from the Client to the EventLoop
#[derive(Debug)]
pub enum Command {
	StartListening {
		addr: Multiaddr,
		sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
	},
	/// Dial a peer
	Dial {
		peer_id: PeerId,
		peer_addr: Multiaddr,
		sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
	},
	/// Broadcast an CIPHER transaction to the network
	BroadcastNodeInfo {
		node_info: NodeInfo,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast an CIPHER transaction to the network
	BroadcastTransaction {
		transaction: Transaction,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast a proposed block_payload to the network for validation
	BroadcastValidateBlock {
		block_payload: BlockPayload,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast a validated block_payload to the network
	BroadcastBlock {
		block_payload: BlockPayload,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast a block_payload header to the network
	BroadcastBlockHeader {
		block_header_payload: BlockHeaderPayload,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast a block_payload header to the network
	BroadcastBlockProposer {
		block_proposer_payload: BlockProposerPayload,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast a vote_payload to the network
	BroadcastVote {
		vote: Vote,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Broadcast a vote_payload to the network
	BroadcastVoteResult {
		vote_result: VoteResult,
		sender: oneshot::Sender<Result<MessageId, Box<dyn Error + Send>>>,
	},
	/// Subscribe to a gossipsub topic
	TopicSubscribe {
		topic_string: String,
		sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
	},
	/// Unsubscribe from a gossipsub topic
	TopicUnsubscribe {
		topic_string: String,
		sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
	},
}

/// Events that are supported to send to the event_receiver for processing
/// ex: Receiving a new transaction from a peer node so it is sent to the event_receiver
/// where it is then validated and added to the mempool.
#[derive(Debug)]
pub enum Event {
	InboundNodeInfo { node_info: NodeInfo },
	InboundTransaction { transaction: Transaction },
	InboundValidateBlock { block_payload: BlockPayload },
	InboundBlock { block_payload: BlockPayload },
	InboundBlockHeader { block_header_payload: BlockHeaderPayload },
	InboundBlockProposer { block_proposer_payload: BlockProposerPayload },
	InboundVote { vote: Vote },
	InboundVoteResult { vote_result: VoteResult },
}

/// Given a human-readable topic name, return the topic hash
fn get_topic_hash(topic_str: &str) -> gossipsub::IdentTopic {
	gossipsub::IdentTopic::new(topic_str)
}
