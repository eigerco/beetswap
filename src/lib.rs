use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use asynchronous_codec::FramedRead;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use client::SendingState;
use futures::{stream::SelectAll, StreamExt};
use libp2p_core::{multiaddr::Multiaddr, upgrade::ReadyUpgrade, Endpoint};
use libp2p_identity::PeerId;
use libp2p_swarm::{
    handler::ConnectionEvent, ConnectionClosed, ConnectionDenied, ConnectionHandler,
    ConnectionHandlerEvent, ConnectionId, FromSwarm, NetworkBehaviour, StreamProtocol,
    SubstreamProtocol, THandlerInEvent, THandlerOutEvent, ToSwarm,
};

mod builder;
mod cid_prefix;
mod client;
mod message;
pub mod multihasher;
mod proto;
#[cfg(test)]
mod test_utils;
pub mod utils;
mod wantlist;

use crate::client::{ClientBehaviour, ClientConnectionHandler};
use crate::message::Codec;
use crate::proto::message::mod_Message::Wantlist as ProtoWantlist;
use crate::proto::message::Message;

pub use crate::builder::BehaviourBuilder;
pub use crate::client::QueryId;

/// [`NetworkBehaviour`] for Bitswap protocol.
#[derive(Debug)]
pub struct Behaviour<const MAX_MULTIHASH_SIZE: usize, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    protocol: StreamProtocol,
    client: ClientBehaviour<MAX_MULTIHASH_SIZE, B>,
}

/// Event produced by [`Behaviour`].
#[derive(Debug)]
pub enum Event {
    GetQueryResponse { query_id: QueryId, data: Vec<u8> },
    GetQueryError { query_id: QueryId, error: Error },
}

/// Representation of all the errors that can occur when interacting with this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid multihash size")]
    InvalidMultihashSize,

    #[error("Invalid protocol prefix: {0}")]
    InvalidProtocolPrefix(String),

    #[error("Blockstore error: {0}")]
    Blockstore(#[from] BlockstoreError),
}

/// Alias for a [`Result`] with the error type [`beetswap::Error`].
///
/// [`beetswap::Error`]: crate::Error
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<const MAX_MULTIHASH_SIZE: usize, B> Behaviour<MAX_MULTIHASH_SIZE, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    /// Creates a new [`Behaviour`] with the default configuration.
    pub fn new(blockstore: B) -> Behaviour<MAX_MULTIHASH_SIZE, B> {
        BehaviourBuilder::new(blockstore).build()
    }

    /// Creates a new [`BehaviourBuilder`].
    pub fn builder(blockstore: B) -> BehaviourBuilder<MAX_MULTIHASH_SIZE, B> {
        BehaviourBuilder::new(blockstore)
    }

    /// Start a query that returns the raw data of a [`Cid`].
    ///
    /// [`Cid`]: cid::CidGeneric
    pub fn get<const S: usize>(&mut self, cid: &CidGeneric<S>) -> QueryId {
        self.client.get(cid)
    }

    /// Cancel an ongoing query.
    pub fn cancel(&mut self, query_id: QueryId) {
        self.client.cancel(query_id)
    }
}

impl<const MAX_MULTIHASH_SIZE: usize, B> NetworkBehaviour for Behaviour<MAX_MULTIHASH_SIZE, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    type ConnectionHandler = ConnHandler<MAX_MULTIHASH_SIZE>;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<Self::ConnectionHandler, ConnectionDenied> {
        Ok(ConnHandler {
            peer,
            protocol: self.protocol.clone(),
            client_handler: self.client.new_connection_handler(peer),
            incoming_streams: SelectAll::new(),
        })
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _addr: &Multiaddr,
        _role_override: Endpoint,
    ) -> Result<Self::ConnectionHandler, ConnectionDenied> {
        Ok(ConnHandler {
            peer,
            protocol: self.protocol.clone(),
            client_handler: self.client.new_connection_handler(peer),
            incoming_streams: SelectAll::new(),
        })
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        #[allow(clippy::single_match)]
        match event {
            FromSwarm::ConnectionClosed(ConnectionClosed { peer_id, .. }) => {
                self.client.on_connection_closed(peer_id);
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            ToBehaviourEvent::IncomingMessage(peer, msg) => {
                self.client.process_incoming_message(peer, &msg);
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        self.client.poll(cx)
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub enum ToBehaviourEvent<const S: usize> {
    IncomingMessage(PeerId, Message),
}

#[derive(Debug)]
#[doc(hidden)]
pub enum ToHandlerEvent {
    SendWantlist(ProtoWantlist, Arc<Mutex<SendingState>>),
}

#[derive(Debug)]
#[doc(hidden)]
pub struct ConnHandler<const MAX_MULTIHASH_SIZE: usize> {
    peer: PeerId,
    protocol: StreamProtocol,
    client_handler: ClientConnectionHandler<MAX_MULTIHASH_SIZE>,
    incoming_streams: SelectAll<StreamFramedRead>,
}

impl<const MAX_MULTIHASH_SIZE: usize> ConnectionHandler for ConnHandler<MAX_MULTIHASH_SIZE> {
    type ToBehaviour = ToBehaviourEvent<MAX_MULTIHASH_SIZE>;
    type FromBehaviour = ToHandlerEvent;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(self.protocol.clone()), ())
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            ToHandlerEvent::SendWantlist(wantlist, state) => {
                self.client_handler.send_wantlist(wantlist, state);
            }
        }
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            '_,
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedOutbound(ev) => {
                if self.client_handler.stream_requested() {
                    self.client_handler.set_stream(ev.protocol);
                }
            }
            ConnectionEvent::FullyNegotiatedInbound(ev) => {
                let stream = StreamFramedRead(FramedRead::new(ev.protocol, Codec));
                self.incoming_streams.push(stream);
            }
            _ => {}
        }
    }

    fn connection_keep_alive(&self) -> bool {
        // TODO
        true
    }

    fn poll_close(&mut self, _cx: &mut Context) -> Poll<Option<Self::ToBehaviour>> {
        Poll::Ready(None)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        if let Poll::Ready(Some(msg)) = self.incoming_streams.poll_next_unpin(cx) {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                ToBehaviourEvent::IncomingMessage(self.peer, msg),
            ));
        }

        if let Poll::Ready(ev) = self.client_handler.poll(cx) {
            return Poll::Ready(ev);
        }

        Poll::Pending
    }
}

/// Wrapper that converts `Option<Result<Message>>` to `Option<Message>`.
///
/// By returning `None` on error, we instruct `SelectAll` to drop the stream.
struct StreamFramedRead(FramedRead<libp2p_swarm::Stream, Codec>);

impl fmt::Debug for StreamFramedRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StreamFramedRead")
    }
}

impl futures::Stream for StreamFramedRead {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        match self.0.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(msg))) => Poll::Ready(Some(msg)),
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
