use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use asynchronous_codec::FramedRead;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use client::SendingState;
use futures::{stream::SelectAll, StreamExt};
use libp2p::{
    core::{upgrade::ReadyUpgrade, Endpoint},
    swarm::{
        handler::ConnectionEvent, ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent,
        ConnectionId, FromSwarm, NetworkBehaviour, StreamProtocol, SubstreamProtocol,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};

mod cid_prefix;
mod client;
mod message;
mod proto;
#[cfg(test)]
mod test_utils;
mod utils;
mod wantlist;

use crate::client::{ClientBehaviour, ClientConnectionHandler};
use crate::message::Codec;
use crate::proto::message::mod_Message::Wantlist as ProtoWantlist;
use crate::proto::message::Message;
use crate::utils::stream_protocol;

pub use crate::client::{ClientConfig, QueryId};

#[derive(Debug)]
pub struct BitswapBehaviour<const MAX_MULTIHASH_SIZE: usize, B>
where
    B: Blockstore + Send + Sync,
{
    protocol: StreamProtocol,
    client: ClientBehaviour<MAX_MULTIHASH_SIZE, B>,
}

#[derive(Debug)]
pub enum BitswapEvent {
    GetQueryResponse {
        query_id: QueryId,
        data: Vec<u8>,
    },
    GetQueryError {
        query_id: QueryId,
        error: BitswapError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum BitswapError {
    #[error("Invalid multihash size")]
    InvalidMultihashSize,

    #[error("Invalid protocol prefix")]
    InvalidProtocolPrefix,

    #[error("Blockstore error: {0}")]
    Blockstore(#[from] BlockstoreError),
}

pub type Result<T, E = BitswapError> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct BitswapConfig<B>
where
    B: Blockstore + Send + Sync,
{
    /// The prefix that will be added on `/ipfs/bitswap/1.2.0`. It must start with `/`.
    pub protocol_prefix: Option<String>,
    pub store: B,
    pub client: ClientConfig,
}

impl<const MAX_MULTIHASH_SIZE: usize, B> BitswapBehaviour<MAX_MULTIHASH_SIZE, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    pub fn new(config: BitswapConfig<B>) -> Result<Self> {
        let store = Arc::new(config.store);
        let protocol_prefix = config.protocol_prefix.as_deref();
        let protocol = stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")?;

        Ok(BitswapBehaviour {
            protocol: protocol.clone(),
            client: ClientBehaviour::new(config.client, store, protocol_prefix)?,
        })
    }

    pub fn get<const S: usize>(&mut self, cid: &CidGeneric<S>) -> QueryId {
        self.client.get(cid)
    }

    pub fn cancel(&mut self, query_id: QueryId) {
        self.client.cancel(query_id)
    }
}

impl<const MAX_MULTIHASH_SIZE: usize, B> NetworkBehaviour
    for BitswapBehaviour<MAX_MULTIHASH_SIZE, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    type ConnectionHandler = Handler<MAX_MULTIHASH_SIZE>;
    type ToSwarm = BitswapEvent;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<Self::ConnectionHandler, ConnectionDenied> {
        Ok(Handler {
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
        Ok(Handler {
            peer,
            protocol: self.protocol.clone(),
            client_handler: self.client.new_connection_handler(peer),
            incoming_streams: SelectAll::new(),
        })
    }

    fn on_swarm_event(&mut self, _event: FromSwarm) {}

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
pub enum ToBehaviourEvent<const S: usize> {
    IncomingMessage(PeerId, Message),
}

#[derive(Debug)]
#[doc(hidden)]
pub enum ToHandlerEvent {
    SendWantlist(ProtoWantlist, Arc<Mutex<SendingState>>),
}

#[derive(Debug)]
pub struct Handler<const MAX_MULTIHASH_SIZE: usize> {
    peer: PeerId,
    protocol: StreamProtocol,
    client_handler: ClientConnectionHandler<MAX_MULTIHASH_SIZE>,
    incoming_streams: SelectAll<StreamFramedRead>,
}

impl<const MAX_MULTIHASH_SIZE: usize> ConnectionHandler for Handler<MAX_MULTIHASH_SIZE> {
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
struct StreamFramedRead(FramedRead<libp2p::Stream, Codec>);

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
