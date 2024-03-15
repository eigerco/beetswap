use std::collections::{hash_map, VecDeque};
use std::mem::take;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use std::{fmt, mem};

use asynchronous_codec::FramedWrite;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use fnv::{FnvHashMap, FnvHashSet};
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::task::AtomicWaker;
use futures::{FutureExt, SinkExt, StreamExt};
use futures_timer::Delay;
use instant::Instant;
use libp2p_core::upgrade::ReadyUpgrade;
use libp2p_identity::PeerId;
use libp2p_swarm::{
    ConnectionHandlerEvent, ConnectionId, NotifyHandler, StreamProtocol, SubstreamProtocol, ToSwarm,
};
use smallvec::SmallVec;
use tracing::warn;

use crate::incoming_stream::ClientMessage;
use crate::message::Codec;
use crate::proto::message::mod_Message::{BlockPresenceType, Wantlist as ProtoWantlist};
use crate::proto::message::Message;
use crate::utils::{convert_cid, stream_protocol};
use crate::wantlist::{Wantlist, WantlistState};
use crate::{ConnHandlerEvent, StreamRequester};
use crate::{Error, Event, Result, ToBehaviourEvent, ToHandlerEvent};

const SEND_FULL_INTERVAL: Duration = Duration::from_secs(30);
const RECEIVE_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const START_SENDING_TIMEOUT: Duration = Duration::from_secs(5);

/// ID of an ongoing query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(u64);

#[derive(Debug)]
pub struct ClientConfig {
    pub set_send_dont_have: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            set_send_dont_have: true,
        }
    }
}

enum TaskResult<const S: usize> {
    Get(
        QueryId,
        CidGeneric<S>,
        Result<Option<Vec<u8>>, BlockstoreError>,
    ),
    Set(Result<Vec<(CidGeneric<S>, Vec<u8>)>, BlockstoreError>),
    Cancelled,
}

#[derive(Debug)]
pub(crate) struct ClientBehaviour<const S: usize, B>
where
    B: Blockstore + Send + Sync,
{
    store: Arc<B>,
    protocol: StreamProtocol,
    queue: VecDeque<ToSwarm<Event, ToHandlerEvent>>,
    wantlist: Wantlist<S>,
    peers: FnvHashMap<PeerId, PeerState<S>>,
    cid_to_queries: FnvHashMap<CidGeneric<S>, SmallVec<[QueryId; 1]>>,
    tasks: FuturesUnordered<BoxFuture<'static, TaskResult<S>>>,
    query_abort_handle: FnvHashMap<QueryId, AbortHandle>,
    next_query_id: u64,
    waker: Arc<AtomicWaker>,
    send_full_timer: Delay,
    new_blocks: Vec<(CidGeneric<S>, Vec<u8>)>,
}

#[derive(Debug)]
struct PeerState<const S: usize> {
    established_connections: FnvHashSet<ConnectionId>,
    sending_state: SendingState,
    wantlist: WantlistState<S>,
    send_full: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum SendingState {
    Ready,
    Requested(Instant, ConnectionId),
    RequestReceived(Instant, ConnectionId),
    Sending(Instant, ConnectionId),
    Failed(ConnectionId),
}

impl<const S: usize, B> ClientBehaviour<S, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    pub(crate) fn new(config: ClientConfig, store: Arc<B>, protocol_prefix: Option<&str>) -> Self {
        let protocol = stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")
            .expect("prefix checked by beetswap::BehaviourBuilder::protocol_prefix");
        let set_send_dont_have = config.set_send_dont_have;

        ClientBehaviour {
            store,
            protocol,
            queue: VecDeque::new(),
            wantlist: Wantlist::new(set_send_dont_have),
            peers: FnvHashMap::default(),
            cid_to_queries: FnvHashMap::default(),
            tasks: FuturesUnordered::new(),
            query_abort_handle: FnvHashMap::default(),
            next_query_id: 0,
            waker: Arc::new(AtomicWaker::new()),
            send_full_timer: Delay::new(SEND_FULL_INTERVAL),
            new_blocks: Vec::new(),
        }
    }

    pub(crate) fn new_connection_handler(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
    ) -> ClientConnectionHandler<S> {
        let peer = self.peers.entry(peer_id).or_insert_with(|| PeerState {
            established_connections: FnvHashSet::default(),
            sending_state: SendingState::Ready,
            wantlist: WantlistState::new(),
            send_full: true,
        });

        peer.established_connections.insert(connection_id);

        ClientConnectionHandler {
            peer_id,
            connection_id,
            queue: VecDeque::new(),
            protocol: self.protocol.clone(),
            msg: None,
            sink_state: SinkState::None,
            sending_state: SendingState::Ready,
            closing: false,
        }
    }

    pub(crate) fn on_connection_closed(&mut self, peer: PeerId, connection_id: ConnectionId) {
        if let hash_map::Entry::Occupied(mut entry) = self.peers.entry(peer) {
            entry
                .get_mut()
                .established_connections
                .remove(&connection_id);

            if entry.get().established_connections.is_empty() {
                entry.remove();
            }
        }
    }

    fn next_query_id(&mut self) -> QueryId {
        let id = QueryId(self.next_query_id);
        self.next_query_id += 1;
        id
    }

    /// Schedule a `Blockstore::get` for the specified cid
    fn schedule_store_get(&mut self, query_id: QueryId, cid: CidGeneric<S>) {
        let store = self.store.clone();
        let (handle, reg) = AbortHandle::new_pair();

        // Try to asynchronously get the CID from the store..
        self.tasks.push(
            async move {
                match Abortable::new(store.get(&cid), reg).await {
                    // ..And continue the procedure in `poll`. Missing CID will be handled there.
                    Ok(res) => TaskResult::Get(query_id, cid, res),
                    Err(_) => TaskResult::Cancelled,
                }
            }
            .boxed(),
        );

        self.query_abort_handle.insert(query_id, handle);
    }

    /// Schedule a `Blockstore::put_many_keyed` for the specified blocks
    fn schedule_store_put_many(&mut self, blocks: Vec<(CidGeneric<S>, Vec<u8>)>) {
        let store = self.store.clone();

        self.tasks.push(
            async move {
                let res = store
                    .put_many_keyed(blocks.clone().into_iter())
                    .await
                    .map(|_| blocks);
                TaskResult::Set(res)
            }
            .boxed(),
        );
    }

    pub(crate) fn get<const CS: usize>(&mut self, cid: &CidGeneric<CS>) -> QueryId {
        let query_id = self.next_query_id();

        match convert_cid(cid) {
            // Schedule an asynchronous get from the blockstore. The result will be provided
            // from `poll` and if CID is missing `poll` will query the network.
            Some(cid) => self.schedule_store_get(query_id, cid),
            // If CID conversion fails, an event with the error will be given to
            // the requestor on the next `poll`.
            None => {
                self.queue
                    .push_back(ToSwarm::GenerateEvent(Event::GetQueryError {
                        query_id,
                        error: Error::InvalidMultihashSize,
                    }));
            }
        }

        query_id
    }

    pub(crate) fn cancel(&mut self, query_id: QueryId) {
        if let Some(abort_handle) = self.query_abort_handle.remove(&query_id) {
            abort_handle.abort();
        }

        for (cid, queries) in self.cid_to_queries.iter_mut() {
            if let Some(pos) = queries.iter().position(|id| *id == query_id) {
                queries.swap_remove(pos);

                // If CID doesn't have any other queries requesting it, remove it completely.
                // Cancel message will be send to the servers from `poll`.
                if queries.is_empty() {
                    // Cancelling message will be generated from `poll` method
                    let cid = cid.to_owned();
                    self.cid_to_queries.remove(&cid);
                    self.wantlist.remove(&cid);
                }

                break;
            }
        }
    }

    pub(crate) fn process_incoming_message(&mut self, peer: PeerId, msg: ClientMessage<S>) {
        let Some(peer_state) = self.peers.get_mut(&peer) else {
            return;
        };

        let mut new_blocks = Vec::new();

        // Update presence
        for (cid, block_presence) in msg.block_presences {
            match block_presence {
                BlockPresenceType::Have => peer_state.wantlist.got_have(&cid),
                BlockPresenceType::DontHave => peer_state.wantlist.got_dont_have(&cid),
            }
        }

        // TODO: If someone sends a huge message, the executor will block! We need to
        // truncate the data, maybe even in the `message::Codec` level
        for (cid, block) in msg.blocks {
            if !self.wantlist.remove(&cid) {
                debug_assert!(!self.cid_to_queries.contains_key(&cid));
                continue;
            }

            peer_state.wantlist.got_block(&cid);
            new_blocks.push((cid, block.clone()));

            // Inform the upper layer for the result
            if let Some(queries) = self.cid_to_queries.remove(&cid) {
                for query_id in queries {
                    self.queue
                        .push_back(ToSwarm::GenerateEvent(Event::GetQueryResponse {
                            query_id,
                            data: block.clone(),
                        }));
                }
            }
        }

        // Store them in blockstore
        if !new_blocks.is_empty() {
            self.schedule_store_put_many(new_blocks);
        }
    }

    pub(crate) fn sending_state_changed(&mut self, peer_id: PeerId, state: SendingState) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.sending_state = state;
        }
    }

    fn update_handlers(&mut self) -> bool {
        let mut handler_updated = false;
        let mut to_be_removed = SmallVec::<[PeerId; 8]>::new();

        for (peer, state) in self.peers.iter_mut() {
            // Clear out bad connections. In case of a bad connection we
            // must send the full wantlist because we don't know what
            // the remote peer has received.
            match state.sending_state {
                SendingState::Ready => {
                    // Allowed to send
                }
                SendingState::Requested(instant, connection_id) => {
                    if instant.elapsed() < RECEIVE_REQUEST_TIMEOUT {
                        // Sending in progress
                        continue;
                    }
                    // Bad connection
                    state.established_connections.remove(&connection_id);
                    state.send_full = true;
                }
                SendingState::RequestReceived(instant, connection_id) => {
                    if instant.elapsed() < START_SENDING_TIMEOUT {
                        // Sending in progress
                        continue;
                    }
                    // Bad connection
                    state.established_connections.remove(&connection_id);
                    state.send_full = true;
                    // TODO: Stop ongoing request
                }
                SendingState::Sending(..) => {
                    // Sending in progress
                    continue;
                }
                SendingState::Failed(connection_id) => {
                    // Bad connection
                    state.established_connections.remove(&connection_id);
                    state.send_full = true;
                }
            };

            let Some(connection_id) = state.established_connections.iter().next().copied() else {
                to_be_removed.push(*peer);
                continue;
            };

            let wantlist = if state.send_full {
                state.wantlist.generate_proto_full(&self.wantlist)
            } else {
                state.wantlist.generate_proto_update(&self.wantlist)
            };

            // Allow empty entries to be sent when send_full flag is set.
            if state.send_full {
                // Reset flag
                state.send_full = false;
            } else if wantlist.entries.is_empty() {
                // No updates to be sent for this peer
                continue;
            }

            self.queue.push_back(ToSwarm::NotifyHandler {
                peer_id: peer.to_owned(),
                handler: NotifyHandler::One(connection_id),
                event: ToHandlerEvent::SendWantlist(wantlist),
            });

            state.sending_state = SendingState::Requested(Instant::now(), connection_id);
            handler_updated = true;
        }

        for peer in to_be_removed {
            self.peers.remove(&peer);
        }

        // This is true if at least one handler is updated
        handler_updated
    }

    pub(crate) fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Event, ToHandlerEvent>> {
        // Update waker
        self.waker.register(cx.waker());

        loop {
            if let Some(ev) = self.queue.pop_front() {
                return Poll::Ready(ev);
            }

            if self.send_full_timer.poll_unpin(cx).is_ready() {
                for state in self.peers.values_mut() {
                    state.send_full = true;
                }

                // Reset timer and loop again to get it registered
                self.send_full_timer.reset(SEND_FULL_INTERVAL);
                continue;
            }

            if let Poll::Ready(Some(task_result)) = self.tasks.poll_next_unpin(cx) {
                match task_result {
                    // Blockstore already has the data so return them to the user
                    TaskResult::Get(query_id, _, Ok(Some(data))) => {
                        return Poll::Ready(ToSwarm::GenerateEvent(Event::GetQueryResponse {
                            query_id,
                            data: data.clone(),
                        }));
                    }

                    // If blockstore doesn't have the data, add CID in the wantlist.
                    //
                    // Connection handlers will be informed via `update_handlers` about the new items in wantlist.
                    TaskResult::Get(query_id, cid, Ok(None)) => {
                        self.wantlist.insert(cid);
                        self.cid_to_queries.entry(cid).or_default().push(query_id);
                    }

                    // Blockstore error
                    TaskResult::Get(query_id, _, Err(e)) => {
                        return Poll::Ready(ToSwarm::GenerateEvent(Event::GetQueryError {
                            query_id,
                            error: e.into(),
                        }));
                    }

                    TaskResult::Set(Ok(blocks)) => {
                        self.new_blocks.extend(blocks);
                    }

                    // TODO: log it
                    TaskResult::Set(Err(_e)) => {}

                    // Nothing to do
                    TaskResult::Cancelled => {}
                }

                // If we didn't return an event, we need to retry the whole loop
                continue;
            }

            if self.update_handlers() {
                // New events generated, loop again to send them.
                continue;
            }

            return Poll::Pending;
        }
    }

    pub(crate) fn get_new_blocks(&mut self) -> Vec<(CidGeneric<S>, Vec<u8>)> {
        take(&mut self.new_blocks)
    }
}

pub(crate) struct ClientConnectionHandler<const S: usize> {
    peer_id: PeerId,
    connection_id: ConnectionId,
    queue: VecDeque<ToBehaviourEvent<S>>,
    protocol: StreamProtocol,
    msg: Option<Message>,
    sink_state: SinkState,
    sending_state: SendingState,
    closing: bool,
}

enum SinkState {
    None,
    Requested,
    Ready(FramedWrite<libp2p_swarm::Stream, Codec>),
}

impl<const S: usize> ClientConnectionHandler<S> {
    pub(crate) fn set_stream(&mut self, stream: libp2p_swarm::Stream) {
        // Convert `AsyncWrite` stream to `Sink`
        self.sink_state = SinkState::Ready(FramedWrite::new(stream, Codec));
    }

    pub(crate) fn stream_allocation_failed(&mut self) {
        debug_assert!(matches!(self.sink_state, SinkState::Requested));
        // Reset state to force a new allocation in `poll`
        self.sink_state = SinkState::None;
    }

    pub(crate) fn send_wantlist(&mut self, wantlist: ProtoWantlist) {
        debug_assert!(self.msg.is_none());
        debug_assert!(matches!(self.sending_state, SendingState::Ready));

        self.msg = Some(Message {
            wantlist: Some(wantlist),
            ..Message::default()
        });

        self.change_sending_state(SendingState::RequestReceived(
            Instant::now(),
            self.connection_id,
        ));
    }

    fn change_sending_state(&mut self, state: SendingState) {
        if self.sending_state != state {
            self.sending_state = state;
            self.queue
                .push_back(ToBehaviourEvent::SendingStateChanged(self.peer_id, state));
        }
    }

    fn open_new_substream(
        &mut self,
    ) -> Poll<
        ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, StreamRequester, ToBehaviourEvent<S>>,
    > {
        self.sink_state = SinkState::Requested;

        Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
            protocol: SubstreamProtocol::new(
                ReadyUpgrade::new(self.protocol.clone()),
                StreamRequester::Client,
            ),
        })
    }

    pub(crate) fn poll_close(&mut self, cx: &mut Context) -> Poll<Option<ToBehaviourEvent<S>>> {
        if !self.closing {
            self.closing = true;
            self.msg.take();

            if let SinkState::Ready(mut sink) = mem::replace(&mut self.sink_state, SinkState::None)
            {
                let _ = sink.poll_close_unpin(cx);
            }

            // If sending is in progress, then we don't know how much data the other end received
            // so we consider this as "failed".
            if matches!(
                self.sending_state,
                SendingState::RequestReceived(..) | SendingState::Sending(..)
            ) {
                self.change_sending_state(SendingState::Failed(self.connection_id));
            }

            self.queue
                .push_back(ToBehaviourEvent::ClientClosingConnection(
                    self.peer_id,
                    self.connection_id,
                ));
        }

        Poll::Ready(self.queue.pop_front())
    }

    fn close_sink_on_error(&mut self, location: &str) {
        warn!("sink operation failed, closing: {location}");
        self.sink_state = SinkState::None;
    }

    pub(crate) fn poll(&mut self, cx: &mut Context) -> Poll<ConnHandlerEvent<S>> {
        loop {
            if let Some(ev) = self.queue.pop_front() {
                return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(ev));
            }

            match (&mut self.msg, &mut self.sink_state) {
                (None, SinkState::None) => return Poll::Pending,
                (Some(_), SinkState::None) => return self.open_new_substream(),
                (_, SinkState::Requested) => return Poll::Pending,
                (None, SinkState::Ready(sink)) => {
                    if ready!(sink.poll_flush_unpin(cx)).is_err() {
                        self.close_sink_on_error("poll_flush_unpin");
                        self.change_sending_state(SendingState::Failed(self.connection_id));
                        continue;
                    }

                    let _ = sink.poll_close_unpin(cx);
                    self.sink_state = SinkState::None;
                    self.change_sending_state(SendingState::Ready);
                }
                (msg @ Some(_), SinkState::Ready(sink)) => {
                    if ready!(sink.poll_ready_unpin(cx)).is_err() {
                        self.close_sink_on_error("poll_ready_unpin");
                        continue;
                    }

                    let msg = msg.take().expect("msg is always Some here");

                    if sink.start_send_unpin(&msg).is_err() {
                        self.msg = Some(msg);
                        self.close_sink_on_error("start_send_unpin");
                        continue;
                    }

                    self.change_sending_state(SendingState::Sending(
                        Instant::now(),
                        self.connection_id,
                    ));

                    // Loop again, so `poll_flush` will be called and register a waker.
                }
            }
        }
    }
}

impl<const S: usize> fmt::Debug for ClientConnectionHandler<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientConnectionHandler")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::message::mod_Message::mod_Wantlist::WantType;
    use crate::test_utils::{cid_of_data, poll_fn_once};
    use blockstore::InMemoryBlockstore;
    use std::future::poll_fn;

    #[tokio::test]
    async fn get_known_cid() {
        let mut client = new_client().await;

        let cid1 = cid_of_data(b"1");
        let query_id1 = client.get(&cid1);

        let cid2 = cid_of_data(b"2");
        let query_id2 = client.get(&cid2);

        for _ in 0..2 {
            let ev = poll_fn(|cx| client.poll(cx)).await;

            match ev {
                ToSwarm::GenerateEvent(Event::GetQueryResponse { query_id, data }) => {
                    if query_id == query_id1 {
                        assert_eq!(data, b"1");
                    } else if query_id == query_id2 {
                        assert_eq!(data, b"2");
                    } else {
                        unreachable!()
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn get_unknown_cid_responds_with_have() {
        let mut client = new_client().await;

        let peer1 = PeerId::random();
        let mut conn1 = client.new_connection_handler(peer1);

        let peer2 = PeerId::random();
        let mut _conn2 = client.new_connection_handler(peer2);

        let cid1 = cid_of_data(b"x1");
        let _query_id1 = client.get(&cid1);

        // Wantlist will be generated for both peers
        for _ in 0..2 {
            // wantlist with Have request will be generated
            let ev = poll_fn(|cx| client.poll(cx)).await;
            let (peer_id, wantlist) = expect_send_wantlist_event(ev);

            assert_eq!(wantlist.entries.len(), 1);
            assert!(wantlist.full);

            let entry = &wantlist.entries[0];
            assert_eq!(entry.block, cid1.to_bytes());
            assert!(!entry.cancel);
            assert_eq!(entry.wantType, WantType::Have);
            assert!(entry.sendDontHave);

            if peer_id == peer1 {}

            // Mark send state as ready
            *send_state.lock().unwrap() = SendingState::Ready;
        }

        // Simulate that peer1 responsed with Have
        let mut client_msg = ClientMessage::default();
        client_msg
            .block_presences
            .insert(cid1, BlockPresenceType::Have);
        client.process_incoming_message(peer1, client_msg);

        // wantlist with Block request will be generated
        let ev = poll_fn(|cx| client.poll(cx)).await;
        let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

        assert_eq!(peer_id, peer1);
        assert_eq!(wantlist.entries.len(), 1);
        assert!(!wantlist.full);

        let entry = &wantlist.entries[0];
        assert_eq!(entry.block, cid1.to_bytes());
        assert!(!entry.cancel);
        assert_eq!(entry.wantType, WantType::Block);
        assert!(entry.sendDontHave);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;
    }

    #[tokio::test]
    async fn get_unknown_cid_responds_with_dont_have() {
        let mut client = new_client().await;

        let peer1 = PeerId::random();
        let mut _conn1 = client.new_connection_handler(peer1);

        let peer2 = PeerId::random();
        let mut _conn2 = client.new_connection_handler(peer2);

        let cid1 = cid_of_data(b"x1");
        let _query_id1 = client.get(&cid1);

        // Wantlist will be generated for both peers
        for _ in 0..2 {
            // wantlist with Have request will be generated
            let ev = poll_fn(|cx| client.poll(cx)).await;
            let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

            assert!(peer_id == peer1 || peer_id == peer2);
            assert_eq!(wantlist.entries.len(), 1);
            assert!(wantlist.full);

            let entry = &wantlist.entries[0];
            assert_eq!(entry.block, cid1.to_bytes());
            assert!(!entry.cancel);
            assert_eq!(entry.wantType, WantType::Have);
            assert!(entry.sendDontHave);

            // Mark send state as ready
            *send_state.lock().unwrap() = SendingState::Ready;
        }

        // Simulate that peer1 responsed with DontHave
        let mut client_msg = ClientMessage::default();
        client_msg
            .block_presences
            .insert(cid1, BlockPresenceType::DontHave);
        client.process_incoming_message(peer1, client_msg);

        // Simulate that full wantlist is needed
        for peer_state in client.peers.values_mut() {
            peer_state.send_full = true;
        }

        for _ in 0..2 {
            let ev = poll_fn(|cx| client.poll(cx)).await;
            let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

            if peer_id == peer1 {
                // full wantlist of peer1 will be empty because it alreayd replied with DontHave
                assert!(wantlist.entries.is_empty());
                assert!(wantlist.full);
            } else if peer_id == peer2 {
                assert_eq!(wantlist.entries.len(), 1);
                assert!(wantlist.full);

                let entry = &wantlist.entries[0];
                assert_eq!(entry.block, cid1.to_bytes());
                assert!(!entry.cancel);
                assert_eq!(entry.wantType, WantType::Have);
                assert!(entry.sendDontHave);
            } else {
                panic!("Unknown peer id");
            }

            // Mark send state as ready
            *send_state.lock().unwrap() = SendingState::Ready;
        }

        // No other events should be produced
        assert!(dbg!(poll_fn_once(|cx| client.poll(cx)).await).is_none());
    }

    #[tokio::test]
    async fn get_unknown_cid_responds_with_block() {
        let mut client = new_client().await;

        let peer = PeerId::random();
        let mut _conn = client.new_connection_handler(peer);

        let cid1 = cid_of_data(b"x1");
        let query_id1 = client.get(&cid1);

        // wantlist with Have request will be generated
        let ev = poll_fn(|cx| client.poll(cx)).await;
        let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

        assert_eq!(peer_id, peer);
        assert_eq!(wantlist.entries.len(), 1);
        assert!(wantlist.full);

        let entry = &wantlist.entries[0];
        assert_eq!(entry.block, cid1.to_bytes());
        assert!(!entry.cancel);
        assert_eq!(entry.wantType, WantType::Have);
        assert!(entry.sendDontHave);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;

        // Simulate that peer responsed with a block
        let mut client_msg = ClientMessage::default();
        client_msg.blocks.insert(cid1, b"x1".to_vec());
        client.process_incoming_message(peer, client_msg);

        // Receive an event with the found data
        let ev = poll_fn(|cx| client.poll(cx)).await;

        let (query_id, data) = match ev {
            ToSwarm::GenerateEvent(Event::GetQueryResponse { query_id, data }) => (query_id, data),
            _ => unreachable!(),
        };

        assert_eq!(query_id, query_id1);
        assert_eq!(data, b"x1");

        // Poll once more for the store to be updated. This does not produce an event.
        poll_fn_once(|cx| client.poll(cx)).await;
        assert_eq!(client.store.get(&cid1).await.unwrap().unwrap(), b"x1");
    }

    #[tokio::test]
    async fn full_wantlist_then_update() {
        let mut client = new_client().await;

        let peer = PeerId::random();
        let mut _conn = client.new_connection_handler(peer);

        let cid1 = cid_of_data(b"x1");
        let _query_id1 = client.get(&cid1);

        let cid2 = cid_of_data(b"x2");
        let _query_id2 = client.get(&cid2);

        let ev = poll_fn(|cx| client.poll(cx)).await;

        let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

        assert_eq!(peer_id, peer);
        assert_eq!(wantlist.entries.len(), 2);
        assert!(wantlist.full);

        let entry1 = wantlist
            .entries
            .iter()
            .find(|item| item.block == cid1.to_bytes())
            .unwrap();
        assert!(!entry1.cancel);
        assert_eq!(entry1.wantType, WantType::Have);
        assert!(entry1.sendDontHave);

        let entry2 = wantlist
            .entries
            .iter()
            .find(|item| item.block == cid2.to_bytes())
            .unwrap();
        assert!(!entry2.cancel);
        assert_eq!(entry2.wantType, WantType::Have);
        assert!(entry2.sendDontHave);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;

        let cid3 = cid_of_data(b"x3");
        let _query_id3 = client.get(&cid3);

        let ev = poll_fn(|cx| client.poll(cx)).await;
        let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

        assert_eq!(peer_id, peer);
        assert_eq!(wantlist.entries.len(), 1);
        assert!(!wantlist.full);

        let entry = &wantlist.entries[0];
        assert_eq!(entry.block, cid3.to_bytes());
        assert!(!entry.cancel);
        assert_eq!(entry.wantType, WantType::Have);
        assert!(entry.sendDontHave);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;
    }

    #[tokio::test]
    async fn request_then_cancel() {
        let mut client = new_client().await;

        let peer = PeerId::random();
        let mut _conn = client.new_connection_handler(peer);

        let cid1 = cid_of_data(b"x1");
        let query_id1 = client.get(&cid1);

        let cid2 = cid_of_data(b"x2");
        let query_id2 = client.get(&cid2);

        // This cancel will not generate any messages because request was not send yet
        client.cancel(query_id2);

        // wantlist with Have request will be generated
        let ev = poll_fn(|cx| client.poll(cx)).await;
        let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

        assert_eq!(peer_id, peer);
        assert_eq!(wantlist.entries.len(), 1);
        assert!(wantlist.full);

        let entry = &wantlist.entries[0];
        assert_eq!(entry.block, cid1.to_bytes());
        assert!(!entry.cancel);
        assert_eq!(entry.wantType, WantType::Have);
        assert!(entry.sendDontHave);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;

        // This cancel should produce a message for cancelling the request
        client.cancel(query_id1);

        // wantlist with Cancel request will be generated
        let ev = poll_fn(|cx| client.poll(cx)).await;
        let (peer_id, wantlist, _) = expect_send_wantlist_event(ev);

        assert_eq!(peer_id, peer);
        assert_eq!(wantlist.entries.len(), 1);
        assert!(!wantlist.full);

        let entry = &wantlist.entries[0];
        assert_eq!(entry.block, cid1.to_bytes());
        assert!(entry.cancel);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;
    }

    async fn blockstore() -> Arc<InMemoryBlockstore<64>> {
        let store = Arc::new(InMemoryBlockstore::<64>::new());

        for i in 0..16 {
            let data = format!("{i}").into_bytes();
            let cid = cid_of_data(&data);
            store.put_keyed(&cid, &data).await.unwrap();
        }

        store
    }

    async fn new_client() -> ClientBehaviour<64, InMemoryBlockstore<64>> {
        let store = blockstore().await;
        ClientBehaviour::<64, _>::new(ClientConfig::default(), store, None)
    }

    fn expect_send_wantlist_event(ev: ToSwarm<Event, ToHandlerEvent>) -> (PeerId, ProtoWantlist) {
        match ev {
            ToSwarm::NotifyHandler {
                peer_id,
                event: ToHandlerEvent::SendWantlist(wantlist),
                ..
            } => (peer_id, wantlist),
            ev => panic!("Expecting ToHandlerEvent::SendWantlist, found {ev:?}"),
        }
    }

    //fn expect_sending_state_notified_event(ev: ToSwarm<Event, ToHandlerEvent>) ->
}
