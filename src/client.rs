use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};

use asynchronous_codec::FramedWrite;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use fnv::FnvHashMap;
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::task::AtomicWaker;
use futures::{FutureExt, SinkExt, StreamExt};
use libp2p::swarm::NotifyHandler;
use libp2p::PeerId;
use libp2p::{
    core::upgrade::ReadyUpgrade,
    swarm::{ConnectionHandlerEvent, SubstreamProtocol, ToSwarm},
    StreamProtocol,
};
use smallvec::SmallVec;
use std::sync::Mutex;

use crate::cid_prefix::CidPrefix;
use crate::message::Codec;
use crate::proto::message::mod_Message::{BlockPresenceType, Wantlist as ProtoWantlist};
use crate::proto::message::Message;
use crate::utils::{convert_cid, stream_protocol};
use crate::wantlist::{Wantlist, WantlistState};
use crate::{BitswapError, BitswapEvent, Result, ToBehaviourEvent, ToHandlerEvent};

const SEND_FULL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    Set(Result<(), BlockstoreError>),
    Cancelled,
}

#[derive(Debug)]
pub(crate) struct ClientBehaviour<const S: usize, B>
where
    B: Blockstore + Send + Sync,
{
    store: Arc<B>,
    protocol: StreamProtocol,
    queue: VecDeque<ToSwarm<BitswapEvent, ToHandlerEvent>>,
    wantlist: Wantlist<S>,
    peers: FnvHashMap<PeerId, PeerState<S>>,
    cid_to_queries: FnvHashMap<CidGeneric<S>, SmallVec<[QueryId; 1]>>,
    tasks: FuturesUnordered<BoxFuture<'static, TaskResult<S>>>,
    query_abort_handle: FnvHashMap<QueryId, AbortHandle>,
    next_query_id: u64,
    waker: Arc<AtomicWaker>,
}

#[derive(Debug)]
struct PeerState<const S: usize> {
    sending: Arc<Mutex<SendingState>>,
    wantlist: WantlistState<S>,
    last_send_full_tm: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum SendingState {
    Ready,
    Sending,
    Poisoned,
}

impl<const S: usize, B> ClientBehaviour<S, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    pub(crate) fn new(
        config: ClientConfig,
        store: Arc<B>,
        protocol_prefix: Option<&str>,
    ) -> Result<Self> {
        let protocol = stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")?;
        let set_send_dont_have = config.set_send_dont_have;

        Ok(ClientBehaviour {
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
        })
    }

    pub(crate) fn new_connection_handler(&mut self, peer: PeerId) -> ClientConnectionHandler<S> {
        self.peers.insert(
            peer,
            PeerState {
                sending: Arc::new(Mutex::new(SendingState::Ready)),
                wantlist: WantlistState::new(),
                last_send_full_tm: None,
            },
        );

        ClientConnectionHandler {
            protocol: self.protocol.clone(),
            stream_requested: false,
            sink: None,
            wantlist: None,
            sending_state: None,
            behaviour_waker: Arc::new(AtomicWaker::new()),
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
                    // ..And continue the procedure in `pool`. Missing CID will be handled there.
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
                let res = store.put_many_keyed(blocks.into_iter()).await;
                TaskResult::Set(res)
            }
            .boxed(),
        );
    }

    pub(crate) fn get<const CS: usize>(&mut self, cid: &CidGeneric<CS>) -> QueryId {
        let query_id = self.next_query_id();

        match convert_cid(cid) {
            Some(cid) => self.schedule_store_get(query_id, cid),
            None => {
                self.queue
                    .push_back(ToSwarm::GenerateEvent(BitswapEvent::GetQueryError {
                        query_id,
                        error: BitswapError::InvalidMultihashSize,
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

                // If CID doesn't have any other queries requesting it, remove it completely
                if queries.is_empty() {
                    // Cancelling message will be generated from `pool` method
                    let cid = cid.to_owned();
                    self.cid_to_queries.remove(&cid);
                    self.wantlist.remove(&cid);
                }

                break;
            }
        }
    }

    pub(crate) fn process_incoming_message(&mut self, peer: PeerId, msg: &Message) {
        let Some(peer_state) = self.peers.get_mut(&peer) else {
            return;
        };

        let mut new_blocks = Vec::new();

        // Update presence
        for block_presense in &msg.blockPresences {
            let Ok(cid) = CidGeneric::try_from(&*block_presense.cid) else {
                continue;
            };

            match block_presense.type_pb {
                BlockPresenceType::Have => peer_state.wantlist.got_have(&cid),
                BlockPresenceType::DontHave => peer_state.wantlist.got_dont_have(&cid),
            }
        }

        // TODO: If someone sends a huge message, the executor will block! We need to
        // truncate the data, maybe even in the `message::Codec` level
        for block in &msg.payload {
            let Some(cid_prefix) = CidPrefix::from_bytes(&block.prefix) else {
                continue;
            };

            let Some(cid) = cid_prefix.to_cid(&block.data) else {
                continue;
            };

            if !self.wantlist.remove(&cid) {
                debug_assert!(!self.cid_to_queries.contains_key(&cid));
                continue;
            }

            peer_state.wantlist.got_block(&cid);
            new_blocks.push((cid.to_owned(), block.data.clone()));

            // Inform the upper layer for the result
            if let Some(queries) = self.cid_to_queries.remove(&cid) {
                for query_id in queries {
                    self.queue
                        .push_back(ToSwarm::GenerateEvent(BitswapEvent::GetQueryResponse {
                            query_id,
                            data: block.data.clone(),
                        }));
                }
            }
        }

        // Store them in blockstore
        if !new_blocks.is_empty() {
            self.schedule_store_put_many(new_blocks);
        }
    }

    fn update_handlers(&mut self) -> bool {
        let mut handler_updated = false;

        for (peer, state) in self.peers.iter_mut() {
            let mut sending_state = state.sending.lock().unwrap();

            // Decide if full list is needed or not.
            let send_full = match &*sending_state {
                SendingState::Sending => {
                    if Arc::strong_count(&state.sending) == 1 {
                        // `Sending` state with strong count of 1 can happen only
                        // when the connection is dropped just before it reads our
                        // event. In this case we treat is with the same way as `Poisoned`
                        // state.
                        true
                    } else {
                        // ClientConnectionHandler will wake us when we can retry
                        continue;
                    }
                }
                SendingState::Ready => match state.last_send_full_tm {
                    // Send full list if interval time is elapsed.
                    Some(tm) => tm.elapsed() >= SEND_FULL_INTERVAL,
                    // Send full list the first time.
                    None => true,
                },
                // State is poisoned, send full list to recover.
                SendingState::Poisoned => true,
            };

            let wantlist = if send_full {
                state.wantlist.generate_proto_full(&self.wantlist)
            } else {
                state.wantlist.generate_proto_update(&self.wantlist)
            };

            if wantlist.entries.is_empty() {
                // Nothing to send
                //
                // TODO: What if the send_full is true? Shouldn't we send it to clear
                // the wantlist? However we should do it once.
                continue;
            }

            if wantlist.full {
                state.last_send_full_tm = Some(Instant::now());
            }

            self.queue.push_back(ToSwarm::NotifyHandler {
                peer_id: peer.to_owned(),
                handler: NotifyHandler::Any,
                event: ToHandlerEvent::SendWantlist(wantlist, state.sending.clone()),
            });

            *sending_state = SendingState::Sending;
            handler_updated = true;
        }

        // This is true if at least one handler is updated
        handler_updated
    }

    pub(crate) fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<BitswapEvent, ToHandlerEvent>> {
        // Update waker
        self.waker.register(cx.waker());

        loop {
            if let Some(ev) = self.queue.pop_front() {
                return Poll::Ready(ev);
            }

            if let Poll::Ready(Some(task_result)) = self.tasks.poll_next_unpin(cx) {
                match task_result {
                    // Blockstore already has the data so return them to the user
                    TaskResult::Get(query_id, _, Ok(Some(data))) => {
                        return Poll::Ready(ToSwarm::GenerateEvent(
                            BitswapEvent::GetQueryResponse {
                                query_id,
                                data: data.clone(),
                            },
                        ));
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
                        return Poll::Ready(ToSwarm::GenerateEvent(BitswapEvent::GetQueryError {
                            query_id,
                            error: e.into(),
                        }));
                    }

                    TaskResult::Set(Ok(_)) => {}

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
}

pub(crate) struct ClientConnectionHandler<const S: usize> {
    protocol: StreamProtocol,
    stream_requested: bool,
    sink: Option<FramedWrite<libp2p::Stream, Codec>>,
    /// Wantlist to be send
    wantlist: Option<ProtoWantlist>,
    /// Sending state of peer.
    ///
    /// Even if we have multiple concurrent connections with the peer, only
    /// one of them will be sending and have this value filled.
    sending_state: Option<Arc<Mutex<SendingState>>>,
    behaviour_waker: Arc<AtomicWaker>,
}

impl<const S: usize> ClientConnectionHandler<S> {
    pub(crate) fn stream_requested(&self) -> bool {
        self.stream_requested
    }

    pub(crate) fn set_stream(&mut self, stream: libp2p::Stream) {
        // Convert `AsyncWrite` stream to `Sink`
        self.sink = Some(FramedWrite::new(stream, Codec));
        self.stream_requested = false;
    }

    pub(crate) fn send_wantlist(
        &mut self,
        wantlist: ProtoWantlist,
        state: Arc<Mutex<SendingState>>,
    ) {
        debug_assert!(self.wantlist.is_none());
        debug_assert!(self.sending_state.is_none());

        self.wantlist = Some(wantlist);
        self.sending_state = Some(state);
    }

    fn poll_outgoing_no_stream(
        &mut self,
    ) -> Poll<ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, (), ToBehaviourEvent<S>>> {
        // `stream_requested` already checked in `poll_outgoing`
        debug_assert!(!self.stream_requested);
        // `wantlist` and `sending_state` must be both `Some` or both `None`
        debug_assert_eq!(self.wantlist.is_some(), self.sending_state.is_some());

        if self.wantlist.is_none() {
            // Nothing to send
            return Poll::Pending;
        }

        // There are data to send, so request a new stream.
        self.stream_requested = true;

        Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
            protocol: SubstreamProtocol::new(
                ReadyUpgrade::new(self.protocol.clone()),
                (), // TODO: maybe we can say here that we are the client?
            ),
        })
    }

    fn on_sink_error(&mut self) {
        self.sink.take();

        if self.wantlist.is_none() {
            if let Some(state) = self.sending_state.take() {
                *state.lock().unwrap() = SendingState::Poisoned;
                self.behaviour_waker.wake();
            }
        }
    }

    fn poll_outgoing(
        &mut self,
        cx: &mut Context,
    ) -> Poll<ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, (), ToBehaviourEvent<S>>> {
        loop {
            if self.stream_requested {
                // We can not progress until we have a stream
                return Poll::Pending;
            }

            let Some(sink) = self.sink.as_mut() else {
                return self.poll_outgoing_no_stream();
            };

            // Send the ongoing message before we continue to a new one
            if ready!(sink.poll_flush_unpin(cx)).is_err() {
                // Sink closed unexpectedly, retry
                self.on_sink_error();
                continue;
            }

            let Some(wantlist) = self.wantlist.take() else {
                // Nothing to send
                if let Some(state) = self.sending_state.take() {
                    *state.lock().unwrap() = SendingState::Ready;
                    self.behaviour_waker.wake();
                }
                return Poll::Pending;
            };

            let message = Message {
                wantlist: Some(wantlist),
                ..Message::default()
            };

            if sink.start_send_unpin(&message).is_err() {
                // Something went wrong, retry
                self.on_sink_error();
            }

            // Loop again, so `poll_flush` will be called and register a waker.
        }
    }

    pub(crate) fn poll(
        &mut self,
        cx: &mut Context,
    ) -> Poll<ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, (), ToBehaviourEvent<S>>> {
        self.poll_outgoing(cx)
    }
}

impl<const S: usize> fmt::Debug for ClientConnectionHandler<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientConnectionHandler")
    }
}

impl<const S: usize> Drop for ClientConnectionHandler<S> {
    fn drop(&mut self) {
        if let Some(state) = self.sending_state.take() {
            let mut state = state.lock().unwrap();

            // If sending was never done
            if *state == SendingState::Sending {
                *state = SendingState::Poisoned;
                self.behaviour_waker.wake();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::message::mod_Message::{mod_Wantlist::WantType, Block, BlockPresence};
    use crate::test_utils::{cid_of_data, poll_fn_once};
    use blockstore::{Blockstore, InMemoryBlockstore};
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
                ToSwarm::GenerateEvent(BitswapEvent::GetQueryResponse { query_id, data }) => {
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

        // Simulate that peer1 responsed with Have
        client.process_incoming_message(
            peer1,
            &Message {
                blockPresences: vec![BlockPresence {
                    cid: cid1.to_bytes(),
                    type_pb: BlockPresenceType::Have,
                }],
                ..Default::default()
            },
        );

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
        client.process_incoming_message(
            peer1,
            &Message {
                blockPresences: vec![BlockPresence {
                    cid: cid1.to_bytes(),
                    type_pb: BlockPresenceType::DontHave,
                }],
                ..Default::default()
            },
        );

        // Simulate that full wantlist is needed
        for peer_state in client.peers.values_mut() {
            *peer_state.last_send_full_tm.as_mut().unwrap() -= SEND_FULL_INTERVAL;
        }

        let ev = poll_fn(|cx| client.poll(cx)).await;
        let (peer_id, wantlist, send_state) = expect_send_wantlist_event(ev);

        // wantlist should be generated only for peer2, because peer1 already replied with DontHave
        assert_eq!(peer_id, peer2);
        assert_eq!(wantlist.entries.len(), 1);
        assert!(wantlist.full);

        let entry = &wantlist.entries[0];
        assert_eq!(entry.block, cid1.to_bytes());
        assert!(!entry.cancel);
        assert_eq!(entry.wantType, WantType::Have);
        assert!(entry.sendDontHave);

        // Mark send state as ready
        *send_state.lock().unwrap() = SendingState::Ready;

        // No other events should be produced
        assert!(poll_fn_once(|cx| client.poll(cx)).await.is_none());
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

        // Simulate that peer1 responsed with block
        client.process_incoming_message(
            peer,
            &Message {
                payload: vec![Block {
                    prefix: CidPrefix::from_cid(&cid1).to_bytes(),
                    data: b"x1".to_vec(),
                }],
                ..Default::default()
            },
        );

        // Receive an event with the found data
        let ev = poll_fn(|cx| client.poll(cx)).await;

        let (query_id, data) = match ev {
            ToSwarm::GenerateEvent(BitswapEvent::GetQueryResponse { query_id, data }) => {
                (query_id, data)
            }
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
        ClientBehaviour::<64, _>::new(ClientConfig::default(), store, None).unwrap()
    }

    fn expect_send_wantlist_event(
        ev: ToSwarm<BitswapEvent, ToHandlerEvent>,
    ) -> (PeerId, ProtoWantlist, Arc<Mutex<SendingState>>) {
        match ev {
            ToSwarm::NotifyHandler {
                peer_id,
                event: ToHandlerEvent::SendWantlist(wantlist, send_state),
                ..
            } => (peer_id, wantlist, send_state),
            ev => panic!("Expecting ToHandlerEvent::SendWantlist, found {ev:?}"),
        }
    }
}
