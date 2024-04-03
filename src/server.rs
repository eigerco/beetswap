use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use asynchronous_codec::FramedWrite;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use fnv::{FnvHashMap, FnvHashSet};
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt};
use libp2p_core::upgrade::ReadyUpgrade;
use libp2p_identity::PeerId;
use libp2p_swarm::{
    ConnectionHandlerEvent, NotifyHandler, StreamProtocol, SubstreamProtocol, ToSwarm,
};
use smallvec::SmallVec;
use tracing::{debug, trace, warn};

use crate::incoming_stream::ServerMessage;
use crate::message::Codec;
use crate::proto::message::{
    mod_Message::Block as ProtoBlock, mod_Message::Wantlist as ProtoWantlist, Message,
};
use crate::utils::{box_future, stream_protocol, BoxFuture};
use crate::{Event, Result, StreamRequester, ToBehaviourEvent, ToHandlerEvent};

type Sink = FramedWrite<libp2p_swarm::Stream, Codec>;
type BlockWithCid<const S: usize> = (CidGeneric<S>, Vec<u8>);

const MAX_WANTLIST_ENTRIES_PER_PEER: usize = 1024;

#[derive(Debug)]
pub(crate) struct ServerBehaviour<const S: usize, B>
where
    B: Blockstore,
{
    /// protocol, used to create connection handler
    protocol: StreamProtocol,
    /// blockstore to fetch blocks from
    store: Arc<B>,
    /// list of CIDs each connected peer is waiting for
    peers_wantlists: FnvHashMap<PeerId, PeerWantlist<S>>,
    /// list of peers that wait for particular CID
    peers_waiting_for_cid: FnvHashMap<CidGeneric<S>, SmallVec<[Arc<PeerId>; 1]>>,
    /// list of blocks received from blockstore or network, that connected peers may be waiting for
    outgoing_queue: VecDeque<BlockWithCid<S>>,
    /// list of events to be sent back to swarm when poll is called
    outgoing_event_queue: VecDeque<ToSwarm<Event, ToHandlerEvent>>,
    /// list of long running tasks, currently used to interact with the store
    tasks: FuturesUnordered<BoxFuture<'static, TaskResult<S>>>,
}

enum TaskResult<const S: usize> {
    Get(Arc<PeerId>, Vec<GetCidResult<S>>),
}

struct GetCidResult<const S: usize> {
    cid: CidGeneric<S>,
    data: Result<Option<Vec<u8>>, BlockstoreError>,
}

#[derive(Debug, Default)]
struct PeerWantlist<const S: usize>(FnvHashSet<CidGeneric<S>>);

impl<const S: usize> PeerWantlist<S> {
    /// Updates peers wantlist according to received message. Returns tuple with CIDs added and
    /// CIDs removed from wantlist.
    fn process_wantlist(
        &mut self,
        wantlist: ProtoWantlist,
    ) -> (Vec<CidGeneric<S>>, Vec<CidGeneric<S>>) {
        if wantlist.full {
            let wanted_cids = wantlist
                .entries
                .into_iter()
                .filter_map(|e| {
                    if e.cancel {
                        return None;
                    }
                    CidGeneric::try_from(e.block).ok()
                })
                .collect();

            return self.wantlist_replace(wanted_cids);
        }

        let (cancels, additions): (Vec<_>, Vec<_>) = wantlist
            .entries
            .into_iter()
            .filter_map(|e| {
                CidGeneric::<S>::try_from(e.block)
                    .map(|cid| (e.cancel, cid))
                    .ok()
            })
            .partition(|(cancel, _cid)| *cancel);

        // process cancels first, so that we truncate wantlist correctly
        let mut removed = Vec::with_capacity(cancels.len());
        for (_, cid) in cancels {
            if self.0.remove(&cid) {
                removed.push(cid);
            }
        }

        let mut added = Vec::with_capacity(additions.len());
        for (_, cid) in additions {
            if self.0.len() >= MAX_WANTLIST_ENTRIES_PER_PEER {
                break;
            }
            if self.0.insert(cid) {
                added.push(cid)
            }
        }

        (added, removed)
    }

    fn wantlist_replace(
        &mut self,
        cids: FnvHashSet<CidGeneric<S>>,
    ) -> (Vec<CidGeneric<S>>, Vec<CidGeneric<S>>) {
        let additions = cids.difference(&self.0).copied().collect();
        let removals = self.0.difference(&cids).copied().collect();

        self.0 = cids;

        (additions, removals)
    }
}

impl<const S: usize, B> ServerBehaviour<S, B>
where
    B: Blockstore + 'static,
{
    pub(crate) fn new(store: Arc<B>, protocol_prefix: Option<&str>) -> Self {
        let protocol = stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")
            .expect("prefix checked by beetswap::BehaviourBuilder::protocol_prefix");

        ServerBehaviour {
            protocol,
            store,
            peers_wantlists: Default::default(),
            peers_waiting_for_cid: Default::default(),
            tasks: Default::default(),
            outgoing_queue: Default::default(),
            outgoing_event_queue: Default::default(),
        }
    }

    fn schedule_store_get(&mut self, peer: Arc<PeerId>, cids: Vec<CidGeneric<S>>) {
        let store = self.store.clone();

        self.tasks.push(box_future(async move {
            let result = get_multiple_cids_from_store(store, cids).await;
            TaskResult::Get(peer, result)
        }));
    }

    fn cancel_request(&mut self, peer: Arc<PeerId>, cid: CidGeneric<S>) {
        // remove peer from the waitlist for cid, in case we happen to get it later
        if let Entry::Occupied(mut entry) = self.peers_waiting_for_cid.entry(cid) {
            if entry.get().as_ref() == [peer.clone()] {
                entry.remove();
            } else {
                let peers = entry.get_mut();
                if let Some(index) = peers.iter().position(|p| *p == peer) {
                    peers.swap_remove(index);
                }
            }
        }

        if let Some(peer_state) = self.peers_wantlists.get_mut(&peer) {
            peer_state.0.remove(&cid);
        }
    }

    pub(crate) fn process_incoming_message(&mut self, peer: PeerId, msg: ServerMessage) {
        let Some(wantlist) = self.peers_wantlists.get_mut(&peer) else {
            return; // entry should have been created in `new_connection_handler`
        };

        let (additions, removals) = wantlist.process_wantlist(msg.wantlist);

        debug!(
            "updating local wantlist for {peer}: added {}, removed {}",
            additions.len(),
            removals.len()
        );

        let peer = Arc::new(peer);
        for cid in &additions {
            self.peers_waiting_for_cid
                .entry(*cid)
                .or_default()
                .push(peer.clone());
        }
        self.schedule_store_get(peer.clone(), additions);

        for cid in removals {
            self.cancel_request(peer.clone(), cid);
        }
    }

    pub(crate) fn new_blocks_available(&mut self, blocks: Vec<BlockWithCid<S>>) {
        self.outgoing_queue.extend(blocks);
    }

    pub(crate) fn new_connection_handler(&mut self, peer: PeerId) -> ServerConnectionHandler<S> {
        self.peers_wantlists.entry(peer).or_default();

        ServerConnectionHandler {
            protocol: self.protocol.clone(),
            sink: Default::default(),
            pending_outgoing_messages: None,
        }
    }

    fn update_handlers(&mut self) -> bool {
        if self.outgoing_queue.is_empty() {
            return false;
        }

        let mut blocks_ready_for_peer =
            FnvHashMap::<Arc<PeerId>, Vec<(Vec<u8>, Vec<u8>)>>::default();

        while let Some((cid, data)) = self.outgoing_queue.pop_front() {
            let Some(peers_waiting) = self.peers_waiting_for_cid.remove(&cid) else {
                continue;
            };

            for peer in peers_waiting {
                blocks_ready_for_peer
                    .entry(peer)
                    .or_default()
                    .push((cid.to_bytes(), data.clone()))
            }
        }

        if blocks_ready_for_peer.is_empty() {
            return false;
        }

        trace!(
            "sending response to {} peer(s)",
            blocks_ready_for_peer.len()
        );
        for (peer, blocks) in blocks_ready_for_peer {
            self.outgoing_event_queue.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer,
                handler: NotifyHandler::Any,
                event: ToHandlerEvent::QueueOutgoingMessages(blocks),
            })
        }

        true
    }

    fn process_store_get_results(&mut self, peer: Arc<PeerId>, results: Vec<GetCidResult<S>>) {
        for result in results {
            let cid = result.cid;
            match result.data {
                Ok(None) => {
                    // requested CID isn't present locally. If we happen to get it, we'll
                    // forward it to the peer later
                    debug!("Cid {cid} not in blockstore for {peer}");
                }
                Ok(Some(data)) => {
                    trace!("Cid {cid} for {peer} present in blockstore");
                    self.outgoing_queue.push_back((cid, data));
                }
                Err(e) => {
                    warn!("Fetching {cid} from blockstore failed: {e}");
                }
            }
        }
    }

    pub(crate) fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Event, ToHandlerEvent>> {
        loop {
            if let Some(ev) = self.outgoing_event_queue.pop_front() {
                return Poll::Ready(ev);
            }

            if let Poll::Ready(Some(task_result)) = self.tasks.poll_next_unpin(cx) {
                match task_result {
                    TaskResult::Get(peer, results) => self.process_store_get_results(peer, results),
                }
                continue;
            }

            if self.update_handlers() {
                continue;
            }

            return Poll::Pending;
        }
    }
}

pub(crate) struct ServerConnectionHandler<const S: usize> {
    protocol: StreamProtocol,
    sink: SinkState,
    pending_outgoing_messages: Option<Vec<ProtoBlock>>,
}

impl<const S: usize> fmt::Debug for ServerConnectionHandler<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ServerConnectionHandler")
    }
}

#[derive(Default)]
enum SinkState {
    #[default]
    None,
    Requested,
    Ready(Sink),
}

impl<const S: usize> ServerConnectionHandler<S> {
    pub(crate) fn set_stream(&mut self, stream: libp2p_swarm::Stream) {
        // Convert `AsyncWrite` stream to `Sink`
        self.sink = SinkState::Ready(FramedWrite::new(stream, Codec));
    }

    pub(crate) fn queue_messages(&mut self, messages: Vec<(Vec<u8>, Vec<u8>)>) {
        let block_list = messages
            .into_iter()
            .map(|(prefix, data)| ProtoBlock { prefix, data })
            .collect::<Vec<_>>();

        self.pending_outgoing_messages
            .get_or_insert_with(|| Vec::with_capacity(block_list.len()))
            .extend(block_list);
    }

    fn open_new_substream(
        &mut self,
    ) -> Poll<
        ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, StreamRequester, ToBehaviourEvent<S>>,
    > {
        self.sink = SinkState::Requested;

        Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
            protocol: SubstreamProtocol::new(
                ReadyUpgrade::new(self.protocol.clone()),
                StreamRequester::Server,
            ),
        })
    }

    fn poll_outgoing(
        &mut self,
        cx: &mut Context,
    ) -> Poll<
        ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, StreamRequester, ToBehaviourEvent<S>>,
    > {
        loop {
            match (&mut self.pending_outgoing_messages, &mut self.sink) {
                (_, SinkState::Requested) => return Poll::Pending,
                (None, SinkState::None) => return Poll::Pending,
                (None, SinkState::Ready(sink)) => {
                    if ready!(sink.poll_flush_unpin(cx)).is_err() {
                        self.close_sink_on_error("poll_flush_unpin");
                    }

                    return Poll::Pending;
                }
                (Some(_), SinkState::None) => return self.open_new_substream(),
                (pending_messages @ Some(_), SinkState::Ready(sink)) => {
                    if ready!(sink.poll_flush_unpin(cx)).is_err() {
                        self.close_sink_on_error("poll_flush_unpin before sending message");
                        continue;
                    }

                    let messages = pending_messages
                        .take()
                        .expect("pending_messages can't be None here");
                    let message = Message {
                        payload: messages,
                        ..Message::default()
                    };

                    if sink.start_send_unpin(&message).is_err() {
                        self.close_sink_on_error("start_send_unpin");
                        continue;
                    }
                }
            }
        }
    }

    fn close_sink_on_error(&mut self, location: &str) {
        warn!("sink operation failed, closing: {location}");
        self.sink = SinkState::None;
    }

    pub(crate) fn poll(
        &mut self,
        cx: &mut Context,
    ) -> Poll<
        ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, StreamRequester, ToBehaviourEvent<S>>,
    > {
        self.poll_outgoing(cx)
    }
}

async fn get_multiple_cids_from_store<const S: usize, B: Blockstore>(
    store: Arc<B>,
    cids: Vec<CidGeneric<S>>,
) -> Vec<GetCidResult<S>> {
    let mut results = Vec::with_capacity(cids.len());

    for cid in cids {
        let data = store.get(&cid).await;
        results.push(GetCidResult { cid, data });
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::message::mod_Message::mod_Wantlist::{Entry, WantType};
    use crate::test_utils::{cid_of_data, poll_fn_once};
    use blockstore::InMemoryBlockstore;
    use cid::Cid;
    use multihash::Multihash;
    use std::future::poll_fn;

    #[test]
    fn wantlist_replace() {
        let initial_cids =
            (0..500_i32).map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()));
        let replacing_cids = (600..1200_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()));

        let mut wantlist = PeerWantlist::<64>::default();
        let (initial_events, _) = wantlist.wantlist_replace(initial_cids.clone().collect());
        assert_eq!(initial_cids.len(), initial_events.len());
        for cid in initial_cids.clone() {
            assert!(initial_events.contains(&cid));
        }

        let (added, removed) = wantlist.wantlist_replace(replacing_cids.clone().collect());
        assert_eq!(added.len(), replacing_cids.len());
        assert_eq!(removed.len(), initial_cids.len());
        for cid in replacing_cids {
            assert!(added.contains(&cid));
        }
        for cid in initial_cids {
            assert!(removed.contains(&cid));
        }
    }

    #[test]
    fn wantlist_replace_overlaping() {
        let initial_cids = (0..600_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();
        let replacing_cids = (500..1000_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();

        let mut wantlist = PeerWantlist::<64>::default();
        wantlist.wantlist_replace(initial_cids);
        let (added, removed) = wantlist.wantlist_replace(replacing_cids);

        let removed_cids: Vec<_> = (0..500_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();
        let added_cids: Vec<_> = (600..1000_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();
        assert_eq!(added.len(), added_cids.len());
        assert_eq!(removed.len(), removed_cids.len());
        for cid in added_cids {
            assert!(added.contains(&cid));
        }
        for cid in removed_cids {
            assert!(removed.contains(&cid));
        }
    }

    #[tokio::test]
    async fn client_requests_known_cid() {
        let data = "1";
        let cid = cid_of_data(data.as_bytes());
        let message = ServerMessage {
            wantlist: ProtoWantlist {
                full: true,
                entries: vec![Entry {
                    cancel: false,
                    priority: 0,
                    sendDontHave: false,
                    block: cid.into(),
                    wantType: WantType::Block,
                }],
            },
        };
        let peer = PeerId::random();

        let mut server = new_server().await;
        let _client_connection = server.new_connection_handler(peer);
        server.process_incoming_message(peer, message);

        let ev = poll_fn(|cx| server.poll(cx)).await;

        let ToSwarm::NotifyHandler { peer_id, event, .. } = ev else {
            panic!("Unexpected event {ev:?}");
        };
        assert_eq!(peer_id, peer);
        let ToHandlerEvent::QueueOutgoingMessages(msgs) = event else {
            panic!("Invalid handler message type ");
        };
        assert_eq!(msgs, vec![(cid.into(), data.as_bytes().to_vec())]);
    }

    #[tokio::test]
    async fn client_requests_unknown_cid() {
        let data = "unknown";
        let cid = cid_of_data(data.as_bytes());
        let message = ServerMessage {
            wantlist: ProtoWantlist {
                full: true,
                entries: vec![Entry {
                    cancel: false,
                    priority: 0,
                    sendDontHave: false,
                    block: cid.into(),
                    wantType: WantType::Block,
                }],
            },
        };
        let peer = PeerId::random();

        let mut server = new_server().await;
        let _client_connection = server.new_connection_handler(peer);
        server.process_incoming_message(peer, message);

        // no data yet
        assert!(poll_fn_once(|cx| server.poll(cx)).await.is_none());

        server.new_blocks_available(vec![(cid, data.into())]);

        let ev = poll_fn(|cx| server.poll(cx)).await;

        let ToSwarm::NotifyHandler { peer_id, event, .. } = ev else {
            panic!("Unexpected event {ev:?}");
        };
        assert_eq!(peer_id, peer);
        let ToHandlerEvent::QueueOutgoingMessages(msgs) = event else {
            panic!("Invalid handler message type ");
        };
        assert_eq!(msgs, vec![(cid.into(), data.as_bytes().to_vec())]);
    }

    async fn new_server() -> ServerBehaviour<64, InMemoryBlockstore<64>> {
        let store = Arc::new(InMemoryBlockstore::<64>::new());
        for i in 0..16 {
            let data = format!("{i}").into_bytes();
            let cid = cid_of_data(&data);
            store.put_keyed(&cid, &data).await.unwrap();
        }
        ServerBehaviour::<64, _>::new(store, None)
    }
}
