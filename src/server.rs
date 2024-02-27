use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::fmt;
use std::mem::take;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use asynchronous_codec::FramedWrite;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use fnv::{FnvHashMap, FnvHashSet};
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};
use libp2p_core::upgrade::ReadyUpgrade;
use libp2p_identity::PeerId;
use libp2p_swarm::{
    ConnectionHandlerEvent, NotifyHandler, StreamProtocol, SubstreamProtocol, ToSwarm,
};
use smallvec::SmallVec;
use tracing::{debug, info, trace, warn};

use crate::incoming_stream::ServerMessage;
use crate::message::Codec;
use crate::proto::message::{
    mod_Message::Block as ProtoBlock, mod_Message::Wantlist as ProtoWantlist, Message,
};
use crate::utils::stream_protocol;
use crate::{Event, Result, StreamRequester, ToBehaviourEvent, ToHandlerEvent};

type Sink = FramedWrite<libp2p_swarm::Stream, Codec>;
type BlockWithCid<const S: usize> = (CidGeneric<S>, Vec<u8>);

#[derive(Debug)]
pub(crate) struct ServerBehaviour<const S: usize, B>
where
    B: Blockstore,
{
    protocol: StreamProtocol,
    store: Arc<B>,

    peers_waitlists: FnvHashMap<PeerId, PeerWantlist<S>>,
    global_waitlist: FnvHashMap<CidGeneric<S>, SmallVec<[PeerId; 1]>>,

    outgoing_queue: Vec<BlockWithCid<S>>,
    outgoing_event_queue: VecDeque<ToSwarm<Event, ToHandlerEvent>>,

    blockstore_tasks: FuturesUnordered<BoxFuture<'static, BlockstoreResult<S>>>,
    blockstore_tasks_abort_handles: FnvHashMap<(PeerId, CidGeneric<S>), AbortHandle>,
}

#[derive(Debug)]
enum BlockstoreResult<const S: usize> {
    Get(
        PeerId,
        CidGeneric<S>,
        Result<Option<Vec<u8>>, BlockstoreError>,
    ),
    Cancelled,
}

#[derive(Debug, Default)]
struct PeerWantlist<const S: usize>(FnvHashSet<CidGeneric<S>>);

impl<const S: usize> PeerWantlist<S> {
    pub fn process_wantlist(&mut self, wantlist: ProtoWantlist) -> Vec<WishlistChange<S>> {
        // XXX quietly drop invalid entries from wantlist, do we care about logging?
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

        let mut results = Vec::with_capacity(wantlist.entries.len());

        for entry in wantlist.entries {
            let Ok(cid) = CidGeneric::try_from(entry.block) else {
                continue;
            };

            if !entry.cancel {
                if self.0.insert(cid) {
                    results.push(WishlistChange::WantCid(cid));
                }
            } else if self.0.remove(&cid) {
                results.push(WishlistChange::DoesntWantCid(cid))
            }
        }

        results
    }

    fn wantlist_replace(&mut self, cids: FnvHashSet<CidGeneric<S>>) -> Vec<WishlistChange<S>> {
        let wishlist_delta = cids
            .difference(&self.0)
            .map(|cid| WishlistChange::WantCid(*cid))
            .chain(
                self.0
                    .difference(&cids)
                    .map(|cid| WishlistChange::DoesntWantCid(*cid)),
            );

        let wishlist_delta = wishlist_delta.collect();

        self.0 = cids;

        wishlist_delta
    }
}

#[derive(Debug, PartialEq)]
enum WishlistChange<const S: usize> {
    Want(CidGeneric<S>),
    DontWant(CidGeneric<S>),
}

impl<const S: usize, B> ServerBehaviour<S, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    pub(crate) fn new(store: Arc<B>, protocol_prefix: Option<&str>) -> Self {
        let protocol = stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")
            .expect("prefix checked by beetswap::BehaviourBuilder::protocol_prefix");

        ServerBehaviour {
            protocol,
            store,
            peers_waitlists: FnvHashMap::default(),
            global_waitlist: FnvHashMap::default(),
            blockstore_tasks: Default::default(),
            blockstore_tasks_abort_handles: FnvHashMap::default(),
            outgoing_queue: Default::default(),
            outgoing_event_queue: Default::default(),
        }
    }

    fn schedule_store_get(&mut self, peer: PeerId, cid: CidGeneric<S>) {
        let store = self.store.clone();
        let (handle, reg) = AbortHandle::new_pair();

        self.blockstore_tasks.push(
            async move {
                match Abortable::new(store.get(&cid), reg).await {
                    Ok(result) => BlockstoreResult::Get(peer, cid, result),
                    Err(_) => BlockstoreResult::Cancelled,
                }
            }
            .boxed(),
        );

        self.blockstore_tasks_abort_handles
            .insert((peer, cid), handle);
    }

    fn cancel_request(&mut self, peer: PeerId, cid: CidGeneric<S>) {
        // remove pending blockstore read, if any
        if let Some(abort_handle) = self.blockstore_tasks_abort_handles.remove(&(peer, cid)) {
            abort_handle.abort();
        }

        // remove peer from the waitlist for cid, in case we happen to get it later
        if let Entry::Occupied(mut entry) = self.global_waitlist.entry(cid) {
            if entry.get().as_ref() == [peer] {
                entry.remove();
            } else {
                let peers = entry.get_mut();
                if let Some(index) = peers.iter().position(|p| *p == peer) {
                    peers.swap_remove(index);
                }
            }
        }

        if let Some(peer_state) = self.peers_waitlists.get_mut(&peer) {
            peer_state.0.remove(&cid);
        }
    }

    pub(crate) fn process_incoming_message(&mut self, peer: PeerId, msg: ServerMessage) {
        // TODO: or default once, and then rely on the data being there
        let rs = self
            .peers_waitlists
            .entry(peer)
            .or_default()
            .process_wantlist(msg.wantlist);

        debug!("{peer}: {rs:?}");

        for r in rs {
            match r {
                WishlistChange::WantCid(cid) => {
                    self.schedule_store_get(peer, cid);
                    self.global_waitlist.entry(cid).or_default().push(peer);
                }
                WishlistChange::DoesntWantCid(cid) => {
                    self.cancel_request(peer, cid);
                }
            }
        }
    }

    pub(crate) fn new_blocks_available(&mut self, blocks: Vec<BlockWithCid<S>>) {
        self.outgoing_queue.extend(blocks);
    }

    pub(crate) fn new_connection_handler(&mut self, peer: PeerId) -> ServerConnectionHandler<S> {
        self.peers_waitlists.entry(peer).or_default();

        ServerConnectionHandler {
            protocol: self.protocol.clone(),
            sink: Default::default(),
            sendlist: None,
        }
    }

    fn update_handlers(&mut self) -> bool {
        if self.outgoing_queue.is_empty() {
            return false;
        }

        let outgoing = take(&mut self.outgoing_queue);

        let mut peer_to_block = FnvHashMap::<PeerId, Vec<(Vec<u8>, Vec<u8>)>>::default();

        for (cid, data) in outgoing {
            let Some(waitlist) = self.global_waitlist.remove(&cid) else {
                continue;
            };

            for peer in waitlist {
                peer_to_block
                    .entry(peer)
                    .or_default()
                    .push((cid.to_bytes(), data.clone()))
            }
        }

        if peer_to_block.is_empty() {
            return false;
        }

        trace!("sending response to {} peer(s)", peer_to_block.len());

        for (peer, data) in peer_to_block {
            self.outgoing_event_queue.push_back(ToSwarm::NotifyHandler {
                peer_id: peer,
                handler: NotifyHandler::Any,
                event: ToHandlerEvent::SendSendlist(data),
            })
        }

        true
    }

    pub(crate) fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Event, ToHandlerEvent>> {
        loop {
            if let Some(ev) = self.outgoing_event_queue.pop_front() {
                return Poll::Ready(ev);
            }

            if let Poll::Ready(Some(blockstore_result)) = self.blockstore_tasks.poll_next_unpin(cx)
            {
                match blockstore_result {
                    BlockstoreResult::Get(peer, cid, Ok(None)) => {
                        // requested CID isn't present locally. If we happen to get it, we'll
                        // forward it to the peer later
                        debug!("Cid {cid} not in blockstore for {peer}");
                    }
                    BlockstoreResult::Get(peer, cid, Ok(Some(data))) => {
                        trace!("Cid {cid} for {peer} present in blockstore");
                        self.outgoing_queue.push((cid, data));
                    }
                    BlockstoreResult::Get(_peer, cid, Err(error)) => {
                        warn!("Fetching {cid} from blockstore failed: {error}");
                    }
                    BlockstoreResult::Cancelled => (),
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
    sendlist: Option<Vec<ProtoBlock>>,
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
        info!("got set stream");

        // Convert `AsyncWrite` stream to `Sink`
        self.sink = SinkState::Ready(FramedWrite::new(stream, Codec));
    }

    pub(crate) fn send_sendlist(&mut self, sendlist: Vec<(Vec<u8>, Vec<u8>)>) {
        let block_list = sendlist
            .into_iter()
            .map(|(prefix, data)| ProtoBlock { prefix, data })
            .collect::<Vec<_>>();

        self.sendlist
            .get_or_insert(Vec::with_capacity(block_list.len()))
            .extend(block_list);

        info!(
            "updated sendlist len: {:?}",
            self.sendlist.as_ref().map(|s| s.len())
        );
    }

    fn open_new_substream(
        &mut self,
    ) -> Poll<
        ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, StreamRequester, ToBehaviourEvent<S>>,
    > {
        info!("requesting new substream");

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
            match (&mut self.sendlist, &mut self.sink) {
                (_, SinkState::Requested) => return Poll::Pending,
                (None, SinkState::None) => return Poll::Pending,
                (None, SinkState::Ready(sink)) => {
                    if ready!(sink.poll_flush_unpin(cx)).is_err() {
                        self.close_sink("poll_flush_unpin");
                    }
                    return Poll::Pending;
                }
                (Some(_), SinkState::None) => return self.open_new_substream(),
                (sendlist @ Some(_), SinkState::Ready(sink)) => {
                    let sendlist = sendlist.take().expect("sendlist can't be None here");
                    let blocks = sendlist.len();

                    let message = Message {
                        payload: sendlist,
                        ..Message::default()
                    };

                    if ready!(sink.poll_flush_unpin(cx)).is_err() {
                        self.close_sink("poll_flush_unpin");
                        continue;
                    }

                    info!("start_send: {} blocks", blocks);
                    if sink.start_send_unpin(&message).is_err() {
                        self.close_sink("start_send_unpin");
                        continue;
                    }
                }
            }
        }
    }

    fn close_sink(&mut self, location: &str) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid;
    use multihash::Multihash;

    #[test]
    fn wishlist_replace() {
        let initial_cids =
            (0..512_i32).map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()));
        let replacing_cids = (513..1024_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()));

        let mut wantlist = PeerWantlist::<64>::default();
        let initial_events = wantlist.wantlist_replace(initial_cids.clone().collect());
        assert_eq!(initial_cids.len(), initial_events.len());
        for cid in initial_cids.clone() {
            assert!(initial_events.contains(&WishlistChange::WantCid(cid)));
        }

        let replacing_events = wantlist.wantlist_replace(replacing_cids.clone().collect());
        assert_eq!(
            replacing_events.len(),
            initial_cids.len() + replacing_cids.len()
        );
        for cid in replacing_cids {
            assert!(replacing_events.contains(&WishlistChange::WantCid(cid)));
        }
        for cid in initial_cids {
            assert!(replacing_events.contains(&WishlistChange::DoesntWantCid(cid)));
        }
    }

    #[test]
    fn wishlist_replace_overlaping() {
        let initial_cids = (0..600_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();
        let replacing_cids = (500..1000_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();

        let mut wantlist = PeerWantlist::<64>::default();
        wantlist.wantlist_replace(initial_cids);
        let events = wantlist.wantlist_replace(replacing_cids);

        let removed_cids: Vec<_> = (0..500_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();
        let added_cids: Vec<_> = (600..1000_i32)
            .map(|v| Cid::new_v1(24, Multihash::wrap(42, &v.to_le_bytes()).unwrap()))
            .collect();
        assert_eq!(events.len(), added_cids.len() + removed_cids.len());
        for cid in added_cids {
            assert!(events.contains(&WishlistChange::WantCid(cid)));
        }
        for cid in removed_cids {
            assert!(events.contains(&WishlistChange::DoesntWantCid(cid)));
        }
    }
}
