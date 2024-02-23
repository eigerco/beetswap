use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::fmt;
use std::mem::take;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use asynchronous_codec::FramedWrite;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use fnv::FnvHashMap;
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};
use libp2p_core::upgrade::ReadyUpgrade;
use libp2p_identity::PeerId;
use libp2p_swarm::{
    ConnectionHandlerEvent, NotifyHandler, StreamProtocol, SubstreamProtocol, ToSwarm,
};
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
    B: Blockstore + Send + Send,
{
    protocol: StreamProtocol,
    store: Arc<B>,
    peers: FnvHashMap<PeerId, PeerState<S>>,
    global_waitlist: FnvHashMap<CidGeneric<S>, Vec<PeerId>>,

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
struct PeerState<const S: usize> {
    map: FnvHashMap<CidGeneric<S>, ()>,
}

impl<const S: usize> PeerState<S> {
    pub fn process_wantlist(&mut self, wantlist: ProtoWantlist) -> Vec<WishlistChange<S>> {
        let (add, remove): (Vec<_>, Vec<_>) = wantlist.entries.into_iter().partition(|e| !e.cancel);
        let add = add
            .into_iter()
            .map(|e| CidGeneric::try_from(e.block).unwrap())
            .collect();
        let remove = remove
            .into_iter()
            .map(|e| CidGeneric::try_from(e.block).unwrap())
            .collect();

        let mut results = vec![];
        if wantlist.full {
            results.extend(self.wantlist_replace(add));
        } else {
            results.extend(self.wantlist_add(add));
            results.extend(self.wantlist_remove(remove));
        }

        results
    }

    fn wantlist_add(&mut self, cids: Vec<CidGeneric<S>>) -> Vec<WishlistChange<S>> {
        let mut r = vec![];
        for cid in cids {
            if self.map.insert(cid, ()).is_none() {
                r.push(WishlistChange::WantCid(cid))
            }
        }
        r
    }

    fn wantlist_remove(&mut self, cids: Vec<CidGeneric<S>>) -> Vec<WishlistChange<S>> {
        let mut r = vec![];
        for cid in cids {
            if self.map.remove(&cid).is_some() {
                r.push(WishlistChange::DoesntWantCid(cid))
            }
        }
        r
    }

    fn wantlist_replace(&mut self, cids: Vec<CidGeneric<S>>) -> Vec<WishlistChange<S>> {
        let mut r = vec![];
        // TODO smarter algo
        for cid in &cids {
            if !self.map.contains_key(cid) {
                r.push(WishlistChange::WantCid(*cid));
            }
        }
        for key in self.map.keys() {
            if !cids.contains(key) {
                r.push(WishlistChange::DoesntWantCid(*key));
            }
        }

        self.map.clear();
        for c in cids {
            self.map.insert(c, ());
        }

        r
    }
}

#[derive(Debug)]
enum WishlistChange<const S: usize> {
    WantCid(CidGeneric<S>),
    DoesntWantCid(CidGeneric<S>),
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
            peers: FnvHashMap::default(),
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
            if entry.get() == &vec![peer] {
                entry.remove();
            } else {
                let peers = entry.get_mut();
                if let Some(index) = peers.iter().position(|p| *p == peer) {
                    peers.swap_remove(index);
                }
            }
        }

        if let Some(peer_state) = self.peers.get_mut(&peer) {
            peer_state.map.remove(&cid);
        }
    }

    pub(crate) fn process_incoming_message(&mut self, peer: PeerId, msg: ServerMessage) {
        // TODO: or default once, and then rely on the data being there
        let rs = self
            .peers
            .entry(peer)
            .or_default()
            .process_wantlist(msg.wantlist);

        info!("{peer}: {rs:?}");

        for r in rs {
            match r {
                WishlistChange::WantCid(cid) => {
                    self.schedule_store_get(peer, cid);
                    self.global_waitlist.entry(cid).or_default().push(peer);
                }
                WishlistChange::DoesntWantCid(cid) => {
                    self.cancel_request(peer, cid);
                    match self.global_waitlist.entry(cid) {
                        Entry::Occupied(mut entry) => {
                            let v = entry.get_mut();
                            // TODO better algo?
                            if let Some(index) = v.iter().position(|p| *p == peer) {
                                v.swap_remove(index);
                            } else {
                                warn!("Requesting to remove CID for unexpected peer");
                            }
                        }
                        Entry::Vacant(_) => {
                            warn!("Requesting to remove unexpected CID from wishlist")
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn new_blocks_available(&mut self, blocks: Vec<BlockWithCid<S>>) {
        self.outgoing_queue.extend(blocks);
    }

    pub(crate) fn new_connection_handler(&mut self, peer: PeerId) -> ServerConnectionHandler<S> {
        self.peers.entry(peer).or_default();

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
