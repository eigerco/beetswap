use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use asynchronous_codec::FramedWrite;
use blockstore::{Blockstore, BlockstoreError};
use cid::CidGeneric;
use fnv::FnvHashMap;
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};
use libp2p::{
    core::upgrade::ReadyUpgrade,
    swarm::{ConnectionHandlerEvent, SubstreamProtocol, ToSwarm},
    StreamProtocol,
};
use smallvec::SmallVec;
use std::sync::Mutex;

use crate::message::{Codec, MessageMetadata};
use crate::proto::message::mod_Message::{BlockPresenceType, Wantlist as ProtoWantlist};
use crate::proto::message::Message;
use crate::utils::convert_cid;
use crate::wantlist::{Wantlist, WantlistState};
use crate::{BitswapError, BitswapEvent, Result, ToBehaviourEvent, ToHandlerEvent};

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
    config: Arc<ClientConfig>,
    store: Arc<B>,
    protocol: StreamProtocol,
    wantlist: Arc<Mutex<Wantlist<S>>>,
    cid_to_queries: FnvHashMap<CidGeneric<S>, SmallVec<[QueryId; 1]>>,
    queue: VecDeque<ToSwarm<BitswapEvent, ToHandlerEvent>>,
    tasks: FuturesUnordered<BoxFuture<'static, TaskResult<S>>>,
    query_abort_handle: FnvHashMap<QueryId, AbortHandle>,
    next_query_id: u64,
}

impl<const S: usize, B> ClientBehaviour<S, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    pub(crate) fn new(config: ClientConfig, store: Arc<B>, protocol: StreamProtocol) -> Self {
        ClientBehaviour {
            config: Arc::new(config),
            store,
            protocol,
            wantlist: Arc::new(Mutex::new(Wantlist::new())),
            cid_to_queries: FnvHashMap::default(),
            queue: VecDeque::new(),
            tasks: FuturesUnordered::new(),
            query_abort_handle: FnvHashMap::default(),
            next_query_id: 0,
        }
    }

    pub(crate) fn new_connection_handler(&self) -> ClientConnectionHandler<S> {
        ClientConnectionHandler {
            config: self.config.clone(),
            protocol: self.protocol.clone(),
            stream_requested: false,
            sink: None,
            wantlist_state: WantlistState::new(self.wantlist.clone()),
        }
    }

    fn next_query_id(&mut self) -> QueryId {
        let id = QueryId(self.next_query_id);
        self.next_query_id += 1;
        id
    }

    pub(crate) fn get<const CS: usize>(&mut self, cid: &CidGeneric<CS>) -> QueryId {
        let query_id = self.next_query_id();

        match convert_cid(cid) {
            Some(cid) => {
                let store = self.store.clone();
                let (handle, reg) = AbortHandle::new_pair();

                self.tasks.push(
                    async move {
                        match Abortable::new(store.get(&cid), reg).await {
                            Ok(res) => TaskResult::Get(query_id, cid, res),
                            Err(_) => TaskResult::Cancelled,
                        }
                    }
                    .boxed(),
                );

                self.query_abort_handle.insert(query_id, handle);
            }
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
                    let cid = cid.to_owned();
                    self.cid_to_queries.remove(&cid);
                    self.wantlist.lock().unwrap().remove(&cid);
                }

                break;
            }
        }
    }

    pub(crate) fn process_incoming_message(
        &mut self,
        msg: &Message,
        metadata: &MessageMetadata<S>,
    ) {
        let mut new_blocks = Vec::new();

        // TODO: If some sends a huge message, the executor will block! We need to truncate the
        // data, maybe even in the `message::Codec` level
        for (block, cid) in msg.payload.iter().zip(metadata.blocks_cid.iter()) {
            let Some(cid) = cid else {
                continue;
            };

            if !self.wantlist.lock().unwrap().remove(cid) {
                debug_assert!(!self.cid_to_queries.contains_key(cid));
                continue;
            }

            new_blocks.push((cid.to_owned(), block.data.clone()));

            if let Some(queries) = self.cid_to_queries.remove(cid) {
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
            let store = self.store.clone();

            self.tasks.push(
                async move {
                    let res = store.put_many_keyed(new_blocks.into_iter()).await;
                    TaskResult::Set(res)
                }
                .boxed(),
            );
        }
    }

    pub(crate) fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<BitswapEvent, ToHandlerEvent>> {
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

                    // Blockstore doesn't have the data, add CID in the wantlist. Connection handler will
                    // read the list and do the request.
                    TaskResult::Get(query_id, cid, Ok(None)) => {
                        self.wantlist.lock().unwrap().insert(cid);
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

            return Poll::Pending;
        }
    }
}

pub(crate) struct ClientConnectionHandler<const S: usize> {
    config: Arc<ClientConfig>,
    protocol: StreamProtocol,
    stream_requested: bool,
    sink: Option<FramedWrite<libp2p::Stream, Codec>>,
    wantlist_state: WantlistState<S>,
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

    pub(crate) fn process_incoming_message(
        &mut self,
        msg: &Message,
        metadata: &MessageMetadata<S>,
    ) {
        for block_presense in &msg.blockPresences {
            let Ok(cid) = CidGeneric::try_from(&*block_presense.cid) else {
                continue;
            };

            match block_presense.type_pb {
                BlockPresenceType::Have => self.wantlist_state.got_have(&cid),
                BlockPresenceType::DontHave => self.wantlist_state.got_dont_have(&cid),
            }
        }

        for block_cid in metadata.blocks_cid.iter().flatten() {
            self.wantlist_state.got_block(block_cid);
        }
    }

    fn poll_outgoing_no_stream(
        &mut self,
    ) -> Poll<ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, (), ToBehaviourEvent<S>>> {
        // `stream_requested` already checked in `poll_outgoing`
        debug_assert!(!self.stream_requested);

        if self.wantlist_state.is_updated() {
            return Poll::Pending;
        }

        // Other end needs to be updated, so request a new stream
        self.stream_requested = true;

        Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
            protocol: SubstreamProtocol::new(
                ReadyUpgrade::new(self.protocol.clone()),
                (), // TODO: maybe we can say here that we are the client?
            ),
        })
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
                // Sink closed unexpectedly, remove it and retry
                self.sink.take();
                continue;
            }

            let entries = self
                .wantlist_state
                .generate_update_entries(self.config.set_send_dont_have);

            if entries.is_empty() {
                return Poll::Pending;
            }

            let message = Message {
                wantlist: Some(ProtoWantlist {
                    entries,
                    full: false,
                }),
                ..Message::default()
            };

            if sink.start_send_unpin(&message).is_err() {
                // Something went wrong, retry
                self.sink.take();
                continue;
            }

            return Poll::Pending;
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
