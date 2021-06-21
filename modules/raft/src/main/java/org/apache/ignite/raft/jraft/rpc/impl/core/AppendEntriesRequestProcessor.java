/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc.impl.core;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.impl.ConnectionClosedEventListener;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.jraft.util.concurrent.SingleThreadExecutor;

/**
 * Append entries request processor.
 */
public class AppendEntriesRequestProcessor extends NodeRequestProcessor<AppendEntriesRequest> implements
    ConnectionClosedEventListener {
    /**
     * Peer executor selector.
     */
    final class PeerExecutorSelector implements ExecutorSelector {
        PeerExecutorSelector() {
            super();
        }

        @Override
        public Executor select(final String reqClass, final Object req, NodeManager nodeManager) {
            final AppendEntriesRequest req0 = (AppendEntriesRequest) req;
            final String groupId = req0.getGroupId();
            final String peerId = req0.getPeerId();
            final String serverId = req0.getServerId();

            final PeerId peer = new PeerId();

            if (!peer.parse(peerId)) {
                return executor();
            }

            final Node node = nodeManager.get(groupId, peer);

            if (node == null || !node.getRaftOptions().isReplicatorPipeline()) {
                return executor();
            }

            PeerPair pair = pairOf(peerId, serverId);

            final PeerRequestContext ctx = getOrCreatePeerRequestContext(groupId, pair, nodeManager);

            return ctx.executor;
        }
    }

    /**
     * RpcRequestClosure that will send responses in pipeline mode.
     */
    class SequenceRpcRequestClosure extends RpcRequestClosure {

        private final int reqSequence;
        private final String groupId;
        private final PeerPair pair;
        private final boolean isHeartbeat;

        SequenceRpcRequestClosure(final RpcRequestClosure parent, final Message defaultResp,
            final String groupId, final PeerPair pair, final int sequence,
            final boolean isHeartbeat) {
            super(parent.getRpcCtx(), defaultResp);
            this.reqSequence = sequence;
            this.groupId = groupId;
            this.pair = pair;
            this.isHeartbeat = isHeartbeat;
        }

        @Override
        public void sendResponse(final Message msg) {
            if (this.isHeartbeat) {
                super.sendResponse(msg);
            }
            else {
                sendSequenceResponse(this.groupId, this.pair, this.reqSequence, getRpcCtx(), msg);
            }
        }
    }

    /**
     * Response message wrapper with a request sequence number and asyncContext.done
     */
    static class SequenceMessage implements Comparable<SequenceMessage> {
        public final Message msg;
        private final int sequence;
        private final RpcContext rpcCtx;

        SequenceMessage(final RpcContext rpcCtx, final Message msg, final int sequence) {
            super();
            this.rpcCtx = rpcCtx;
            this.msg = msg;
            this.sequence = sequence;
        }

        /**
         * Send the response.
         */
        void sendResponse() {
            this.rpcCtx.sendResponse(this.msg);
        }

        /**
         * Order by sequence number
         */
        @Override
        public int compareTo(final SequenceMessage o) {
            return Integer.compare(this.sequence, o.sequence);
        }
    }

    // constant pool for peer pair
    private final Map<String, Map<String, PeerPair>> pairConstants = new HashMap<>();

    PeerPair pairOf(final String peerId, final String serverId) {
        synchronized (this.pairConstants) {
            Map<String, PeerPair> pairs = this.pairConstants.computeIfAbsent(peerId, k -> new HashMap<>());

            PeerPair pair = pairs.computeIfAbsent(serverId, k -> new PeerPair(peerId, serverId));
            return pair;
        }
    }

    /**
     * A peer pair
     */
    static class PeerPair {
        // peer in local node
        final String local;
        // peer in remote node
        final String remote;

        PeerPair(final String local, final String remote) {
            super();
            this.local = local;
            this.remote = remote;
        }

        @Override
        public String toString() {
            return "PeerPair[" + this.local + " -> " + this.remote + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.local == null) ? 0 : this.local.hashCode());
            result = prime * result + ((this.remote == null) ? 0 : this.remote.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PeerPair other = (PeerPair) obj;
            if (this.local == null) {
                if (other.local != null) {
                    return false;
                }
            }
            else if (!this.local.equals(other.local)) {
                return false;
            }
            if (this.remote == null) {
                if (other.remote != null) {
                    return false;
                }
            }
            else if (!this.remote.equals(other.remote)) {
                return false;
            }
            return true;
        }
    }

    static class PeerRequestContext {
        private final String groupId;
        private final PeerPair pair;

        // Executor to run the requests
        private SingleThreadExecutor executor;

        // The request sequence;
        private int sequence;

        // The required sequence to be sent.
        private int nextRequiredSequence;

        // The response queue,it's not thread-safe and protected by it self object monitor.
        private final PriorityQueue<SequenceMessage> responseQueue;

        private final int maxPendingResponses;

        PeerRequestContext(final String groupId, final PeerPair pair, final int maxPendingResponses) {
            super();
            this.pair = pair;
            this.groupId = groupId;
            this.sequence = 0;
            this.nextRequiredSequence = 0;
            this.maxPendingResponses = maxPendingResponses;
            this.responseQueue = new PriorityQueue<>(50);
        }

        boolean hasTooManyPendingResponses() {
            return this.responseQueue.size() > this.maxPendingResponses;
        }

        int getAndIncrementSequence() {
            final int prev = this.sequence;
            this.sequence++;
            if (this.sequence < 0) {
                this.sequence = 0;
            }
            return prev;
        }

        int getNextRequiredSequence() {
            return this.nextRequiredSequence;
        }

        int getAndIncrementNextRequiredSequence() {
            final int prev = this.nextRequiredSequence;
            this.nextRequiredSequence++;
            if (this.nextRequiredSequence < 0) {
                this.nextRequiredSequence = 0;
            }
            return prev;
        }
    }

    PeerRequestContext getPeerRequestContext(final String groupId, final PeerPair pair) {
        ConcurrentMap<PeerPair, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);

        if (groupContexts == null) {
            return null;
        }
        return groupContexts.get(pair);
    }

    /**
     * Send response in pipeline mode.
     */
    void sendSequenceResponse(final String groupId, final PeerPair pair, final int seq, final RpcContext rpcCtx,
        final Message msg) {
        final PeerRequestContext ctx = getPeerRequestContext(groupId, pair);

        if (ctx == null) {
            // the context was destroyed, so the response can be ignored.
            return;
        }

        // TODO asch queue is not needed if handled by single thread. Replicator should send message from the same
        //  thread per pair https://issues.apache.org/jira/browse/IGNITE-14832.
        final PriorityQueue<SequenceMessage> respQueue = ctx.responseQueue;
        assert (respQueue != null);

        synchronized (Utils.withLockObject(respQueue)) {
            respQueue.add(new SequenceMessage(rpcCtx, msg, seq));

            if (!ctx.hasTooManyPendingResponses()) {
                while (!respQueue.isEmpty()) {
                    final SequenceMessage queuedPipelinedResponse = respQueue.peek();

                    if (queuedPipelinedResponse.sequence != ctx.getNextRequiredSequence()) {
                        // sequence mismatch, waiting for next response.
                        break;
                    }

                    respQueue.remove();

                    try {
                        queuedPipelinedResponse.sendResponse();
                    }
                    finally {
                        ctx.getAndIncrementNextRequiredSequence();
                    }
                }
            }
            else {
                LOG.warn("Dropping pipelined responses to peer {}/{}, because of too many pending responses, queued={}, max={}",
                    ctx.groupId, pair, respQueue.size(), ctx.maxPendingResponses);

                removePeerRequestContext(groupId, pair);
            }
        }
    }

    PeerRequestContext getOrCreatePeerRequestContext(final String groupId, final PeerPair pair,
        NodeManager nodeManager) {
        ConcurrentMap<PeerPair, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);
        if (groupContexts == null) {
            groupContexts = new ConcurrentHashMap<>();
            final ConcurrentMap<PeerPair, PeerRequestContext> existsCtxs = this.peerRequestContexts.putIfAbsent(
                groupId, groupContexts);
            if (existsCtxs != null) {
                groupContexts = existsCtxs;
            }
        }

        PeerRequestContext peerCtx = groupContexts.get(pair);
        if (peerCtx == null) {
            synchronized (Utils.withLockObject(groupContexts)) {
                peerCtx = groupContexts.get(pair);
                // double check in lock
                if (peerCtx == null) {
                    // only one thread to process append entries for every jraft node
                    final PeerId peer = new PeerId();
                    final boolean parsed = peer.parse(pair.local);
                    assert (parsed);
                    final Node node = nodeManager.get(groupId, peer);
                    assert (node != null);
                    peerCtx = new PeerRequestContext(groupId, pair, node.getRaftOptions().getMaxReplicatorInflightMsgs());

                    peerCtx.executor = node.getOptions().getStripedExecutor().next();

                    groupContexts.put(pair, peerCtx);
                }
            }
        }

        return peerCtx;
    }

    void removePeerRequestContext(final String groupId, final PeerPair pair) {
        final ConcurrentMap<PeerPair, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);

        if (groupContexts == null) {
            return;
        }

        groupContexts.remove(pair);
    }

    /**
     * RAFT group peer request contexts.
     */
    private final ConcurrentMap<String /*groupId*/, ConcurrentMap<PeerPair, PeerRequestContext>> peerRequestContexts = new ConcurrentHashMap<>();

    /**
     * The executor selector to select executor for processing request.
     */
    private final ExecutorSelector executorSelector;

    public AppendEntriesRequestProcessor(final Executor executor) {
        super(executor, RpcRequests.AppendEntriesResponse.getDefaultInstance());
        this.executorSelector = new PeerExecutorSelector();
    }

    @Override
    protected String getPeerId(final AppendEntriesRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final AppendEntriesRequest request) {
        return request.getGroupId();
    }

    private int getAndIncrementSequence(final String groupId, final PeerPair pair, NodeManager nodeManager) {
        // TODO asch can use getPeerContext because it must already present (created before) ??? IGNITE-14832
        return getOrCreatePeerRequestContext(groupId, pair, nodeManager).getAndIncrementSequence();
    }

    private boolean isHeartbeatRequest(final AppendEntriesRequest request) {
        // No entries and no data means a true heartbeat request.
        // TODO refactor, adds a new flag field? https://issues.apache.org/jira/browse/IGNITE-14832
        return request.getEntriesCount() == 0 && !request.hasData();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final AppendEntriesRequest request,
        final RpcRequestClosure done) {

        final Node node = (Node) service;

        if (node.getRaftOptions().isReplicatorPipeline()) {
            final String groupId = request.getGroupId();
            final PeerPair pair = pairOf(request.getPeerId(), request.getServerId());

            boolean isHeartbeat = isHeartbeatRequest(request);
            int reqSequence = -1;

            if (!isHeartbeat) {
                reqSequence = getAndIncrementSequence(groupId, pair, done.getRpcCtx().getNodeManager());
            }

            final Message response = service.handleAppendEntriesRequest(request, new SequenceRpcRequestClosure(done,
                defaultResp(), groupId, pair, reqSequence, isHeartbeat));

            if (response != null) {
                // heartbeat or probe request
                if (isHeartbeat) {
                    done.getRpcCtx().sendResponse(response);
                }
                else {
                    sendSequenceResponse(groupId, pair, reqSequence, done.getRpcCtx(), response);
                }
            }

            return null;
        }
        else {
            return service.handleAppendEntriesRequest(request, done);
        }
    }

    @Override
    public String interest() {
        return AppendEntriesRequest.class.getName();
    }

    @Override
    public ExecutorSelector executorSelector() {
        return this.executorSelector;
    }

    // TODO called when shutdown service https://issues.apache.org/jira/browse/IGNITE-14832
    public void destroy() {
        this.peerRequestContexts.clear();
    }

    /**
     * @param local Local peer.
     * @param remote Remote peer.
     */
    @Override public void onClosed(String local, String remote) {
        PeerPair pair = new PeerPair(local, remote);

        for (final Map.Entry<String, ConcurrentMap<PeerPair, PeerRequestContext>> entry : this.peerRequestContexts
            .entrySet()) {
            final ConcurrentMap<PeerPair, PeerRequestContext> groupCtxs = entry.getValue();
            groupCtxs.remove(pair);
        }
    }
}
