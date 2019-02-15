/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class DiscoveryMessageResultsCollector<M, R>  {
    /** */
    private final Map<UUID, NodeMessage<M>> rcvd = new HashMap<>();

    /** */
    private int leftMsgs;

    /** */
    protected DiscoCache discoCache;

    /** */
    protected final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    protected DiscoveryMessageResultsCollector(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param rcvd Received messages.
     * @return Result.
     */
    protected abstract R createResult(Map<UUID, NodeMessage<M>> rcvd);

    /**
     * @param r Result.
     */
    protected abstract void onResultsCollected(R r);

    /**
     * @param discoCache Discovery state when discovery message was received.
     * @param node Node.
     * @return {@code True} if need wait for result from given node.
     */
    protected abstract boolean waitForNode(DiscoCache discoCache, ClusterNode node);

    /**
     * @param discoCache Discovery state.
     */
    public final void init(DiscoCache discoCache) {
        assert discoCache != null;

        R res = null;

        synchronized (this) {
            assert this.discoCache == null;
            assert leftMsgs == 0 : leftMsgs;

            this.discoCache = discoCache;

            for (ClusterNode node : discoCache.allNodes()) {
                if (ctx.discovery().alive(node) && waitForNode(discoCache, node) && !rcvd.containsKey(node.id())) {
                    rcvd.put(node.id(), new NodeMessage<>((M)null));

                    leftMsgs++;
                }
            }

            if (leftMsgs == 0)
                res = createResult(rcvd);
        }

        if (res != null)
            onResultsCollected(res);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    public final void onMessage(UUID nodeId, M msg) {
        R res = null;

        synchronized (this) {
            if (allReceived())
                return;

            NodeMessage<M> expMsg = rcvd.get(nodeId);

            if (expMsg == null)
                rcvd.put(nodeId, new NodeMessage<>(msg));
            else if (expMsg.set(msg)) {
                assert leftMsgs > 0;

                leftMsgs--;

                if (allReceived())
                    res = createResult(rcvd);
            }
        }

        if (res != null)
            onResultsCollected(res);
    }

    /**
     * @param nodeId Failed node ID.
     */
    public final void onNodeFail(UUID nodeId) {
        R res = null;

        synchronized (this) {
            if (allReceived())
                return;

            NodeMessage expMsg = rcvd.get(nodeId);

            if (expMsg != null && expMsg.onNodeFailed()) {
                assert leftMsgs > 0 : leftMsgs;

                leftMsgs--;

                if (allReceived())
                    res = createResult(rcvd);
            }
        }

        if (res != null)
            onResultsCollected(res);
    }

    /**
     * @return {@code True} if expected messages are initialized and all message are received.
     */
    private boolean allReceived() {
        return discoCache != null && leftMsgs == 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryMessageResultsCollector.class, this);
    }

    /**
     *
     */
    protected static class NodeMessage<M> {
        /** */
        boolean nodeFailed;

        /** */
        M msg;

        /**
         * @param msg Message.
         */
        NodeMessage(M msg) {
            this.msg = msg;
        }

        /**
         * @return Message or {@code null} if node failed.
         */
        @Nullable public M message() {
            return msg;
        }

        /**
         * @return {@code True} if node result was not set before.
         */
        boolean onNodeFailed() {
            if (nodeFailed || msg != null)
                return false;

            nodeFailed = true;

            return true;
        }

        /**
         * @param msg Received message.
         * @return {@code True} if node result was not set before.
         */
        boolean set(M msg) {
            assert msg != null;

            if (this.msg != null)
                return false;

            this.msg = msg;

            return !nodeFailed;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NodeMessage.class, this);
        }
    }
}
