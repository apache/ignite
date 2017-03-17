/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.clock;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryTopologySnapshot;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridBoundedConcurrentOrderedMap;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_TIME_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TIME_SERVER_HOST;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TIME_SERVER_PORT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Time synchronization processor.
 */
public class GridClockSyncProcessor extends GridProcessorAdapter {
    /** Maximum size for time sync history. */
    private static final int MAX_TIME_SYNC_HISTORY = 100;

    /** Time server instance. */
    private GridClockServer srv;

    /** Shutdown lock. */
    private GridSpinReadWriteLock rw = new GridSpinReadWriteLock();

    /** Stopping flag. */
    private volatile boolean stopping;

    /** Time coordinator thread. */
    private volatile TimeCoordinator timeCoord;

    /** Time delta history. Constructed on coordinator. */
    private NavigableMap<GridClockDeltaVersion, GridClockDeltaSnapshot> timeSyncHist =
        new GridBoundedConcurrentOrderedMap<>(MAX_TIME_SYNC_HISTORY);

    /** Last recorded. */
    private volatile T2<GridClockDeltaVersion, GridClockDeltaSnapshot> lastSnapshot;

    /** Time source. */
    private GridClockSource clockSrc;

    /**
     * @param ctx Kernal context.
     */
    public GridClockSyncProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        clockSrc = ctx.timeSource();

        srv = new GridClockServer();

        srv.start(ctx);

        ctx.io().addMessageListener(TOPIC_TIME_SYNC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                assert msg instanceof GridClockDeltaSnapshotMessage;

                GridClockDeltaSnapshotMessage msg0 = (GridClockDeltaSnapshotMessage)msg;

                GridClockDeltaVersion ver = msg0.snapshotVersion();

                GridClockDeltaSnapshot snap = new GridClockDeltaSnapshot(ver, msg0.deltas());

                lastSnapshot = new T2<>(ver, snap);

                timeSyncHist.put(ver, snap);
            }
        });

        // We care only about node leave and fail events.
        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_JOINED;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                    checkLaunchCoordinator(discoEvt);

                TimeCoordinator timeCoord0 = timeCoord;

                if (timeCoord0 != null)
                    timeCoord0.onDiscoveryEvent(discoEvt);
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_NODE_JOINED);

        ctx.addNodeAttribute(ATTR_TIME_SERVER_HOST, srv.host());
        ctx.addNodeAttribute(ATTR_TIME_SERVER_PORT, srv.port());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        srv.afterStart();

        // Check at startup if this node is a fragmentizer coordinator.
        DiscoveryEvent locJoinEvt = ctx.discovery().localJoinEvent();

        checkLaunchCoordinator(locJoinEvt);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        rw.writeLock();

        try {
            stopping = false;

            if (timeCoord != null) {
                timeCoord.cancel();

                U.join(timeCoord, log);

                timeCoord = null;
            }

            if (srv != null)
                srv.beforeStop();
        }
        finally {
            rw.writeUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (srv != null)
            srv.stop();
    }

    /**
     * Gets current time on local node.
     *
     * @return Current time in milliseconds.
     */
    private long currentTime() {
        return clockSrc.currentTimeMillis();
    }

    /**
     * @return Time sync history.
     */
    public NavigableMap<GridClockDeltaVersion, GridClockDeltaSnapshot> timeSyncHistory() {
        return timeSyncHist;
    }

    /**
     * Callback from server for message receiving.
     *
     * @param msg Received message.
     * @param addr Remote node address.
     * @param port Remote node port.
     */
    public void onMessageReceived(GridClockMessage msg, InetAddress addr, int port) {
        long rcvTs = currentTime();

        if (!msg.originatingNodeId().equals(ctx.localNodeId())) {
            // We received time request from remote node, set current time and reply back.
            msg.replyTimestamp(rcvTs);

            try {
                srv.sendPacket(msg, addr, port);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send time server reply to remote node: " + msg, e);
            }
        }
        else
            timeCoord.onMessage(msg, rcvTs);
    }

    /**
     * Checks if local node is the oldest node in topology and starts time coordinator if so.
     *
     * @param discoEvt Discovery event.
     */
    private void checkLaunchCoordinator(DiscoveryEvent discoEvt) {
        rw.readLock();

        try {
            if (stopping)
                return;

            if (timeCoord == null) {
                long minNodeOrder = Long.MAX_VALUE;

                Collection<ClusterNode> nodes = discoEvt.topologyNodes();

                for (ClusterNode node : nodes) {
                    if (node.order() < minNodeOrder)
                        minNodeOrder = node.order();
                }

                ClusterNode locNode = ctx.discovery().localNode();

                if (locNode.order() == minNodeOrder) {
                    if (log.isDebugEnabled())
                        log.debug("Detected local node to be the eldest node in topology, starting time " +
                            "coordinator thread [discoEvt=" + discoEvt + ", locNode=" + locNode + ']');

                    synchronized (this) {
                        if (timeCoord == null && !stopping) {
                            timeCoord = new TimeCoordinator(discoEvt);

                            IgniteThread th = new IgniteThread(timeCoord);

                            th.setPriority(Thread.MAX_PRIORITY);

                            th.start();
                        }
                    }
                }
            }
        }
        finally {
            rw.readUnlock();
        }
    }

    /**
     * Gets time adjusted with time coordinator on given topology version.
     *
     * @param topVer Topology version.
     * @return Adjusted time.
     */
    public long adjustedTime(long topVer) {
        T2<GridClockDeltaVersion, GridClockDeltaSnapshot> fastSnap = lastSnapshot;

        GridClockDeltaSnapshot snap;

        if (fastSnap != null && fastSnap.get1().topologyVersion() == topVer)
            snap = fastSnap.get2();
        else {
            // Get last synchronized time on given topology version.
            Map.Entry<GridClockDeltaVersion, GridClockDeltaSnapshot> entry = timeSyncHistory().lowerEntry(
                new GridClockDeltaVersion(0, topVer + 1));

            snap = entry == null ? null : entry.getValue();
        }

        long now = clockSrc.currentTimeMillis();

        if (snap == null)
            return now;

        Long delta = snap.deltas().get(ctx.localNodeId());

        if (delta == null)
            delta = 0L;

        return now + delta;
    }

    /**
     * Publishes snapshot to topology.
     *
     * @param snapshot Snapshot to publish.
     * @param top Topology to send given snapshot to.
     */
    private void publish(GridClockDeltaSnapshot snapshot, GridDiscoveryTopologySnapshot top) {
        if (!rw.tryReadLock())
            return;

        try {
            lastSnapshot = new T2<>(snapshot.version(), snapshot);

            timeSyncHist.put(snapshot.version(), snapshot);

            for (ClusterNode n : top.topologyNodes()) {
                GridClockDeltaSnapshotMessage msg = new GridClockDeltaSnapshotMessage(
                    snapshot.version(), snapshot.deltas());

                try {
                    ctx.io().send(n, TOPIC_TIME_SYNC, msg, SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    if (ctx.discovery().pingNodeNoError(n.id()))
                        U.error(log, "Failed to send time sync snapshot to remote node (did not leave grid?) " +
                            "[nodeId=" + n.id() + ", msg=" + msg + ", err=" + e.getMessage() + ']');
                    else if (log.isDebugEnabled())
                        log.debug("Failed to send time sync snapshot to remote node (did not leave grid?) " +
                            "[nodeId=" + n.id() + ", msg=" + msg + ", err=" + e.getMessage() + ']');
                }
            }
        }
        finally {
            rw.readUnlock();
        }
    }

    /**
     * Time coordinator thread.
     */
    private class TimeCoordinator extends GridWorker {
        /** Last discovery topology snapshot. */
        private volatile GridDiscoveryTopologySnapshot lastSnapshot;

        /** Snapshot being constructed. May be not null only on coordinator node. */
        private volatile GridClockDeltaSnapshot pendingSnapshot;

        /** Version counter. */
        private long verCnt = 1;

        /**
         * Time coordinator thread constructor.
         *
         * @param evt Discovery event on which this node became a coordinator.
         */
        protected TimeCoordinator(DiscoveryEvent evt) {
            super(ctx.gridName(), "grid-time-coordinator", GridClockSyncProcessor.this.log);

            lastSnapshot = new GridDiscoveryTopologySnapshot(evt.topologyVersion(), evt.topologyNodes());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                GridDiscoveryTopologySnapshot top = lastSnapshot;

                if (log.isDebugEnabled())
                    log.debug("Creating time sync snapshot for topology: " + top);

                GridClockDeltaSnapshot snapshot = new GridClockDeltaSnapshot(
                    new GridClockDeltaVersion(verCnt++, top.topologyVersion()),
                    ctx.localNodeId(),
                    top,
                    ctx.config().getClockSyncSamples());

                pendingSnapshot = snapshot;

                while (!snapshot.ready()) {
                    if (log.isDebugEnabled())
                        log.debug("Requesting time from remote nodes: " + snapshot.pendingNodeIds());

                    for (UUID nodeId : snapshot.pendingNodeIds())
                        requestTime(nodeId);

                    if (log.isDebugEnabled())
                        log.debug("Waiting for snapshot to be ready: " + snapshot);

                    // Wait for all replies
                    snapshot.awaitReady(1000);
                }

                // No more messages should be processed.
                pendingSnapshot = null;

                if (log.isDebugEnabled())
                    log.debug("Collected time sync results: " + snapshot.deltas());

                publish(snapshot, top);

                synchronized (this) {
                    if (top.topologyVersion() == lastSnapshot.topologyVersion())
                        wait(ctx.config().getClockSyncFrequency());
                }
            }
        }

        /**
         * @param evt Discovery event.
         */
        public void onDiscoveryEvent(DiscoveryEvent evt) {
            if (log.isDebugEnabled())
                log.debug("Processing discovery event: " + evt);

            if (evt.type() == EventType.EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT)
                onNodeLeft(evt.eventNode().id());

            synchronized (this) {
                lastSnapshot = new GridDiscoveryTopologySnapshot(evt.topologyVersion(), evt.topologyNodes());

                notifyAll();
            }
        }

        /**
         * @param msg Message received from remote node.
         * @param rcvTs Receive timestamp.
         */
        private void onMessage(GridClockMessage msg, long rcvTs) {
            GridClockDeltaSnapshot curr = pendingSnapshot;

            if (curr != null) {
                long delta = (msg.originatingTimestamp() + rcvTs) / 2 - msg.replyTimestamp();

                boolean needMore = curr.onDeltaReceived(msg.targetNodeId(), delta);

                if (needMore)
                    requestTime(msg.targetNodeId());
            }
        }

        /**
         * Requests time from remote node.
         *
         * @param rmtNodeId Remote node ID.
         */
        private void requestTime(UUID rmtNodeId) {
            ClusterNode node = ctx.discovery().node(rmtNodeId);

            if (node != null) {
                InetAddress addr = node.attribute(ATTR_TIME_SERVER_HOST);
                int port = node.attribute(ATTR_TIME_SERVER_PORT);

                try {
                    GridClockMessage req = new GridClockMessage(ctx.localNodeId(), rmtNodeId, currentTime(), 0);

                    srv.sendPacket(req, addr, port);
                }
                catch (IgniteCheckedException e) {
                    LT.error(log, e, "Failed to send time request to remote node [rmtNodeId=" + rmtNodeId +
                        ", addr=" + addr + ", port=" + port + ']');
                }
            }
            else
                onNodeLeft(rmtNodeId);
        }

        /**
         * Node left callback.
         *
         * @param nodeId Left node ID.
         */
        private void onNodeLeft(UUID nodeId) {
            GridClockDeltaSnapshot curr = pendingSnapshot;

            if (curr != null)
                curr.onNodeLeft(nodeId);
        }
    }
}
