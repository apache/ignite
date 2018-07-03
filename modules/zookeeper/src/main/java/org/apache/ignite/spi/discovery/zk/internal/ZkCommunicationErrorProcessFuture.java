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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Future is created on each node when either connection error occurs or resolve communication error request
 * received.
 */
class ZkCommunicationErrorProcessFuture extends GridFutureAdapter<Void> implements IgniteSpiTimeoutObject, Runnable {
    /** */
    private final ZookeeperDiscoveryImpl impl;

    /** */
    private final IgniteLogger log;

    /** */
    private final Map<Long, GridFutureAdapter<Boolean>> nodeFuts = new ConcurrentHashMap<>();

    /** */
    private final long endTime;

    /** */
    private final IgniteUuid id;

    /** */
    private State state;

    /** */
    private long resolveTopVer;

    /** */
    private Set<Long> resFailedNodes;

    /** */
    private Exception resErr;

    /** */
    private ZkDistributedCollectDataFuture collectResFut;

    /**
     * @param impl Discovery impl.
     * @param timeout Timeout to wait before initiating resolve process.
     * @return Future.
     */
    static ZkCommunicationErrorProcessFuture createOnCommunicationError(ZookeeperDiscoveryImpl impl, long timeout) {
        return new ZkCommunicationErrorProcessFuture(impl, State.WAIT_TIMEOUT, timeout);
    }

    /**
     * @param impl Discovery impl.
     * @return Future.
     */
    static ZkCommunicationErrorProcessFuture createOnStartResolveRequest(ZookeeperDiscoveryImpl impl) {
        return new ZkCommunicationErrorProcessFuture(impl, State.RESOLVE_STARTED, 0);
    }

    /**
     * @param impl Discovery implementation.
     * @param state Initial state.
     * @param timeout Wait timeout before initiating communication errors resolve.
     */
    private ZkCommunicationErrorProcessFuture(ZookeeperDiscoveryImpl impl, State state, long timeout) {
        assert state != State.DONE;

        this.impl = impl;
        this.log = impl.log();

        if (state == State.WAIT_TIMEOUT) {
            assert timeout > 0 : timeout;

            id = IgniteUuid.fromUuid(impl.localNode().id());
            endTime = System.currentTimeMillis() + timeout;
        }
        else {
            id = null;
            endTime = 0;
        }

        this.state = state;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteLogger logger() {
        return log;
    }

    /**
     * @param collectResFut Collect nodes' communication status future.
     */
    void nodeResultCollectFuture(ZkDistributedCollectDataFuture collectResFut) {
        assert this.collectResFut == null : collectResFut;

        this.collectResFut = collectResFut;
    }

    /**
     * @param top Topology.
     * @throws Exception If failed.
     */
    void onTopologyChange(ZkClusterNodes top) throws Exception {
        for (Map.Entry<Long, GridFutureAdapter<Boolean>> e : nodeFuts.entrySet()) {
            if (!top.nodesByOrder.containsKey(e.getKey()))
                e.getValue().onDone(false);
        }

        if (collectResFut != null)
            collectResFut.onTopologyChange(top);
    }

    /**
     * @param rtState Runtime state.
     * @param futPath Future path.
     * @param nodes Nodes to ping.
     */
    void checkConnection(final ZkRuntimeState rtState, final String futPath, List<ClusterNode> nodes) {
        final TcpCommunicationSpi spi = (TcpCommunicationSpi)impl.spi.ignite().configuration().getCommunicationSpi();

        IgniteFuture<BitSet> fut = spi.checkConnection(nodes);

        fut.listen(new IgniteInClosure<IgniteFuture<BitSet>>() {
            @Override public void apply(final IgniteFuture<BitSet> fut) {
                // Future completed either from NIO thread or timeout worker, save result from another thread.
                impl.runInWorkerThread(new ZkRunnable(rtState, impl) {
                    @Override public void run0() throws Exception {
                        BitSet commState = null;
                        Exception err = null;

                        try {
                            commState = fut.get();
                        }
                        catch (Exception e) {
                            err = e;
                        }

                        ZkCommunicationErrorNodeState state = new ZkCommunicationErrorNodeState(commState, err);

                        ZkDistributedCollectDataFuture.saveNodeResult(futPath,
                            rtState.zkClient,
                            impl.localNode().order(),
                            impl.marshalZip(state));
                    }

                    @Override void onStartFailed() {
                        onError(rtState.errForClose);
                    }
                });

            }
        });
    }

    /**
     *
     */
    void scheduleCheckOnTimeout() {
        synchronized (this) {
            if (state == State.WAIT_TIMEOUT)
                impl.spi.getSpiContext().addTimeoutObject(this);
        }
    }

    /**
     * @param topVer Topology version.
     * @return {@code False} if future was already completed and need create another future instance.
     */
    boolean onStartResolveRequest(long topVer) {
        synchronized (this) {
            if (state == State.DONE)
                return false;

            if (state == State.WAIT_TIMEOUT)
                impl.spi.getSpiContext().removeTimeoutObject(this);

            assert resolveTopVer == 0 : resolveTopVer;

            resolveTopVer = topVer;

            state = State.RESOLVE_STARTED;
        }

        return true;
    }

    /**
     * @param err Error.
     */
    void onError(Exception err) {
        assert err != null;

        Map<Long, GridFutureAdapter<Boolean>> futs;

        synchronized (this) {
            if (state == State.DONE) {
                assert resErr != null;

                return;
            }

            state = State.DONE;

            resErr = err;

            futs = nodeFuts; // nodeFuts should not be modified after state changed to DONE.
        }

        for (Map.Entry<Long, GridFutureAdapter<Boolean>> e : futs.entrySet())
            e.getValue().onDone(err);

        onDone(err);
    }

    /**
     * @param failedNodes Node failed as result of resolve process.
     */
    void onFinishResolve(Set<Long> failedNodes) {
        Map<Long, GridFutureAdapter<Boolean>> futs;

        synchronized (this) {
            if (state == State.DONE) {
                assert resErr != null;

                return;
            }

            assert state == State.RESOLVE_STARTED : state;

            state = State.DONE;

            resFailedNodes = failedNodes;

            futs = nodeFuts; // nodeFuts should not be modified after state changed to DONE.
        }

        for (Map.Entry<Long, GridFutureAdapter<Boolean>> e : futs.entrySet()) {
            Boolean res = !F.contains(resFailedNodes, e.getKey());

            e.getValue().onDone(res);
        }

        onDone();
    }

    /**
     * @param node Node.
     * @return Future finished when communication error resolve is done or {@code null} if another
     *      resolve process should be started.
     */
    @Nullable IgniteInternalFuture<Boolean> nodeStatusFuture(ClusterNode node) {
        GridFutureAdapter<Boolean> fut;

        synchronized (this) {
            if (state == State.DONE) {
                if (resolveTopVer != 0 && node.order() <= resolveTopVer) {
                    Boolean res = !F.contains(resFailedNodes, node.order());

                    return new GridFinishedFuture<>(res);
                }
                else
                    return null;
            }

            fut = nodeFuts.get(node.order());

            if (fut == null)
                nodeFuts.put(node.order(), fut = new GridFutureAdapter<>());
        }

        if (impl.node(node.order()) == null)
            fut.onDone(false);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        // Run from zk discovery worker pool after timeout.
        if (needProcessTimeout()) {
            try {
                UUID reqId = UUID.randomUUID();

                if (log.isInfoEnabled()) {
                    log.info("Initiate cluster-wide communication error resolve process [reqId=" + reqId +
                        ", errNodes=" + nodeFuts.size() + ']');
                }

                impl.sendCustomMessage(new ZkCommunicationErrorResolveStartMessage(reqId));
            }
            catch (Exception e) {
                Collection<GridFutureAdapter<Boolean>> futs;

                synchronized (this) {
                    if (state != State.WAIT_TIMEOUT)
                        return;

                    state = State.DONE;
                    resErr = e;

                    futs = nodeFuts.values(); // nodeFuts should not be modified after state changed to DONE.
                }

                for (GridFutureAdapter<Boolean> fut : futs)
                    fut.onDone(e);

                onDone(e);
            }
        }
    }

    /**
     * @return {@code True} if need initiate resolve process after timeout expired.
     */
    private boolean needProcessTimeout() {
        synchronized (this) {
            if (state != State.WAIT_TIMEOUT)
                return false;

            for (GridFutureAdapter<Boolean> fut : nodeFuts.values()) {
                if (!fut.isDone())
                    return true;
            }

            state = State.DONE;
        }

        onDone(null, null);

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (needProcessTimeout())
            impl.runInWorkerThread(this);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            impl.clearCommunicationErrorProcessFuture(this);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZkCommunicationErrorProcessFuture.class, this);
    }

    /**
     *
     */
    enum State {
        /** */
        DONE,

        /** */
        WAIT_TIMEOUT,

        /** */
        RESOLVE_STARTED
    }
}
