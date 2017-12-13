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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkCommunicationErrorProcessFuture extends GridFutureAdapter<Void> implements IgniteSpiTimeoutObject, Runnable {
    /** */
    private final ZookeeperDiscoveryImpl impl;

    /** */
    private final Map<Long, GridFutureAdapter<Boolean>> nodeFuts = new HashMap<>();

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
    private ZkCollectDistributedFuture nodeResFut;

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

    void nodeResultCollectFuture(ZkCollectDistributedFuture nodeResFut) {
        assert nodeResFut == null : nodeResFut;

        this.nodeResFut = nodeResFut;
    }

    void pingNodesAndNotifyFuture(long locNodeOrder, ZkRuntimeState rtState, String futPath, Collection<ClusterNode> nodes)
        throws Exception {
        ZkCollectDistributedFuture.saveNodeResult(futPath, rtState.zkClient, locNodeOrder, null);
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

    /**
     * @param node Failed node.
     */
    void onNodeFailed(ClusterNode node) {
        GridFutureAdapter<Boolean> fut = null;

        synchronized (this) {
            if (state == State.WAIT_TIMEOUT)
                fut = nodeFuts.get(node.order());
        }

        if (fut != null)
            fut.onDone(false);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        // Run from zk discovery worker pool after timeout.
        if (processTimeout()) {
            try {
                impl.sendCustomMessage(new ZkCommunicationErrorResolveStartMessage(UUID.randomUUID()));
            }
            catch (Exception e) {
                Collection<GridFutureAdapter<Boolean>> futs;

                synchronized (this) {
                    if (state != State.WAIT_TIMEOUT)
                        return;

                    state = State.DONE;

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
    private boolean processTimeout() {
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
        if (processTimeout())
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
