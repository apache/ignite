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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;

/**
 *
 */
class ZkCommunicationErrorProcessFuture extends GridFutureAdapter implements IgniteSpiTimeoutObject, Runnable {
    /** */
    private final ZookeeperDiscoveryImpl impl;

    /** */
    private final Map<UUID, GridFutureAdapter<Boolean>> errNodes = new HashMap<>();

    /** */
    private final long endTime;

    /** */
    private final IgniteUuid id;

    /**
     * @param impl Discovery implementation.
     * @param timeout Wait timeout before initiating communication errors resolve.
     */
    ZkCommunicationErrorProcessFuture(ZookeeperDiscoveryImpl impl, long timeout) {
        this.impl = impl;

        id = IgniteUuid.fromUuid(impl.localNode().id());

        endTime = System.currentTimeMillis() + timeout;
    }

    /**
     * @param nodeId Node ID.
     * @return Future finished when communication error resolve is done.
     */
    GridFutureAdapter<Boolean> nodeStatusFuture(UUID nodeId) {
        GridFutureAdapter<Boolean> fut;

        // TODO ZK: finish race.

        synchronized (this) {
            fut = errNodes.get(nodeId);

            if (fut == null)
                errNodes.put(nodeId, fut = new GridFutureAdapter<>());
        }

        if (impl.node(nodeId) == null)
            fut.onDone(false);

        return fut;
    }

    /**
     * @param nodeId Node ID.
     */
    void onNodeFailed(UUID nodeId) {
        GridFutureAdapter<Boolean> fut;

        synchronized (this) {
            fut = errNodes.get(nodeId);
        }

        if (fut != null)
            fut.onDone(false);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (checkNotDoneOnTimeout()) {
            try {
                impl.sendCustomMessage(new ZkInternalCommunicationErrorMessage());
            }
            catch (Exception e) {
                onError(e);
            }
        }
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
        if (isDone())
            return;

        if (checkNotDoneOnTimeout())
            impl.runInWorkerThread(this);
    }

    /**
     * @param e Error.
     */
    private void onError(Exception e) {
        List<GridFutureAdapter<Boolean>> futs;

        synchronized (this) {
            futs = new ArrayList<>(errNodes.values());
        }

        for (GridFutureAdapter<Boolean> fut : futs)
            fut.onDone(e);

        onDone(e);
    }

    /**
     * @return {@code True} if future already finished.
     */
    private boolean checkNotDoneOnTimeout() {
        // TODO ZK check state.
        synchronized (this) {
            for (GridFutureAdapter<Boolean> fut : errNodes.values()) {
                if (!fut.isDone())
                    return false;
            }
        }

        onDone(null);

        return true;
    }
}
