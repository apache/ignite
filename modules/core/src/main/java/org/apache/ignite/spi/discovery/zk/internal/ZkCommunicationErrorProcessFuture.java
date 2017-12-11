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

import java.util.UUID;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 *
 */
class ZkCommunicationErrorProcessFuture extends GridFutureAdapter implements IgniteSpiTimeoutObject, Runnable {
    /** */
    private final ZookeeperDiscoveryImpl impl;

    /** */
    private final ConcurrentHashMap<UUID, GridFutureAdapter<Boolean>> errNodes = new ConcurrentHashMap<>();

    /** */
    private final long endTime;

    /** */
    private final IgniteUuid id;

    /**
     * @param impl Discovery implementation.
     */
    ZkCommunicationErrorProcessFuture(ZookeeperDiscoveryImpl impl, long timeout) {
        this.impl = impl;

        id = IgniteUuid.fromUuid(impl.localNode().id());

        endTime = System.currentTimeMillis() + timeout;
    }

    GridFutureAdapter<Boolean> nodeStatusFuture(UUID nodeId) {
        GridFutureAdapter<Boolean> fut = errNodes.get(nodeId);

        if (fut == null) {
            GridFutureAdapter<Boolean> old = errNodes.putIfAbsent(nodeId, fut = new GridFutureAdapter<>());

            if (old != null)
                fut = old;
        }

        if (impl.node(nodeId) == null)
            fut.onDone(false);

        return fut;
    }

    void onNodeFailed(UUID nodeId) {
        GridFutureAdapter<Boolean> fut = errNodes.get(nodeId);

        if (fut != null)
            fut.onDone(false);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        // TODO ZK
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return null;
    }

    @Override public long endTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        // TODO ZK
    }
}
