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

import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ZkEventAckFuture extends GridFutureAdapter<Void> implements Watcher, AsyncCallback.Children2Callback {
    /** */
    private final IgniteLogger log;

    /** */
    private final ZookeeperDiscoveryImpl impl;

    /** */
    private final Long evtId;

    /** */
    private final String evtPath;

    /** */
    private final int expAcks;

    /** */
    private final Set<Integer> remaininAcks;

    /**
     * @param impl
     * @param evtPath
     * @param evtId
     */
    ZkEventAckFuture(ZookeeperDiscoveryImpl impl, String evtPath, Long evtId) {
        this.impl = impl;
        this.log = impl.log();
        this.evtPath = evtPath;
        this.evtId = evtId;

        ZkClusterNodes top = impl.nodes();

        remaininAcks = U.newHashSet(top.nodesById.size());

        for (ZookeeperClusterNode node : top.nodesByInternalId.values()) {
            if (!node.isLocal())
                remaininAcks.add(node.internalId());
        }

        expAcks = remaininAcks.size();

        if (expAcks == 0)
            onDone();
        else
            impl.zkClient().getChildrenAsync(evtPath, this, this);
    }

    /**
     * @return Event ID.
     */
    Long eventId() {
        return evtId;
    }

    /**
     * @param node Failed node.
     */
    void onNodeFail(ZookeeperClusterNode node) {
        assert !remaininAcks.isEmpty();

        if (remaininAcks.remove(node.internalId()) && remaininAcks.isEmpty())
            onDone();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void process(WatchedEvent evt) {
        if (isDone())
            return;

        if (evt.getType() == Event.EventType.NodeChildrenChanged) {
            if (evtPath.equals(evt.getPath()))
                impl.zkClient().getChildrenAsync(evtPath, this, this);
            else
                U.warn(log, "Received event for unknown path: " + evt.getPath());
        }
    }

    /** {@inheritDoc} */
    @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        assert rc == 0 : rc;

        if (isDone())
            return;

        if (expAcks == stat.getCversion()) {
            log.info("Received expected number of acks [expCnt=" + expAcks + ", cVer=" + stat.getCversion() + ']');

            onDone();
        }
        else {
            for (int i = 0; i < children.size(); i++) {
                Integer nodeInternalId = Integer.parseInt(children.get(i));

                if (remaininAcks.remove(nodeInternalId) && remaininAcks.size() == 0)
                    onDone();
            }
        }
    }
}
