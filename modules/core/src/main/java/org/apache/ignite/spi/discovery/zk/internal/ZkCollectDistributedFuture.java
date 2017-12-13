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
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkCollectDistributedFuture extends GridFutureAdapter<Void> {
    /** */
    private final IgniteLogger log;

    /** */
    private final String futPath;

    /** */
    private final ZookeeperDiscoveryImpl impl;

    /** */
    private final Set<Long> remainingNodes;

    /** */
    private final Callable<Void> lsnr;

    /**
     * @param impl
     * @param rtState
     * @param futPath
     */
    ZkCollectDistributedFuture(ZookeeperDiscoveryImpl impl, ZkRuntimeState rtState, String futPath, Callable<Void> lsnr) throws Exception {
        this.impl = impl;
        this.log = impl.log();
        this.futPath = futPath;
        this.lsnr = lsnr;

        ZkClusterNodes top = impl.nodes();

        remainingNodes = U.newHashSet(top.nodesByOrder.size());

        for (ZookeeperClusterNode node : top.nodesByInternalId.values())
            remainingNodes.add(node.order());

        NodeResultsWatcher watcher = new NodeResultsWatcher(rtState, impl);

        if (remainingNodes.isEmpty())
            completeAndNotifyListener();
        else
            rtState.zkClient.getChildrenAsync(futPath, watcher, watcher);
    }

    /**
     * @throws Exception If listener call failed.
     */
    private void completeAndNotifyListener() throws Exception {
        if (super.onDone())
            lsnr.call();
    }

    /**
     * @param futPath
     * @param client
     * @param nodeOrder
     * @param data
     * @throws Exception If failed.
     */
    static void saveNodeResult(String futPath, ZookeeperClient client, long nodeOrder, byte[] data) throws Exception {
        client.createIfNeeded(futPath + "/" + nodeOrder, data, CreateMode.PERSISTENT);
    }

    /**
     * @param node Failed node.
     */
    void onNodeFail(ZookeeperClusterNode node) throws Exception {
        long nodeOrder = node.order();

        if (remainingNodes.remove(nodeOrder)) {
            int remaining = remainingNodes.size();

            if (log.isInfoEnabled()) {
                log.info("ZkCollectDistributedFuture removed remaining failed node [node=" + nodeOrder +
                    ", remaining=" + remaining +
                    ", futPath=" + futPath + ']');
            }

            if (remaining == 0)
                completeAndNotifyListener();
        }
    }

    /**
     *
     */
    class NodeResultsWatcher extends ZkAbstractWatcher implements AsyncCallback.Children2Callback {
        /**
         * @param rtState Runtime state.
         * @param impl Discovery impl.
         */
        NodeResultsWatcher(ZkRuntimeState rtState, ZookeeperDiscoveryImpl impl) {
            super(rtState, impl);
        }

        /** {@inheritDoc} */
        @Override protected void process0(WatchedEvent evt) {
            if (evt.getType() == Watcher.Event.EventType.NodeChildrenChanged)
                impl.zkClient().getChildrenAsync(evt.getPath(), this, this);
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (!onProcessStart())
                return;

            try {
                assert rc == 0 : KeeperException.Code.get(rc);

                if (isDone())
                    return;

                for (int i = 0; i < children.size(); i++) {
                    Long nodeOrder = Long.parseLong(children.get(i));

                    if (remainingNodes.remove(nodeOrder)) {
                        int remaining = remainingNodes.size();

                        if (log.isInfoEnabled()) {
                            log.info("ZkCollectDistributedFuture added new result [node=" + nodeOrder +
                                ", remaining=" + remaining +
                                ", futPath=" + path + ']');
                        }

                        if (remaining == 0)
                            completeAndNotifyListener();
                    }
                }

                onProcessEnd();
            }
            catch (Throwable e) {
                onProcessError(e);
            }
        }
    }
}
