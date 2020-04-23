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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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

/**
 *
 */
class ZkDistributedCollectDataFuture extends GridFutureAdapter<Void> {
    /** */
    private final IgniteLogger log;

    /** */
    private final String futPath;

    /** */
    private final Set<Long> remainingNodes;

    /** */
    private final Callable<Void> lsnr;

    /**
     * @param impl Disovery impl
     * @param rtState Runtime state.
     * @param futPath Future path.
     * @param lsnr Future listener.
     * @throws Exception If listener call failed.
     */
    ZkDistributedCollectDataFuture(
        ZookeeperDiscoveryImpl impl,
        ZkRuntimeState rtState,
        String futPath,
        Callable<Void> lsnr)
        throws Exception
    {
        this.log = impl.log();
        this.futPath = futPath;
        this.lsnr = lsnr;

        ZkClusterNodes top = rtState.top;

        // Assume new nodes can not join while future is in progress.

        remainingNodes = U.newHashSet(top.nodesByOrder.size());

        for (ZookeeperClusterNode node : top.nodesByInternalId.values())
            remainingNodes.add(node.order());

        NodeResultsWatcher watcher = new NodeResultsWatcher(rtState, impl);

        if (remainingNodes.isEmpty())
            completeAndNotifyListener();
        else {
            if (log.isInfoEnabled()) {
                log.info("Initialize data collect future [futPath=" + futPath + ", " +
                    "remainingNodes=" + remainingNodes.size() + ']');
            }

            rtState.zkClient.getChildrenAsync(futPath, watcher, watcher);
        }
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
     * @param futPath
     * @param client
     * @param nodeOrder
     * @return Node result data.
     * @throws Exception If fai.ed
     */
    static byte[] readNodeResult(String futPath, ZookeeperClient client, long nodeOrder) throws Exception {
        return client.getData(futPath + "/" + nodeOrder);
    }

    /**
     * @param futResPath Result path.
     * @param client Client.
     * @param data Result data.
     * @throws Exception If failed.
     */
    static void saveResult(String futResPath, ZookeeperClient client, byte[] data) throws Exception {
        client.createIfNeeded(futResPath, data, CreateMode.PERSISTENT);
    }

    static byte[] readResult(ZookeeperClient client, ZkIgnitePaths paths, UUID futId) throws Exception {
        return client.getData(paths.distributedFutureResultPath(futId));
    }

    /**
     * @param client Client.
     * @param paths Paths utils.
     * @param futId Future ID.
     * @param log Ignite Logger.
     * @throws Exception If failed.
     */
    static void deleteFutureData(ZookeeperClient client,
        ZkIgnitePaths paths,
        UUID futId,
        IgniteLogger log
    ) throws Exception {
        List<String> batch = new LinkedList<>();

        String evtDir = paths.distributedFutureBasePath(futId);

        if (client.exists(evtDir)) {
            batch.addAll(client.getChildrenPaths(evtDir));

            batch.add(evtDir);
        }

        batch.add(paths.distributedFutureResultPath(futId));

        client.deleteAll(batch, -1);
    }

    /**
     * @param top Current topology.
     * @throws Exception If listener call failed.
     */
    void onTopologyChange(ZkClusterNodes top) throws Exception {
        if (remainingNodes.isEmpty())
            return;

        for (Iterator<Long> it = remainingNodes.iterator(); it.hasNext();) {
            Long nodeOrder = it.next();

            if (!top.nodesByOrder.containsKey(nodeOrder)) {
                it.remove();

                int remaining = remainingNodes.size();

                if (log.isInfoEnabled()) {
                    log.info("ZkDistributedCollectDataFuture removed remaining failed node [node=" + nodeOrder +
                        ", remaining=" + remaining +
                        ", futPath=" + futPath + ']');
                }

                if (remaining == 0) {
                    completeAndNotifyListener();

                    break;
                }
            }
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
                rtState.zkClient.getChildrenAsync(evt.getPath(), this, this);
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (!onProcessStart())
                return;

            try {
                if (!isDone()) {
                    assert rc == 0 : KeeperException.Code.get(rc);

                    for (int i = 0; i < children.size(); i++) {
                        Long nodeOrder = Long.parseLong(children.get(i));

                        if (remainingNodes.remove(nodeOrder)) {
                            int remaining = remainingNodes.size();

                            if (log.isInfoEnabled()) {
                                log.info("ZkDistributedCollectDataFuture added new result [node=" + nodeOrder +
                                    ", remaining=" + remaining +
                                    ", futPath=" + path + ']');
                            }

                            if (remaining == 0)
                                completeAndNotifyListener();
                        }
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
