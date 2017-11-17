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
import org.apache.curator.utils.PathUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * TODO ZK: check if compression makes sense.
 */
public class ZookeeperDiscoveryImpl {
    /** */
    private static final String JOIN_DATA_DIR = "joinData";

    /** */
    private static final String ALIVE_NODES_DIR = "alive";

    /** */
    private final String basePath;

    /** */
    private final String clusterName;

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final IgniteLogger log;

    /** */
    private final ZookeeperClusterNode locNode;

    /** */
    private final DiscoverySpiListener lsnr;

    /** */
    private final DiscoverySpiDataExchange exchange;

    /** */
    private ZookeeperClient zkClient;

    /** */
    private final GridFutureAdapter<Void> joinFut = new GridFutureAdapter<>();

    public ZookeeperDiscoveryImpl(IgniteLogger log,
        String basePath,
        String clusterName,
        ZookeeperClusterNode locNode,
        DiscoverySpiListener lsnr,
        DiscoverySpiDataExchange exchange) {
        assert locNode.id() != null : locNode;

        if (F.isEmpty(clusterName))
            throw new IllegalArgumentException("Cluster name is empty.");

        PathUtils.validatePath(basePath);

        this.log = log.getLogger(getClass());
        this.basePath = basePath;
        this.clusterName = clusterName;
        this.locNode = locNode;
        this.lsnr = lsnr;
        this.exchange = exchange;
    }

    /**
     * @param path Relative path.
     * @return Full path.
     */
    private String zkPath(String path) {
        return basePath + "/" + clusterName + "/" + path;
    }

    /**
     * @param igniteInstanceName
     * @param connectString
     * @param sesTimeout
     * @throws InterruptedException If interrupted.
     */
    public void joinTopology(String igniteInstanceName, String connectString, int sesTimeout)
        throws InterruptedException
    {
        DiscoveryDataBag discoDataBag = new DiscoveryDataBag(locNode.id());

        exchange.collect(discoDataBag);

        ZKJoiningNodeData joinData = new ZKJoiningNodeData(locNode, discoDataBag.joiningNodeData());

        byte[] joinDataBytes;

        try {
            joinDataBytes = marshal(joinData);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to marshal joining node data", e);
        }

        try {
            zkClient = new ZookeeperClient(igniteInstanceName,
                log,
                connectString,
                sesTimeout,
                new ConnectionLossListener());
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create Zookeeper client", e);
        }

        try {
            initZkNodes();

            // TODO ZK: handle max size
            zkClient.createIfNeeded(zkPath(JOIN_DATA_DIR) + "/" + locNode.id() + "-", joinDataBytes, EPHEMERAL_SEQUENTIAL);

            zkClient.createIfNeeded(zkPath(ALIVE_NODES_DIR) + "/" + locNode.id() + "-", null, EPHEMERAL_SEQUENTIAL);

            zkClient.getChildrenAsync(zkPath(ALIVE_NODES_DIR), false, new AsyncCallback.Children2Callback() {
                @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    assert rc == 0 : rc;


                }
            }, null);

            for (;;) {
                try {
                    joinFut.get(10_000);

                    break;
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    U.warn(log, "Waiting for local join event [nodeId=" + locNode.id() + ", name=" + igniteInstanceName + ']');
                }
                catch (Exception e) {
                    throw new IgniteSpiException("Failed to join cluster", e);
                }
            }
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
    }

    private void initZkNodes() throws ZookeeperClientFailedException, InterruptedException {
        // TODO ZK: use multi.
        String joinDir = zkPath(JOIN_DATA_DIR);

        if (zkClient.exists(joinDir))
            return; // Assume all others dirs are created.

        zkClient.createIfNeeded(basePath, null, PERSISTENT);

        zkClient.createIfNeeded(zkPath(ALIVE_NODES_DIR), null, PERSISTENT);

        zkClient.createIfNeeded(joinDir, null, PERSISTENT);
    }

    /**
     *
     */
    public void stop() {
        if (zkClient != null)
            zkClient.close();
    }

    /**
     * @param bytes Bytes.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T unmarshal(byte[] bytes) throws IgniteCheckedException {
        assert bytes != null && bytes.length > 0;

        return marsh.unmarshal(bytes, null);
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws IgniteCheckedException If failed.
     */
    private byte[] marshal(Object obj) throws IgniteCheckedException {
        assert obj != null;

        return marsh.marshal(obj);
    }

    /**
     *
     */
    private class ConnectionLossListener implements IgniteRunnable {
        @Override public void run() {

        }
    }

    /**
     *
     */
    private class ZKChildrenUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {

        }
    }

}
