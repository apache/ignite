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

package org.apache.ignite.spi.discovery.tcp.ipfinder.zk;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 *
 */
public class ZKClusterNode implements Watcher {
    /** */
    static final String CLUSTER_PATH = "/cluster";

    /** */
    private ZooKeeper zk;

    /** */
    volatile String nodePath;

    /** */
    List<String> curNodes;

    /** */
    private NodesUpdateCallback nodesUpdateCallback;

    /** */
    final String nodeName;

    /** */
    final CountDownLatch connectLatch = new CountDownLatch(1);

    public ZKClusterNode(String nodeName) {
        this.nodeName = nodeName;

        nodesUpdateCallback = new NodesUpdateCallback();
    }

    private void log(String msg) {
        System.out.println(nodeName + ": " + msg);
    }

    @Override public void process(WatchedEvent event) {
        log("Process event [type=" + event.getType() + ", state=" + event.getState() + ", path=" + event.getPath() + ']');

        if (event.getType() == Event.EventType.NodeChildrenChanged && event.getPath().equals(CLUSTER_PATH)) {
            zk.getChildren(CLUSTER_PATH, true, nodesUpdateCallback, null);
        }
    }

    /**
     *
     */
    class NodesUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            log("Nodes changed: " + children);

            curNodes = children;
        }
    }

    public void join(String connectString) throws Exception {
        log("Start connect " + connectString);

        zk = new ZooKeeper(connectString, 1000, this);

        if (zk.exists(CLUSTER_PATH, false) == null) {
            try {
                zk.create(CLUSTER_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (KeeperException.NodeExistsException e) {
                // Ignore.
            }
        }

        zk.getChildren(CLUSTER_PATH, true, nodesUpdateCallback, null);


        zk.create(CLUSTER_PATH + "/node-", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
            new AsyncCallback.StringCallback() {
                @Override public void processResult(int rc, String path, Object ctx, String name) {
                    nodePath = name;

                    log("Node created: " + name);

//                        if (name.endsWith("0000000001")) {
//                            try {
//                                Thread.sleep(10_000);
//                            }
//                            catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }

                    connectLatch.countDown();
                }
            },
            null);

        connectLatch.await();
    }

    /**
     *
     */
    public void stop() {
        try {
            if (zk != null)
                zk.close();
        }
        catch (Exception e) {
            log("Closed failed: " + e);
        }
    }

    public static void main(String[] args) throws Exception {
        new ZKClusterNode(args[0]).join(args[1]);

        Thread.sleep(Long.MAX_VALUE);
    }
}
