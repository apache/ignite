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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import sun.reflect.generics.tree.Tree;

/**
 *
 */
public class ZKClusterNodeNew implements Watcher {
    /** */
    private static final String CLUSTER_PATH = "/cluster";

    /** */
    private static final String EVENTS_PATH = CLUSTER_PATH + "/events";

    /** */
    private static final String JOIN_HIST_PATH = CLUSTER_PATH + "/joinHist";

    /** */
    private static final String ALIVE_NODES_PATH = CLUSTER_PATH + "/alive";

    /** */
    private ZooKeeper zk;

    /** */
    private final NodesUpdateCallback nodesUpdateCallback;

    /** */
    private final DataUpdateCallback dataUpdateCallback;

    /** */
    private final String nodeName;

    /** */
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    /** */
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final String nodeId;

    /** */
    private static final int ID_LEN = 1;

    static class Node {
        final String name;

        Node(String name) {
            this.name = name;
        }
    }

    /** */
    static int nodeIdGen;

    public ZKClusterNodeNew(String nodeName) {
        this.nodeName = nodeName;

        nodesUpdateCallback = new NodesUpdateCallback();
        dataUpdateCallback = new DataUpdateCallback();

        nodeId = String.valueOf(nodeIdGen++);//UUID.randomUUID().toString();
    }

    private void log(String msg) {
        System.out.println(nodeName + ": " + msg);
    }

    @Override public void process(WatchedEvent event) {
        log("Process event [type=" + event.getType() + ", state=" + event.getState() + ", path=" + event.getPath() + ']');

        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            zk.getChildren(event.getPath(), true, nodesUpdateCallback, null);
        } else if (event.getType() == Event.EventType.NodeDataChanged) {
            zk.getData(event.getPath(), true, dataUpdateCallback, null);
        }
    }

    /**
     *
     */
    static class NodeData {
        /** */
        final int order;

        /** */
        final String nodeId;

        NodeData(int order, String nodeId) {
            this.order = order;
            this.nodeId = nodeId;
        }
    }

    /**
     *
     */
    static class AliveNodes {
        /** */
        final int ver;

        /** */
        final TreeMap<Integer, NodeData> nodesByOrder;

        /** */
        final TreeMap<String, NodeData> nodesById;

        /**
         * @param ver
         * @param nodesByOrder
         */
        AliveNodes(int ver, TreeMap<Integer, NodeData> nodesByOrder) {
            this.ver = ver;
            this.nodesByOrder = nodesByOrder;

            nodesById = new TreeMap<>();

            for (NodeData nodeData : nodesByOrder.values())
                nodesById.put(nodeData.nodeId, nodeData);
        }
    }

    /** */
    private Map<Integer, NodeData> joinHist = new HashMap<>();

    /** */
    private boolean crd;

    /** */
    private final JdkMarshaller jdkMarshaller = new JdkMarshaller();

    /** */
    private AliveNodes curAlive;

    /**
     *
     */
    static class DiscoveryEvents {
        /** */
        final AliveNodes aliveNodes;

        /** */
        final TreeMap<Integer, DiscoveryEvent> evts;

        int ver;

        DiscoveryEvents(AliveNodes aliveNodes, TreeMap<Integer, DiscoveryEvent> evts) {
            this.aliveNodes = aliveNodes;
            this.evts = evts;
        }
    }

    /**
     *
     */
    static class DiscoveryEvent {
        /** */
        @GridToStringInclude
        final DiscoveryEventType evtType;

        /** */
        @GridToStringInclude
        final String nodeId;

        /** */
        @GridToStringInclude
        final int topVer;

        DiscoveryEvent(DiscoveryEventType evtType, int topVer, String nodeId) {
            this.evtType = evtType;
            this.topVer = topVer;
            this.nodeId = nodeId;
        }

        @Override public String toString() {
            return S.toString(DiscoveryEvent.class, this);
        }
    }

    /**
     *
     */
    enum DiscoveryEventType {
        /** */
        NODE_FAILED,

        /** */
        NODE_JOINED
    }

    /**
     *
     */
    class NodesUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (children == null || children.isEmpty())
                return;

            if (path.equals(JOIN_HIST_PATH)) {
                log("Join nodes changed [rc=" + rc + ", path=" + path + ", nodes=" + children + ", ver=" + (stat != null ? stat.getCversion() : null) + ']');

                for (String child : children) {
                    NodeData data = parseNodePath(child);

                    joinHist.put(data.order, data);
                }
            }
            else if (path.equals(ALIVE_NODES_PATH)) {
                log("Alive nodes changed [rc=" + rc + ", path=" + path + ", nodes=" + children + ", ver=" + (stat != null ? stat.getCversion() : null) + ']');

                assert stat != null;

                TreeMap<Integer, NodeData> nodes = new TreeMap<>();

                for (String child : children) {
                    NodeData data = parseNodePath(child);

                    nodes.put(data.order, data);
                }

                AliveNodes newAlive = new AliveNodes(stat.getCversion(), nodes);

                generateEvents(curAlive, newAlive);

                curAlive = newAlive;

                if (connectLatch.getCount() > 0 && newAlive.nodesById.containsKey(nodeId))
                    connectLatch.countDown();
            }
        }
    }

    private void generateEvents(AliveNodes oldNodes, AliveNodes newNodes) {
        assert newNodes != null;

        if (oldNodes == null) {
            NodeData locNode = newNodes.nodesById.get(nodeId);

            if (locNode == null)
                return;

            if (newNodes.nodesByOrder.firstKey() == locNode.order) {
                log("Coordinator joined");

                curCrdEvts = curEvts;

                crd = true;
            }

            return;
        }

        if (!crd)
            return;

        if (oldNodes.ver == newNodes.ver)
            return;

        int nextJoinOrder = oldNodes.nodesByOrder.lastKey() + 1;

        TreeMap<Integer, DiscoveryEvent> evts = new TreeMap<>();

        Set<Integer> failed = new HashSet<>();

        for (int v = oldNodes.ver + 1; v <= newNodes.ver; v++) {
            NodeData data = joinHist.get(nextJoinOrder);

            if (data != null) {
                evts.put(v, new DiscoveryEvent(DiscoveryEventType.NODE_JOINED, v, data.nodeId));

                if (!newNodes.nodesByOrder.containsKey(data.order)) {
                    v++;

                    evts.put(v, new DiscoveryEvent(DiscoveryEventType.NODE_FAILED, v, data.nodeId));
                }

                nextJoinOrder++;
            }
            else {
                for (NodeData oldData : oldNodes.nodesByOrder.values()) {
                    if (!failed.contains(oldData.order) && !newNodes.nodesByOrder.containsKey(oldData.order)) {
                        failed.add(oldData.order);

                        evts.put(v, new DiscoveryEvent(DiscoveryEventType.NODE_FAILED, v, oldData.nodeId));
                    }
                }
            }
        }

        log("Generate events on coordinator: " + evts);

        DiscoveryEvents newEvents;

        int expVer;

        if (curCrdEvts == null) {
            expVer = 0;

            newEvents = new DiscoveryEvents(newNodes, evts);
        }
        else {
            TreeMap<Integer, DiscoveryEvent> evts0 = new TreeMap<>(curCrdEvts.evts);

            evts0.putAll(evts);

            newEvents = new DiscoveryEvents(newNodes, evts);

            expVer = curCrdEvts.ver;

            newEvents.ver = expVer + 1;
        }

        try {
            zk.setData(EVENTS_PATH, marshal(newEvents), expVer);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        curCrdEvts = newEvents;
    }

    static NodeData parseNodePath(String path) {
        String nodeId = path.substring(0, ID_LEN);
        int nodeOrder = Integer.parseInt(path.substring(ID_LEN + 1));

        return new NodeData(nodeOrder, nodeId);
    }

    /** */
    private DiscoveryEvents curEvts;

    /** */
    private DiscoveryEvents curCrdEvts;

    /**
     *
     */
    class DataUpdateCallback implements AsyncCallback.DataCallback {
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            log("Data changed [path=" + path + ", ver=" + (stat != null ? stat.getVersion() : null) + ']');

            if (data.length == 0)
                return;

            if (path.equals(EVENTS_PATH)) {
                assert stat != null;

                DiscoveryEvents newEvts = unmarshal(data);

                newEvts.ver = stat.getVersion();

                for (DiscoveryEvent e : newEvts.evts.values())
                    log("Event update: " + e)

                curEvts = newEvts;
            }
        }
    }

    private <T> T unmarshal(byte[] data) {
        try {
            return jdkMarshaller.unmarshal(data, null);
        }
        catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    private byte[] marshal(Object obj) {
        try {
            return jdkMarshaller.marshal(obj);
        }
        catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    public void join(String connectString) throws Exception {
        log("Start connect " + connectString);

        try {
            zk = new ZooKeeper(connectString, 5000, this);

            if (zk.exists(CLUSTER_PATH, false) == null) {
                List<Op> initOps = new ArrayList<>();

                initOps.add(Op.create(CLUSTER_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(JOIN_HIST_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(ALIVE_NODES_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                initOps.add(Op.create(EVENTS_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

                zk.multi(initOps);
            }

            zk.getData(EVENTS_PATH, true, dataUpdateCallback, null);
            zk.getChildren(ALIVE_NODES_PATH, true, nodesUpdateCallback, null);
            zk.getChildren(JOIN_HIST_PATH, true, nodesUpdateCallback, null);

            log("Start join: " + nodeId);

            List<Op> joinOps = new ArrayList<>();

            byte[] nodeData = nodeName.getBytes(UTF8);

            String zkNode = "/" + nodeId + "-";

            joinOps.add(Op.create(JOIN_HIST_PATH + zkNode, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL));
            joinOps.add(Op.create(ALIVE_NODES_PATH + zkNode, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));

            List<OpResult> res = zk.multi(joinOps);

            connectLatch.await();

            System.out.println("Node joined: " + nodeId);
        } catch (Exception e) {
            log("Connect failed: " + e);

            e.printStackTrace(System.out);
        }
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
        new ZKClusterNodeNew(args[0]).join(args[1]);

        Thread.sleep(Long.MAX_VALUE);
    }
}
