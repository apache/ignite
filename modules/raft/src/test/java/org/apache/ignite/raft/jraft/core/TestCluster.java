/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.TestIgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.concurrent.FixedThreadsExecutorGroup;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cluster for NodeTest
 */
public class TestCluster {
    /**
     * Default election timeout.
     * Important: due to sync disk ops (writing raft meta) during probe request processing this timeout should be high
     * enough to avoid test flakiness.
     */
    private static final int ELECTION_TIMEOUT_MILLIS = 600;

    private static final IgniteLogger LOG = IgniteLogger.forClass(TestCluster.class);

    private final String dataPath;
    private final String name;
    private final List<PeerId> peers;
    private final List<NodeImpl> nodes;
    private final LinkedHashMap<PeerId, MockStateMachine> fsms;
    private final ConcurrentMap<Endpoint, RaftGroupService> serverMap = new ConcurrentHashMap<>();
    private final int electionTimeoutMs;
    private final Lock lock = new ReentrantLock();
    private final Consumer<NodeOptions> optsClo;

    /**
     * These disruptors will be used for all RAFT servers in the cluster.
     */
    private final HashMap<Endpoint, StripedDisruptor<FSMCallerImpl.ApplyTask>> fsmCallerDusruptors = new HashMap<>();

    /**
     * These disruptors will be used for all RAFT servers in the cluster.
     */
    private final HashMap<Endpoint, StripedDisruptor<NodeImpl.LogEntryAndClosure>> nodeDisruptors = new HashMap<>();

    /**
     * These disruptors will be used for all RAFT servers in the cluster.
     */
    private final HashMap<Endpoint, StripedDisruptor<ReadOnlyServiceImpl.ReadIndexEvent>> readOnlyServiceDisruptors = new HashMap<>();

    /**
     * These disruptors will be used for all RAFT servers in the cluster.
     */
    private final HashMap<Endpoint, StripedDisruptor<LogManagerImpl.StableClosureEvent>> logManagerDisruptors = new HashMap<>();

    private List<ExecutorService> executors = new CopyOnWriteArrayList<>();

    private List<FixedThreadsExecutorGroup> fixedThreadsExecutorGroups = new CopyOnWriteArrayList<>();

    private List<Scheduler> schedulers = new CopyOnWriteArrayList<>();

    private JRaftServiceFactory raftServiceFactory = new TestJRaftServiceFactory();

    private LinkedHashSet<PeerId> learners;

    public JRaftServiceFactory getRaftServiceFactory() {
        return this.raftServiceFactory;
    }

    public void setRaftServiceFactory(final JRaftServiceFactory raftServiceFactory) {
        this.raftServiceFactory = raftServiceFactory;
    }

    public LinkedHashSet<PeerId> getLearners() {
        return this.learners;
    }

    public void setLearners(final LinkedHashSet<PeerId> learners) {
        this.learners = learners;
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public TestCluster(final String name, final String dataPath, final List<PeerId> peers) {
        this(name, dataPath, peers, ELECTION_TIMEOUT_MILLIS);
    }

    public TestCluster(final String name, final String dataPath, final List<PeerId> peers,
        final int electionTimeoutMs) {
        this(name, dataPath, peers, new LinkedHashSet<>(), ELECTION_TIMEOUT_MILLIS, null);
    }

    public TestCluster(final String name, final String dataPath, final List<PeerId> peers,
        final LinkedHashSet<PeerId> learners, final int electionTimeoutMs) {
        this(name, dataPath, peers, learners, ELECTION_TIMEOUT_MILLIS, null);
    }

    public TestCluster(final String name, final String dataPath, final List<PeerId> peers,
        final LinkedHashSet<PeerId> learners, final int electionTimeoutMs, @Nullable Consumer<NodeOptions> optsClo) {
        super();
        this.name = name;
        this.dataPath = dataPath;
        this.peers = peers;
        this.nodes = new ArrayList<>(this.peers.size());
        this.fsms = new LinkedHashMap<>(this.peers.size());
        this.electionTimeoutMs = electionTimeoutMs;
        this.learners = learners;
        this.optsClo = optsClo;
    }

    public boolean start(final Endpoint addr) throws Exception {
        return this.start(addr, false, 300);
    }

    public boolean start(final Endpoint addr, final int priority) throws Exception {
        return this.start(addr, false, 300, false, null, null, priority);
    }

    public boolean startLearner(final PeerId peer) throws Exception {
        this.learners.add(peer);
        return this.start(peer.getEndpoint(), false, 300);
    }

    public boolean start(final Endpoint listenAddr, final boolean emptyPeers, final int snapshotIntervalSecs)
        throws IOException {
        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, false);
    }

    public boolean start(final Endpoint listenAddr, final boolean emptyPeers, final int snapshotIntervalSecs,
        final boolean enableMetrics) throws IOException {
        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, enableMetrics, null, null);
    }

    public boolean start(final Endpoint listenAddr, final boolean emptyPeers, final int snapshotIntervalSecs,
        final boolean enableMetrics, final SnapshotThrottle snapshotThrottle) throws IOException {
        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, enableMetrics, snapshotThrottle, null);
    }

    public boolean start(final Endpoint listenAddr, final boolean emptyPeers, final int snapshotIntervalSecs,
        final boolean enableMetrics, final SnapshotThrottle snapshotThrottle,
        final RaftOptions raftOptions) throws IOException {

        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, enableMetrics, snapshotThrottle, raftOptions,
            ElectionPriority.Disabled);
    }

    public boolean start(final Endpoint listenAddr, final boolean emptyPeers, final int snapshotIntervalSecs,
        final boolean enableMetrics, final SnapshotThrottle snapshotThrottle,
        final RaftOptions raftOptions, final int priority) throws IOException {

        this.lock.lock();
        try {
            if (this.serverMap.get(listenAddr) != null) {
                return true;
            }

            final NodeOptions nodeOptions = new NodeOptions();

            nodeOptions.setServerName(listenAddr.toString());

            ExecutorService executor = JRaftUtils.createCommonExecutor(nodeOptions);
            executors.add(executor);
            nodeOptions.setCommonExecutor(executor);
            FixedThreadsExecutorGroup threadsExecutorGroup = JRaftUtils.createAppendEntriesExecutor(nodeOptions);
            fixedThreadsExecutorGroups.add(threadsExecutorGroup);
            nodeOptions.setStripedExecutor(threadsExecutorGroup);
            ExecutorService clientExecutor = JRaftUtils.createClientExecutor(nodeOptions, nodeOptions.getServerName());
            executors.add(clientExecutor);
            nodeOptions.setClientExecutor(clientExecutor);
            Scheduler scheduler = JRaftUtils.createScheduler(nodeOptions);
            schedulers.add(scheduler);
            nodeOptions.setScheduler(scheduler);

            nodeOptions.setElectionTimeoutMs(this.electionTimeoutMs);
            nodeOptions.setEnableMetrics(enableMetrics);
            nodeOptions.setSnapshotThrottle(snapshotThrottle);
            nodeOptions.setSnapshotIntervalSecs(snapshotIntervalSecs);
            nodeOptions.setServiceFactory(this.raftServiceFactory);
            if (raftOptions != null) {
                nodeOptions.setRaftOptions(raftOptions);
            }
            final String serverDataPath = this.dataPath + File.separator + listenAddr.toString().replace(':', '_');
            new File(serverDataPath).mkdirs();
            nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
            nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
            nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");
            nodeOptions.setElectionPriority(priority);

            nodeOptions.setfSMCallerExecutorDisruptor(fsmCallerDusruptors.computeIfAbsent(listenAddr, endpoint -> new StripedDisruptor<>(
                "JRaft-FSMCaller-Disruptor_TestCluster",
                nodeOptions.getRaftOptions().getDisruptorBufferSize(),
                () -> new FSMCallerImpl.ApplyTask(),
                nodeOptions.getStripes())));

            nodeOptions.setNodeApplyDisruptor(nodeDisruptors.computeIfAbsent(listenAddr, endpoint -> new StripedDisruptor<>(
                "JRaft-NodeImpl-Disruptor_TestCluster",
                nodeOptions.getRaftOptions().getDisruptorBufferSize(),
                () -> new NodeImpl.LogEntryAndClosure(),
                nodeOptions.getStripes())));

            nodeOptions.setReadOnlyServiceDisruptor(readOnlyServiceDisruptors.computeIfAbsent(listenAddr, endpoint -> new StripedDisruptor<>(
                "JRaft-ReadOnlyService-Disruptor_TestCluster",
                nodeOptions.getRaftOptions().getDisruptorBufferSize(),
                () -> new ReadOnlyServiceImpl.ReadIndexEvent(),
                nodeOptions.getStripes())));

            nodeOptions.setLogManagerDisruptor(logManagerDisruptors.computeIfAbsent(listenAddr, endpoint -> new StripedDisruptor<>(
                "JRaft-LogManager-Disruptor_TestCluster",
                nodeOptions.getRaftOptions().getDisruptorBufferSize(),
                () -> new LogManagerImpl.StableClosureEvent(),
                nodeOptions.getStripes())));

            final MockStateMachine fsm = new MockStateMachine(listenAddr);
            nodeOptions.setFsm(fsm);

            if (!emptyPeers)
                nodeOptions.setInitialConf(new Configuration(this.peers, this.learners));

            NodeFinder nodeFinder = (emptyPeers ? Stream.<PeerId>empty() : peers.stream())
                    .map(PeerId::getEndpoint)
                    .map(JRaftUtils::addressFromEndpoint)
                    .collect(collectingAndThen(toList(), StaticNodeFinder::new));

            NodeManager nodeManager = new NodeManager();

            ClusterService clusterService = ClusterServiceTestUtils.clusterService(
                listenAddr.toString(),
                listenAddr.getPort(),
                nodeFinder,
                new TestMessageSerializationRegistryImpl(),
                new TestScaleCubeClusterServiceFactory()
            );

            var rpcClient = new IgniteRpcClient(clusterService);

            nodeOptions.setRpcClient(rpcClient);

            ExecutorService requestExecutor = JRaftUtils.createRequestExecutor(nodeOptions);

            executors.add(requestExecutor);

            var rpcServer = new TestIgniteRpcServer(clusterService, nodeManager, nodeOptions, requestExecutor);

            clusterService.start();

            if (optsClo != null)
                optsClo.accept(nodeOptions);

            final RaftGroupService server = new RaftGroupService(this.name, new PeerId(listenAddr, 0, priority),
                nodeOptions, rpcServer, nodeManager) {
                @Override public synchronized void shutdown() {
                    super.shutdown();

                    rpcServer.shutdown();

                    clusterService.stop();
                }
            };

            this.serverMap.put(listenAddr, server);

            final Node node = server.start();

            this.fsms.put(new PeerId(listenAddr, 0), fsm);
            this.nodes.add((NodeImpl) node);
            return true;
        }
        finally {
            this.lock.unlock();
        }
    }

    public Node getNode(Endpoint endpoint) {
        this.lock.lock();
        try {
            for (NodeImpl node : nodes) {
                if (node.getServerId().getEndpoint().equals(endpoint))
                    return node;
            }
        }
        finally {
            this.lock.unlock();
        }

        return null;
    }

    public RaftGroupService getServer(Endpoint endpoint) {
        return serverMap.get(endpoint);
    }

    public MockStateMachine getFsmByPeer(final PeerId peer) {
        this.lock.lock();
        try {
            return this.fsms.get(peer);
        }
        finally {
            this.lock.unlock();
        }
    }

    public List<MockStateMachine> getFsms() {
        this.lock.lock();
        try {
            return new ArrayList<>(this.fsms.values());
        }
        finally {
            this.lock.unlock();
        }
    }

    public boolean stop(final Endpoint listenAddr) throws InterruptedException {
        removeNode(listenAddr);
        final RaftGroupService raftGroupService = this.serverMap.remove(listenAddr);
        raftGroupService.shutdown();
        return true;
    }

    public void stopAll() throws InterruptedException {
        final List<Endpoint> addrs = getAllNodes();
        for (final Endpoint addr : addrs)
            stop(addr);

        fsmCallerDusruptors.values().forEach(StripedDisruptor::shutdown);
        nodeDisruptors.values().forEach(StripedDisruptor::shutdown);
        readOnlyServiceDisruptors.values().forEach(StripedDisruptor::shutdown);
        logManagerDisruptors.values().forEach(StripedDisruptor::shutdown);
        executors.forEach(ExecutorServiceHelper::shutdownAndAwaitTermination);
        fixedThreadsExecutorGroups.forEach(FixedThreadsExecutorGroup::shutdownGracefully);
        schedulers.forEach(Scheduler::shutdown);
    }

    public void clean(final Endpoint listenAddr) {
        final Path path = Paths.get(this.dataPath, listenAddr.toString().replace(':', '_'));
        LOG.info("Clean dir: {}", path);
        IgniteUtils.deleteIfExists(path);
    }

    public Node getLeader() {
        this.lock.lock();
        try {
            for (int i = 0; i < this.nodes.size(); i++) {
                final NodeImpl node = this.nodes.get(i);
                if (node.isLeader() && this.fsms.get(node.getServerId()).getLeaderTerm() == node.getCurrentTerm()) {
                    return node;
                }
            }
            return null;
        }
        finally {
            this.lock.unlock();
        }
    }

    public MockStateMachine getLeaderFsm() {
        final Node leader = getLeader();
        if (leader != null) {
            return (MockStateMachine) leader.getOptions().getFsm();
        }
        return null;
    }

    /**
     * Wait until a leader is elected.
     * @throws InterruptedException
     */
    public void waitLeader() throws InterruptedException {
        Node node;

        while (true) {
            node = getLeader();

            if (node != null) {
                break;
            }
            else {
                Thread.sleep(10);
            }
        }
    }

    public List<Node> getFollowers() {
        final List<Node> ret = new ArrayList<>();
        this.lock.lock();
        try {
            for (final NodeImpl node : this.nodes) {
                if (!node.isLeader() && !this.learners.contains(node.getServerId())) {
                    ret.add(node);
                }
            }
        }
        finally {
            this.lock.unlock();
        }
        return ret;
    }

    /**
     * Ensure all peers follow the leader
     *
     * @param node The leader.
     * @throws InterruptedException if interrupted
     */
    public void ensureLeader(final Node node) throws InterruptedException {
        while (true) {
            this.lock.lock();
            try {
                boolean wait = false;

                for (final Node node0 : this.nodes) {
                    final PeerId leaderId = node0.getLeaderId();

                    if (leaderId == null || !leaderId.equals(node.getNodeId().getPeerId())) {
                        wait = true;

                        break;
                    }
                }

                if (wait) {
                    Thread.sleep(10);

                    continue;
                }
                else
                    return;
            }
            finally {
                this.lock.unlock();
            }
        }
    }

    public List<NodeImpl> getNodes() {
        this.lock.lock();
        try {
            return new ArrayList<>(this.nodes);
        }
        finally {
            this.lock.unlock();
        }
    }

    public List<Endpoint> getAllNodes() {
        this.lock.lock();
        try {
            return this.nodes.stream().map(node -> node.getNodeId().getPeerId().getEndpoint())
                .collect(toList());
        }
        finally {
            this.lock.unlock();
        }
    }

    public Node removeNode(final Endpoint addr) {
        Node ret = null;
        this.lock.lock();
        try {
            for (int i = 0; i < this.nodes.size(); i++) {
                if (this.nodes.get(i).getNodeId().getPeerId().getEndpoint().equals(addr)) {
                    ret = this.nodes.remove(i);
                    this.fsms.remove(ret.getNodeId().getPeerId());
                    break;
                }
            }
        }
        finally {
            this.lock.unlock();
        }
        return ret;
    }

    public void ensureSame() throws InterruptedException {
        ensureSame(addr -> false);
    }

    /**
     * @param filter The node to exclude filter.
     * @return {@code True} if all FSM state are the same.
     * @throws InterruptedException
     */
    public void ensureSame(Predicate<Endpoint> filter) throws InterruptedException {
        this.lock.lock();

        List<MockStateMachine> fsmList = new ArrayList<>(this.fsms.values());

        if (fsmList.size() <= 1) {
            LOG.warn("ensureSame is skipped because only one node in the group");
            this.lock.unlock();
            return;
        }

        Node leader = getLeader();

        assertNotNull(leader);

        MockStateMachine first = fsms.get(leader.getNodeId().getPeerId());

        LOG.info("Start ensureSame, leader={}", leader);

        try {
            assertTrue(TestUtils.waitForCondition(() -> {
                first.lock();

                try {
                    for (int i = 0; i < fsmList.size(); i++) {
                        MockStateMachine fsm = fsmList.get(i);

                        if (fsm == first || filter.test(fsm.getAddress()))
                            continue;

                        fsm.lock();

                        try {
                            int size0 = first.getLogs().size();
                            int size1 = fsm.getLogs().size();

                            if (size0 == 0 || size0 != size1)
                                return false;

                            for (int j = 0; j < size0; j++) {
                                ByteBuffer data0 = first.getLogs().get(j);
                                ByteBuffer data1 = fsm.getLogs().get(j);

                                if (!data0.equals(data1))
                                    return false;
                            }
                        }
                        finally {
                            fsm.unlock();
                        }
                    }
                }
                finally {
                    first.unlock();
                }

                return true;
            }, 20_000));
        }
        finally {
            this.lock.unlock();

            Node leader1 = getLeader();

            LOG.info("End ensureSame, leader={}", leader1);

            assertSame(leader, leader1, "Leader shouldn't change while comparing fsms");
        }
    }
}
