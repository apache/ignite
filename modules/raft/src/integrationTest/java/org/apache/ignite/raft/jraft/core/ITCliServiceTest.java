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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.jraft.CliService;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Jraft cli tests.
 */
public class ITCliServiceTest {
    /**
     * The logger.
     */
    static final Logger LOG = LoggerFactory.getLogger(ITCliServiceTest.class);

    /**
     * The registry.
     */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /**
     * The message factory.
     */
    private final static ScaleCubeClusterServiceFactory factory = new TestScaleCubeClusterServiceFactory();

    private String dataPath;

    private TestCluster cluster;
    private final String groupId = "CliServiceTest";

    private CliService cliService;

    private Configuration conf;

    @Rule
    public TestName testName = new TestName();

    private static final int LEARNER_PORT_STEP = 100;

    @Before
    public void setup() throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        this.dataPath = TestUtils.mkTempDir();
        new File(this.dataPath).mkdirs();
        final List<PeerId> peers = TestUtils.generatePeers(3);

        final LinkedHashSet<PeerId> learners = new LinkedHashSet<>();

        // 2 learners
        for (int i = 0; i < 2; i++) {
            learners.add(new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + LEARNER_PORT_STEP + i));
        }

        this.cluster = new TestCluster(this.groupId, this.dataPath, peers, learners, 300);
        for (final PeerId peer : peers) {
            this.cluster.start(peer.getEndpoint());
        }

        for (final PeerId peer : learners) {
            this.cluster.startLearner(peer);
        }

        this.cluster.waitLeader();

        for (Node follower : cluster.getFollowers()) {
            assertTrue(waitForCondition(() -> follower.getLeaderId() != null, 3_000));
        }

        for (PeerId learner : cluster.getLearners()) {
            Node node = cluster.getNode(learner.getEndpoint());

            assertTrue(waitForCondition(() -> node.getLeaderId() != null, 3_000));
        }

        this.cliService = new CliServiceImpl();
        this.conf = new Configuration(peers, learners);

        CliOptions opts = new CliOptions();
        opts.setClientExecutor(JRaftUtils.createClientExecutor(opts, "client"));

        ClusterService clientSvc = factory.createClusterService(new ClusterLocalConfiguration("client",
            TestUtils.INIT_PORT - 1, peers.stream().map(p -> p.getEndpoint().toString()).collect(Collectors.toList()),
            SERIALIZATION_REGISTRY));

        IgniteRpcClient rpcClient = new IgniteRpcClient(clientSvc, false);
        opts.setRpcClient(rpcClient);
        assertTrue(this.cliService.init(opts));
    }

    @After
    public void teardown() throws Exception {
        this.cliService.shutdown();
        this.cluster.stopAll();
        Utils.delete(new File(this.dataPath));
        LOG.info(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName());
    }

    @Test
    public void testTransferLeader() throws Exception {
        final PeerId leader = this.cluster.getLeader().getNodeId().getPeerId().copy();
        assertNotNull(leader);

        final Set<PeerId> peers = this.conf.getPeerSet();
        PeerId targetPeer = null;
        for (final PeerId peer : peers) {
            if (!peer.equals(leader)) {
                targetPeer = peer;
                break;
            }
        }
        assertNotNull(targetPeer);
        assertTrue(this.cliService.transferLeader(this.groupId, this.conf, targetPeer).isOk());
        this.cluster.waitLeader();
        assertEquals(targetPeer, this.cluster.getLeader().getNodeId().getPeerId());
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTestTaskAndWait(final Node node, final int code) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            final Task task = new Task(data, new ExpectClosure(code, null, latch));
            node.apply(task);
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLearnerServices() throws Exception {
        final PeerId learner3 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + LEARNER_PORT_STEP + 3);
        assertTrue(this.cluster.startLearner(learner3));
        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(500);
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            if (!fsm.getAddress().equals(learner3.getEndpoint())) {
                assertEquals(10, fsm.getLogs().size());
            }
        }
        assertEquals(0, this.cluster.getFsmByPeer(learner3).getLogs().size());
        List<PeerId> oldLearners = new ArrayList<PeerId>(this.conf.getLearners());
        assertEquals(oldLearners, this.cliService.getLearners(this.groupId, this.conf));
        assertEquals(oldLearners, this.cliService.getAliveLearners(this.groupId, this.conf));

        // Add learner3
        this.cliService.addLearners(this.groupId, this.conf, Arrays.asList(learner3));
        Thread.sleep(1000);
        assertEquals(10, this.cluster.getFsmByPeer(learner3).getLogs().size());

        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(1000);
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());

        }
        List<PeerId> newLearners = new ArrayList<>(oldLearners);
        newLearners.add(learner3);
        assertEquals(newLearners, this.cliService.getLearners(this.groupId, this.conf));
        assertEquals(newLearners, this.cliService.getAliveLearners(this.groupId, this.conf));

        // Remove  3
        this.cliService.removeLearners(this.groupId, this.conf, Arrays.asList(learner3));
        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(1000);
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            if (!fsm.getAddress().equals(learner3.getEndpoint())) {
                assertEquals(30, fsm.getLogs().size());
            }
        }
        // Latest 10 logs are not replicated to learner3, because it's removed.
        assertEquals(20, this.cluster.getFsmByPeer(learner3).getLogs().size());
        assertEquals(oldLearners, this.cliService.getLearners(this.groupId, this.conf));
        assertEquals(oldLearners, this.cliService.getAliveLearners(this.groupId, this.conf));

        // Set learners into [learner3]
        this.cliService.resetLearners(this.groupId, this.conf, Arrays.asList(learner3));
        Thread.sleep(100);
        assertEquals(30, this.cluster.getFsmByPeer(learner3).getLogs().size());

        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(1000);
        // Latest 10 logs are not replicated to learner1 and learner2, because they were removed by resetting learners set.
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            if (!oldLearners.contains(new PeerId(fsm.getAddress(), 0))) {
                assertEquals(40, fsm.getLogs().size());
            }
            else {
                assertEquals(30, fsm.getLogs().size());
            }
        }
        assertEquals(Arrays.asList(learner3), this.cliService.getLearners(this.groupId, this.conf));
        assertEquals(Arrays.asList(learner3), this.cliService.getAliveLearners(this.groupId, this.conf));

        // Stop learner3
        this.cluster.stop(learner3.getEndpoint());
        Thread.sleep(1000);
        assertEquals(Arrays.asList(learner3), this.cliService.getLearners(this.groupId, this.conf));
        assertTrue(this.cliService.getAliveLearners(this.groupId, this.conf).isEmpty());
    }

    @Test
    public void testAddPeerRemovePeer() throws Exception {
        final PeerId peer3 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);
        assertTrue(this.cluster.start(peer3.getEndpoint()));
        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(100);
        assertEquals(0, this.cluster.getFsmByPeer(peer3).getLogs().size());

        assertTrue(this.cliService.addPeer(this.groupId, this.conf, peer3).isOk());
        Thread.sleep(100);
        assertEquals(10, this.cluster.getFsmByPeer(peer3).getLogs().size());
        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(100);
        assertEquals(6, this.cluster.getFsms().size());
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }

        //remove peer3
        assertTrue(this.cliService.removePeer(this.groupId, this.conf, peer3).isOk());
        Thread.sleep(200);
        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        Thread.sleep(1000);
        assertEquals(6, this.cluster.getFsms().size());
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            if (fsm.getAddress().equals(peer3.getEndpoint())) {
                assertEquals(20, fsm.getLogs().size());
            }
            else {
                assertEquals(30, fsm.getLogs().size());
            }
        }
    }

    @Test
    public void testChangePeers() throws Exception {
        final List<PeerId> newPeers = TestUtils.generatePeers(10);
        newPeers.removeAll(this.conf.getPeerSet());
        for (final PeerId peer : newPeers) {
            assertTrue(this.cluster.start(peer.getEndpoint()));
        }
        this.cluster.waitLeader();
        final Node oldLeaderNode = this.cluster.getLeader();
        assertNotNull(oldLeaderNode);
        final PeerId oldLeader = oldLeaderNode.getNodeId().getPeerId();
        assertNotNull(oldLeader);
        assertTrue(this.cliService.changePeers(this.groupId, this.conf, new Configuration(newPeers)).isOk());
        this.cluster.waitLeader();
        final PeerId newLeader = this.cluster.getLeader().getNodeId().getPeerId();
        assertNotEquals(oldLeader, newLeader);
        assertTrue(newPeers.contains(newLeader));
    }

    @Test
    public void testSnapshot() throws Exception {
        sendTestTaskAndWait(this.cluster.getLeader(), 0);
        assertEquals(5, this.cluster.getFsms().size());
        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            assertEquals(0, fsm.getSaveSnapshotTimes());
        }

        for (final PeerId peer : this.conf) {
            assertTrue(this.cliService.snapshot(this.groupId, peer).isOk());
        }

        for (final PeerId peer : this.conf.getLearners()) {
            assertTrue(this.cliService.snapshot(this.groupId, peer).isOk());
        }

        for (final MockStateMachine fsm : this.cluster.getFsms()) {
            assertEquals(1, fsm.getSaveSnapshotTimes());
        }
    }

    @Test
    public void testGetPeers() throws Exception {
        PeerId leader = this.cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(this.conf.getPeerSet().toArray(),
            new HashSet<>(this.cliService.getPeers(this.groupId, this.conf)).toArray());

        // stop one peer
        final List<PeerId> peers = this.conf.getPeers();
        this.cluster.stop(peers.get(0).getEndpoint());

        this.cluster.waitLeader();

        leader = this.cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(this.conf.getPeerSet().toArray(),
            new HashSet<>(this.cliService.getPeers(this.groupId, this.conf)).toArray());

        this.cluster.stopAll();

        try {
            this.cliService.getPeers(this.groupId, this.conf);
            fail();
        }
        catch (final IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().startsWith("Fail to get leader of group " + this.groupId));
        }
    }

    @Test
    public void testGetAlivePeers() throws Exception {
        PeerId leader = this.cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(this.conf.getPeerSet().toArray(),
            new HashSet<>(this.cliService.getAlivePeers(this.groupId, this.conf)).toArray());

        // stop one peer
        final List<PeerId> peers = this.conf.getPeers();
        this.cluster.stop(peers.get(0).getEndpoint());
        peers.remove(0);

        this.cluster.waitLeader();

        Thread.sleep(1000);

        leader = this.cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(new HashSet<>(peers).toArray(),
            new HashSet<>(this.cliService.getAlivePeers(this.groupId, this.conf)).toArray());

        this.cluster.stopAll();

        try {
            this.cliService.getAlivePeers(this.groupId, this.conf);
            fail();
        }
        catch (final IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().startsWith("Fail to get leader of group " + this.groupId));
        }
    }

    @Test
    public void testRebalance() {
        final Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        groupIds.add("group_5");
        groupIds.add("group_6");
        groupIds.add("group_7");
        groupIds.add("group_8");
        final Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        final Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        final CliService cliService = new MockCliService(rebalancedLeaderIds, new PeerId("host_1", 8080));

        assertTrue(cliService.rebalance(groupIds, conf, rebalancedLeaderIds).isOk());
        assertEquals(groupIds.size(), rebalancedLeaderIds.size());

        final Map<PeerId, Integer> ret = new HashMap<>();
        for (Map.Entry<String, PeerId> entry : rebalancedLeaderIds.entrySet()) {
            ret.compute(entry.getValue(), (ignored, num) -> num == null ? 1 : num + 1);
        }
        final int expectedAvgLeaderNum = (int) Math.ceil((double) groupIds.size() / conf.size());
        for (Map.Entry<PeerId, Integer> entry : ret.entrySet()) {
            System.out.println(entry);
            assertTrue(entry.getValue() <= expectedAvgLeaderNum);
        }
    }

    @Test
    public void testRebalanceOnLeaderFail() {
        final Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        final Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        final Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        final CliService cliService = new MockLeaderFailCliService();

        assertEquals("Fail to get leader", cliService.rebalance(groupIds, conf, rebalancedLeaderIds).getErrorMsg());
    }

    @Test
    public void testRelalanceOnTransferLeaderFail() {
        final Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        groupIds.add("group_5");
        groupIds.add("group_6");
        groupIds.add("group_7");
        final Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        final Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        final CliService cliService = new MockTransferLeaderFailCliService(rebalancedLeaderIds,
            new PeerId("host_1", 8080));

        assertEquals("Fail to transfer leader",
            cliService.rebalance(groupIds, conf, rebalancedLeaderIds).getErrorMsg());
        assertTrue(groupIds.size() >= rebalancedLeaderIds.size());

        final Map<PeerId, Integer> ret = new HashMap<>();
        for (Map.Entry<String, PeerId> entry : rebalancedLeaderIds.entrySet()) {
            ret.compute(entry.getValue(), (ignored, num) -> num == null ? 1 : num + 1);
        }
        for (Map.Entry<PeerId, Integer> entry : ret.entrySet()) {
            System.out.println(entry);
            assertEquals(new PeerId("host_1", 8080), entry.getKey());
        }
    }

    static class MockCliService extends CliServiceImpl {
        private final Map<String, PeerId> rebalancedLeaderIds;
        private final PeerId initialLeaderId;

        MockCliService(final Map<String, PeerId> rebalancedLeaderIds, final PeerId initialLeaderId) {
            this.rebalancedLeaderIds = rebalancedLeaderIds;
            this.initialLeaderId = initialLeaderId;
        }

        @Override
        public Status getLeader(final String groupId, final Configuration conf, final PeerId leaderId) {
            final PeerId ret = this.rebalancedLeaderIds.get(groupId);
            if (ret != null) {
                leaderId.parse(ret.toString());
            }
            else {
                leaderId.parse(this.initialLeaderId.toString());
            }
            return Status.OK();
        }

        @Override
        public List<PeerId> getAlivePeers(final String groupId, final Configuration conf) {
            return conf.getPeers();
        }

        @Override
        public Status transferLeader(final String groupId, final Configuration conf, final PeerId peer) {
            return Status.OK();
        }
    }

    static class MockLeaderFailCliService extends MockCliService {
        MockLeaderFailCliService() {
            super(null, null);
        }

        @Override
        public Status getLeader(final String groupId, final Configuration conf, final PeerId leaderId) {
            return new Status(-1, "Fail to get leader");
        }
    }

    static class MockTransferLeaderFailCliService extends MockCliService {
        MockTransferLeaderFailCliService(final Map<String, PeerId> rebalancedLeaderIds, final PeerId initialLeaderId) {
            super(rebalancedLeaderIds, initialLeaderId);
        }

        @Override
        public Status transferLeader(final String groupId, final Configuration conf, final PeerId peer) {
            return new Status(-1, "Fail to transfer leader");
        }
    }

    /**
     * @param cond The condition.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    @SuppressWarnings("BusyWait") protected boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            try {
                sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
