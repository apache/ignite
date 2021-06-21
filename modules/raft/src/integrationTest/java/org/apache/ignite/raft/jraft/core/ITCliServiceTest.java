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
import java.util.Collections;
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
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Jraft cli tests.
 */
public class ITCliServiceTest {
    /**
     * The logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ITCliServiceTest.class);

    private static final int LEARNER_PORT_STEP = 100;

    private String dataPath;

    private TestCluster cluster;

    private final String groupId = "CliServiceTest";

    private CliService cliService;

    private Configuration conf;

    /** */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Start test method: " + testInfo.getDisplayName());
        dataPath = TestUtils.mkTempDir();
        new File(dataPath).mkdirs();
        List<PeerId> peers = TestUtils.generatePeers(3);

        LinkedHashSet<PeerId> learners = new LinkedHashSet<>();

        // 2 learners
        for (int i = 0; i < 2; i++)
            learners.add(new PeerId(TestUtils.getLocalAddress(), TestUtils.INIT_PORT + LEARNER_PORT_STEP + i));

        cluster = new TestCluster(groupId, dataPath, peers, learners, 300);
        for (PeerId peer : peers)
            cluster.start(peer.getEndpoint());

        for (PeerId peer : learners)
            cluster.startLearner(peer);

        cluster.waitLeader();
        cluster.ensureLeader(cluster.getLeader());

        cliService = new CliServiceImpl();
        conf = new Configuration(peers, learners);

        CliOptions opts = new CliOptions();
        opts.setClientExecutor(JRaftUtils.createClientExecutor(opts, "client"));

        List<String> memberAddresses = peers.stream().map(p -> p.getEndpoint().toString()).collect(Collectors.toList());

        var registry = new MessageSerializationRegistryImpl();

        var serviceConfig = new ClusterLocalConfiguration("client", TestUtils.INIT_PORT - 1, memberAddresses, registry);

        var factory = new TestScaleCubeClusterServiceFactory();

        ClusterService clientSvc = factory.createClusterService(serviceConfig);

        clientSvc.start();

        IgniteRpcClient rpcClient = new IgniteRpcClient(clientSvc) {
            @Override public void shutdown() {
                super.shutdown();

                clientSvc.shutdown();
            }
        };

        opts.setRpcClient(rpcClient);
        assertTrue(cliService.init(opts));
    }

    @AfterEach
    public void teardown(TestInfo testInfo) throws Exception {
        cliService.shutdown();
        cluster.stopAll();
        Utils.delete(new File(dataPath));
        LOG.info(">>>>>>>>>>>>>>> End test method: " + testInfo.getDisplayName());
    }

    @Test
    public void testTransferLeader() throws Exception {
        PeerId leader = cluster.getLeader().getNodeId().getPeerId().copy();
        assertNotNull(leader);

        Set<PeerId> peers = conf.getPeerSet();
        PeerId targetPeer = null;
        for (PeerId peer : peers) {
            if (!peer.equals(leader)) {
                targetPeer = peer;
                break;
            }
        }
        assertNotNull(targetPeer);
        assertTrue(cliService.transferLeader(groupId, conf, targetPeer).isOk());
        cluster.waitLeader();
        assertEquals(targetPeer, cluster.getLeader().getNodeId().getPeerId());
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTestTaskAndWait(Node node, int code) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            Task task = new Task(data, new ExpectClosure(code, null, latch));
            node.apply(task);
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLearnerServices() throws Exception {
        PeerId learner3 = new PeerId(TestUtils.getLocalAddress(), TestUtils.INIT_PORT + LEARNER_PORT_STEP + 3);
        assertTrue(cluster.startLearner(learner3));
        sendTestTaskAndWait(cluster.getLeader(), 0);
        cluster.ensureSame(addr -> addr.equals(learner3.getEndpoint()));

        for (MockStateMachine fsm : cluster.getFsms()) {
            if (!fsm.getAddress().equals(learner3.getEndpoint()))
                assertEquals(10, fsm.getLogs().size());
        }

        assertEquals(0, cluster.getFsmByPeer(learner3).getLogs().size());
        List<PeerId> oldLearners = new ArrayList<PeerId>(conf.getLearners());
        assertEquals(oldLearners, cliService.getLearners(groupId, conf));
        assertEquals(oldLearners, cliService.getAliveLearners(groupId, conf));

        // Add learner3
        cliService.addLearners(groupId, conf, Collections.singletonList(learner3));

        assertTrue(waitForCondition(() -> cluster.getFsmByPeer(learner3).getLogs().size() == 10, 5_000));

        sendTestTaskAndWait(cluster.getLeader(), 0);

        cluster.ensureSame();

        for (MockStateMachine fsm : cluster.getFsms())
            assertEquals(20, fsm.getLogs().size());

        List<PeerId> newLearners = new ArrayList<>(oldLearners);
        newLearners.add(learner3);
        assertEquals(newLearners, cliService.getLearners(groupId, conf));
        assertEquals(newLearners, cliService.getAliveLearners(groupId, conf));

        // Remove  3
        cliService.removeLearners(groupId, conf, Collections.singletonList(learner3));
        sendTestTaskAndWait(cluster.getLeader(), 0);

        cluster.ensureSame(addr -> addr.equals(learner3.getEndpoint()));

        for (MockStateMachine fsm : cluster.getFsms()) {
            if (!fsm.getAddress().equals(learner3.getEndpoint()))
                assertEquals(30, fsm.getLogs().size());
        }

        // Latest 10 logs are not replicated to learner3, because it's removed.
        assertEquals(20, cluster.getFsmByPeer(learner3).getLogs().size());
        assertEquals(oldLearners, cliService.getLearners(groupId, conf));
        assertEquals(oldLearners, cliService.getAliveLearners(groupId, conf));

        // Set learners into [learner3]
        cliService.resetLearners(groupId, conf, Collections.singletonList(learner3));

        assertTrue(waitForCondition(() -> cluster.getFsmByPeer(learner3).getLogs().size() == 30, 5_000));

        sendTestTaskAndWait(cluster.getLeader(), 0);

        cluster.ensureSame(addr -> oldLearners.contains(new PeerId(addr, 0)));

        // Latest 10 logs are not replicated to learner1 and learner2, because they were removed by resetting learners set.
        for (MockStateMachine fsm : cluster.getFsms()) {
            if (!oldLearners.contains(new PeerId(fsm.getAddress(), 0)))
                assertEquals(40, fsm.getLogs().size());
            else
                assertEquals(30, fsm.getLogs().size());
        }

        assertEquals(Collections.singletonList(learner3), cliService.getLearners(groupId, conf));
        assertEquals(Collections.singletonList(learner3), cliService.getAliveLearners(groupId, conf));

        // Stop learner3
        cluster.stop(learner3.getEndpoint());
        sleep(1000);
        assertEquals(Collections.singletonList(learner3), cliService.getLearners(groupId, conf));
        assertTrue(cliService.getAliveLearners(groupId, conf).isEmpty());
    }

    @Test
    public void testAddPeerRemovePeer() throws Exception {
        PeerId peer3 = new PeerId(TestUtils.getLocalAddress(), TestUtils.INIT_PORT + 3);
        assertTrue(cluster.start(peer3.getEndpoint()));
        sendTestTaskAndWait(cluster.getLeader(), 0);
        cluster.ensureSame(addr -> addr.equals(peer3.getEndpoint()));
        assertEquals(0, cluster.getFsmByPeer(peer3).getLogs().size());

        assertTrue(cliService.addPeer(groupId, conf, peer3).isOk());

        assertTrue(waitForCondition(() -> cluster.getFsmByPeer(peer3).getLogs().size() == 10, 5_000));
        sendTestTaskAndWait(cluster.getLeader(), 0);

        assertEquals(6, cluster.getFsms().size());

        cluster.ensureSame();

        for (MockStateMachine fsm : cluster.getFsms())
            assertEquals(20, fsm.getLogs().size());

        //remove peer3
        assertTrue(cliService.removePeer(groupId, conf, peer3).isOk());
        sleep(200);
        sendTestTaskAndWait(cluster.getLeader(), 0);

        assertEquals(6, cluster.getFsms().size());

        cluster.ensureSame(addr -> addr.equals(peer3.getEndpoint()));

        for (MockStateMachine fsm : cluster.getFsms()) {
            if (fsm.getAddress().equals(peer3.getEndpoint()))
                assertEquals(20, fsm.getLogs().size());
            else
                assertEquals(30, fsm.getLogs().size());
        }
    }

    @Test
    public void testChangePeers() throws Exception {
        List<PeerId> newPeers = TestUtils.generatePeers(10);
        newPeers.removeAll(conf.getPeerSet());
        for (PeerId peer : newPeers)
            assertTrue(cluster.start(peer.getEndpoint()));
        cluster.waitLeader();
        Node oldLeaderNode = cluster.getLeader();
        assertNotNull(oldLeaderNode);
        PeerId oldLeader = oldLeaderNode.getNodeId().getPeerId();
        assertNotNull(oldLeader);
        assertTrue(cliService.changePeers(groupId, conf, new Configuration(newPeers)).isOk());
        cluster.waitLeader();
        PeerId newLeader = cluster.getLeader().getNodeId().getPeerId();
        assertNotEquals(oldLeader, newLeader);
        assertTrue(newPeers.contains(newLeader));
    }

    @Test
    public void testSnapshot() throws Exception {
        sendTestTaskAndWait(cluster.getLeader(), 0);
        assertEquals(5, cluster.getFsms().size());
        for (MockStateMachine fsm : cluster.getFsms())
            assertEquals(0, fsm.getSaveSnapshotTimes());

        for (PeerId peer : conf)
            assertTrue(cliService.snapshot(groupId, peer).isOk());

        for (PeerId peer : conf.getLearners())
            assertTrue(cliService.snapshot(groupId, peer).isOk());

        for (MockStateMachine fsm : cluster.getFsms())
            assertEquals(1, fsm.getSaveSnapshotTimes());
    }

    @Test
    public void testGetPeers() throws Exception {
        PeerId leader = cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(conf.getPeerSet().toArray(),
            new HashSet<>(cliService.getPeers(groupId, conf)).toArray());

        // stop one peer
        List<PeerId> peers = conf.getPeers();
        cluster.stop(peers.get(0).getEndpoint());

        cluster.waitLeader();

        leader = cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(conf.getPeerSet().toArray(),
            new HashSet<>(cliService.getPeers(groupId, conf)).toArray());

        cluster.stopAll();

        try {
            cliService.getPeers(groupId, conf);
            fail();
        }
        catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Fail to get leader of group " + groupId), e.getMessage());
        }
    }

    @Test
    public void testGetAlivePeers() throws Exception {
        PeerId leader = cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(conf.getPeerSet().toArray(),
            new HashSet<>(cliService.getAlivePeers(groupId, conf)).toArray());

        // stop one peer
        List<PeerId> peers = conf.getPeers();
        cluster.stop(peers.get(0).getEndpoint());
        peers.remove(0);

        cluster.waitLeader();

        sleep(1000);

        leader = cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(new HashSet<>(peers).toArray(),
            new HashSet<>(cliService.getAlivePeers(groupId, conf)).toArray());

        cluster.stopAll();

        try {
            cliService.getAlivePeers(groupId, conf);
            fail();
        }
        catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Fail to get leader of group " + groupId), e.getMessage());
        }
    }

    @Test
    public void testRebalance() {
        Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        groupIds.add("group_5");
        groupIds.add("group_6");
        groupIds.add("group_7");
        groupIds.add("group_8");
        Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        CliService cliService = new MockCliService(rebalancedLeaderIds, new PeerId("host_1", 8080));

        assertTrue(cliService.rebalance(groupIds, conf, rebalancedLeaderIds).isOk());
        assertEquals(groupIds.size(), rebalancedLeaderIds.size());

        Map<PeerId, Integer> ret = new HashMap<>();
        for (Map.Entry<String, PeerId> entry : rebalancedLeaderIds.entrySet())
            ret.compute(entry.getValue(), (ignored, num) -> num == null ? 1 : num + 1);
        int expectedAvgLeaderNum = (int) Math.ceil((double) groupIds.size() / conf.size());
        for (Map.Entry<PeerId, Integer> entry : ret.entrySet()) {
            System.out.println(entry);
            assertTrue(entry.getValue() <= expectedAvgLeaderNum);
        }
    }

    @Test
    public void testRebalanceOnLeaderFail() {
        Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        CliService cliService = new MockLeaderFailCliService();

        assertEquals("Fail to get leader", cliService.rebalance(groupIds, conf, rebalancedLeaderIds).getErrorMsg());
    }

    @Test
    public void testRelalanceOnTransferLeaderFail() {
        Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        groupIds.add("group_5");
        groupIds.add("group_6");
        groupIds.add("group_7");
        Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        CliService cliService = new MockTransferLeaderFailCliService(rebalancedLeaderIds,
            new PeerId("host_1", 8080));

        assertEquals("Fail to transfer leader",
            cliService.rebalance(groupIds, conf, rebalancedLeaderIds).getErrorMsg());
        assertTrue(groupIds.size() >= rebalancedLeaderIds.size());

        Map<PeerId, Integer> ret = new HashMap<>();
        for (Map.Entry<String, PeerId> entry : rebalancedLeaderIds.entrySet())
            ret.compute(entry.getValue(), (ignored, num) -> num == null ? 1 : num + 1);
        for (Map.Entry<PeerId, Integer> entry : ret.entrySet()) {
            System.out.println(entry);
            assertEquals(new PeerId("host_1", 8080), entry.getKey());
        }
    }

    static class MockCliService extends CliServiceImpl {
        private final Map<String, PeerId> rebalancedLeaderIds;
        private final PeerId initialLeaderId;

        MockCliService(Map<String, PeerId> rebalancedLeaderIds, PeerId initialLeaderId) {
            this.rebalancedLeaderIds = rebalancedLeaderIds;
            this.initialLeaderId = initialLeaderId;
        }

        /** {@inheritDoc} */
        @Override
        public Status getLeader(String groupId, Configuration conf, PeerId leaderId) {
            PeerId ret = rebalancedLeaderIds.get(groupId);
            if (ret != null)
                leaderId.parse(ret.toString());
            else
                leaderId.parse(initialLeaderId.toString());
            return Status.OK();
        }

        /** {@inheritDoc} */
        @Override
        public List<PeerId> getAlivePeers(String groupId, Configuration conf) {
            return conf.getPeers();
        }

        /** {@inheritDoc} */
        @Override
        public Status transferLeader(String groupId, Configuration conf, PeerId peer) {
            return Status.OK();
        }
    }

    static class MockLeaderFailCliService extends MockCliService {
        MockLeaderFailCliService() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override
        public Status getLeader(String groupId, Configuration conf, PeerId leaderId) {
            return new Status(-1, "Fail to get leader");
        }
    }

    static class MockTransferLeaderFailCliService extends MockCliService {
        MockTransferLeaderFailCliService(Map<String, PeerId> rebalancedLeaderIds, PeerId initialLeaderId) {
            super(rebalancedLeaderIds, initialLeaderId);
        }

        /** {@inheritDoc} */
        @Override
        public Status transferLeader(String groupId, Configuration conf, PeerId peer) {
            return new Status(-1, "Fail to transfer leader");
        }
    }

    /**
     * @param cond The condition.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    @SuppressWarnings("BusyWait") protected static boolean waitForCondition(BooleanSupplier cond, long timeout) {
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
