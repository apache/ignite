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

import com.codahale.metrics.ConsoleReporter;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.JoinableClosure;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.closure.SynchronizedClosure;
import org.apache.ignite.raft.jraft.closure.TaskClosure;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.entity.UserLog;
import org.apache.ignite.raft.jraft.error.LogIndexOutOfBoundsException;
import org.apache.ignite.raft.jraft.error.LogNotFoundException;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.BootstrapOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.RpcClientEx;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.TestIgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.core.DefaultRaftClientService;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.ThroughputSnapshotThrottle;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Bits;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for raft cluster.
 */
public class ITNodeTest {
    static final Logger LOG = LoggerFactory.getLogger(ITNodeTest.class);

    private String dataPath;

    private final AtomicInteger startedCounter = new AtomicInteger(0);
    private final AtomicInteger stoppedCounter = new AtomicInteger(0);

    @Rule
    public TestName testName = new TestName();

    private long testStartMs;

    private static DumpThread dumpThread;

    private TestCluster cluster;

    static class DumpThread extends Thread {
        private static long DUMP_TIMEOUT_MS = 5 * 60 * 1000;
        private volatile boolean stopped = false;

        @SuppressWarnings("BusyWait") @Override
        public void run() {
            while (!this.stopped) {
                try {
                    Thread.sleep(DUMP_TIMEOUT_MS);
                    LOG.info("Test hang too long, dump threads");
                    TestUtils.dumpThreads();
                }
                catch (InterruptedException e) {
                    // reset request, continue
                    continue;
                }
            }
        }
    }

    @BeforeClass
    public static void setupNodeTest() {
        dumpThread = new DumpThread();
        dumpThread.setName("NodeTest-DumpThread");
        dumpThread.setDaemon(true);
        dumpThread.start();
    }

    @AfterClass
    public static void tearNodeTest() throws Exception {
        dumpThread.stopped = true;
        dumpThread.interrupt();
        dumpThread.join(100);
    }

    @Before
    public void setup() throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        this.dataPath = TestUtils.mkTempDir();

        File dataFile = new File(this.dataPath);

        if (dataFile.exists())
            assertTrue(Utils.delete(dataFile));

        dataFile.mkdirs();
        this.testStartMs = Utils.monotonicMs();
        dumpThread.interrupt(); // reset dump timeout
    }

    @After
    public void teardown() throws Exception {
        if (cluster != null)
            cluster.stopAll();

        assertTrue(Utils.delete(new File(this.dataPath)));
        this.startedCounter.set(0);
        this.stoppedCounter.set(0);
        LOG.info(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName() + ", cost:"
            + (Utils.monotonicMs() - this.testStartMs) + " ms.");
    }

    @Test
    public void testInitShutdown() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final NodeOptions nodeOptions = createNodeOptions();

        nodeOptions.setFsm(new MockStateMachine(addr));
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");

        RaftGroupService service = createService("unittest", new PeerId(addr, 0), nodeOptions);

        service.start(true);

        service.shutdown();
    }

    @Test
    public void testNodeTaskOverload() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final PeerId peer = new PeerId(addr, 0);

        final NodeOptions nodeOptions = createNodeOptions();
        final RaftOptions raftOptions = new RaftOptions();
        raftOptions.setDisruptorBufferSize(2);
        nodeOptions.setRaftOptions(raftOptions);
        final MockStateMachine fsm = new MockStateMachine(addr);
        nodeOptions.setFsm(fsm);
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer)));

        RaftGroupService service = createService("unittest", new PeerId(addr, 0), nodeOptions);

        final Node node = service.start(true);

        assertEquals(1, node.listPeers().size());
        assertTrue(node.listPeers().contains(peer));

        while (!node.isLeader()) {
            ;
        }

        final List<Task> tasks = new ArrayList<>();
        final AtomicInteger c = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            final Task task = new Task(data, new JoinableClosure(status -> {
                System.out.println(status);
                if (!status.isOk()) {
                    assertTrue(
                        status.getRaftError() == RaftError.EBUSY || status.getRaftError() == RaftError.EPERM);
                }
                c.incrementAndGet();
            }));
            node.apply(task);
            tasks.add(task);
        }
        try {
            Task.joinAll(tasks, TimeUnit.SECONDS.toMillis(30));
            assertEquals(10, c.get());
        }
        finally {
            service.shutdown();
        }
    }

    /**
     * Test rollback stateMachine with readIndex for issue 317: https://github.com/sofastack/sofa-jraft/issues/317
     */
    @Test
    public void testRollbackStateMachineWithReadIndex_Issue317() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final PeerId peer = new PeerId(addr, 0);

        final NodeOptions nodeOptions = createNodeOptions();
        final CountDownLatch applyCompleteLatch = new CountDownLatch(1);
        final CountDownLatch applyLatch = new CountDownLatch(1);
        final CountDownLatch readIndexLatch = new CountDownLatch(1);
        final AtomicInteger currentValue = new AtomicInteger(-1);
        final String errorMsg = this.testName.getMethodName();
        final StateMachine fsm = new StateMachineAdapter() {

            @Override
            public void onApply(final Iterator iter) {
                // Notify that the #onApply is preparing to go.
                readIndexLatch.countDown();
                // Wait for submitting a read-index request
                try {
                    applyLatch.await();
                }
                catch (InterruptedException e) {
                    fail();
                }
                int i = 0;
                while (iter.hasNext()) {
                    byte[] data = iter.next().array();
                    int v = Bits.getInt(data, 0);
                    assertEquals(i++, v);
                    currentValue.set(v);
                }
                if (i > 0) {
                    // rollback
                    currentValue.set(i - 1);
                    iter.setErrorAndRollback(1, new Status(-1, errorMsg));
                    applyCompleteLatch.countDown();
                }
            }
        };
        nodeOptions.setFsm(fsm);
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer)));

        RaftGroupService service = createService("unittest", peer, nodeOptions);

        final Node node = service.start(true);

        assertEquals(1, node.listPeers().size());
        assertTrue(node.listPeers().contains(peer));

        while (!node.isLeader()) {
            ;
        }

        int n = 5;
        {
            // apply tasks
            for (int i = 0; i < n; i++) {
                byte[] b = new byte[4];
                Bits.putInt(b, 0, i);
                node.apply(new Task(ByteBuffer.wrap(b), null));
            }
        }

        final AtomicInteger readIndexSuccesses = new AtomicInteger(0);
        {
            // Submit a read-index, wait for #onApply
            readIndexLatch.await();
            final CountDownLatch latch = new CountDownLatch(1);
            node.readIndex(null, new ReadIndexClosure() {

                @Override
                public void run(final Status status, final long index, final byte[] reqCtx) {
                    try {
                        if (status.isOk()) {
                            readIndexSuccesses.incrementAndGet();
                        }
                        else {
                            assertTrue("Unexpected status: " + status,
                                status.getErrorMsg().contains(errorMsg) || status.getRaftError() == RaftError.ETIMEDOUT
                                    || status.getErrorMsg().contains("Invalid state for readIndex: STATE_ERROR"));
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                }
            });
            // We have already submit a read-index request,
            // notify #onApply can go right now
            applyLatch.countDown();

            // The state machine is in error state, the node should step down.
            while (node.isLeader()) {
                Thread.sleep(10);
            }
            latch.await();
            applyCompleteLatch.await();
        }
        // No read-index request succeed.
        assertEquals(0, readIndexSuccesses.get());
        assertTrue(n - 1 >= currentValue.get());

        service.shutdown();
    }

    @Test
    public void testSingleNode() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final PeerId peer = new PeerId(addr, 0);

        final NodeOptions nodeOptions = createNodeOptions();
        final MockStateMachine fsm = new MockStateMachine(addr);
        nodeOptions.setFsm(fsm);
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer)));
        RaftGroupService service = createService("unittest", peer, nodeOptions);

        Node node = service.start();

        assertEquals(1, node.listPeers().size());
        assertTrue(node.listPeers().contains(peer));

        while (!node.isLeader()) {
            ;
        }

        sendTestTaskAndWait(node);
        assertEquals(10, fsm.getLogs().size());
        int i = 0;
        for (final ByteBuffer data : fsm.getLogs()) {
            assertEquals("hello" + i++, new String(data.array()));
        }
        service.shutdown();
    }

    @Test
    public void testNoLeader() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);

        assertTrue(cluster.start(peers.get(0).getEndpoint()));

        final List<Node> followers = cluster.getFollowers();
        assertEquals(1, followers.size());

        final Node follower = followers.get(0);
        sendTestTaskAndWait(follower, 0, RaftError.EPERM);

        // adds a peer3
        final PeerId peer3 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);
        CountDownLatch latch = new CountDownLatch(1);
        follower.addPeer(peer3, new ExpectClosure(RaftError.EPERM, latch));
        waitLatch(latch);

        // remove the peer0
        final PeerId peer0 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        latch = new CountDownLatch(1);
        follower.removePeer(peer0, new ExpectClosure(RaftError.EPERM, latch));
        waitLatch(latch);
    }

    private void sendTestTaskAndWait(final Node node) throws InterruptedException {
        this.sendTestTaskAndWait(node, 0, 10, RaftError.SUCCESS);
    }

    private void sendTestTaskAndWait(final Node node, int amount) throws InterruptedException {
        this.sendTestTaskAndWait(node, 0, amount, RaftError.SUCCESS);
    }

    private void sendTestTaskAndWait(final Node node, final RaftError err) throws InterruptedException {
        this.sendTestTaskAndWait(node, 0, 10, err);
    }

    private void sendTestTaskAndWait(final Node node, final int start, int amount,
        final RaftError err) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(amount);
        for (int i = start; i < start + amount; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            final Task task = new Task(data, new ExpectClosure(err, latch));
            node.apply(task);
        }
        waitLatch(latch);
    }

    private void sendTestTaskAndWait(final Node node, final int start,
        final RaftError err) throws InterruptedException {
        sendTestTaskAndWait(node, start, 10, err);
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTestTaskAndWait(final String prefix, final Node node, final int code) throws InterruptedException {
        sendTestTaskAndWait(prefix, node, 10, code);
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTestTaskAndWait(final String prefix, final Node node, int amount,
        final int code) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < amount; i++) {
            final ByteBuffer data = ByteBuffer.wrap((prefix + i).getBytes());
            final Task task = new Task(data, new ExpectClosure(code, null, latch));
            node.apply(task);
        }
        waitLatch(latch);
    }

    @Test
    public void testTripleNodesWithReplicatorStateListener() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        //final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);

        final UserReplicatorStateListener listener1 = new UserReplicatorStateListener();
        final UserReplicatorStateListener listener2 = new UserReplicatorStateListener();

        cluster = new TestCluster("unitest", this.dataPath, peers, new LinkedHashSet<>(), 300,
            opts -> opts.setReplicationStateListeners(List.of(listener1, listener2)));

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        for (Node follower : cluster.getFollowers())
            waitForCondition(() -> follower.getLeaderId() != null, 5_000);

        assertEquals(4, this.startedCounter.get());
        assertEquals(2, cluster.getLeader().getReplicatorStateListeners().size());
        assertEquals(2, cluster.getFollowers().get(0).getReplicatorStateListeners().size());
        assertEquals(2, cluster.getFollowers().get(1).getReplicatorStateListeners().size());

        for (Node node : cluster.getNodes()) {
            node.removeReplicatorStateListener(listener1);
        }
        assertEquals(1, cluster.getLeader().getReplicatorStateListeners().size());
        assertEquals(1, cluster.getFollowers().get(0).getReplicatorStateListeners().size());
        assertEquals(1, cluster.getFollowers().get(1).getReplicatorStateListeners().size());
    }

    // TODO asch Broken then using volatile log. A follower with empty log can become a leader IGNITE-14832.
    @Test
    @Ignore
    public void testVoteTimedoutStepDown() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        // Stop all followers
        List<Node> followers = cluster.getFollowers();
        assertFalse(followers.isEmpty());
        for (Node node : followers) {
            assertTrue(cluster.stop(node.getNodeId().getPeerId().getEndpoint()));
        }

        // Wait leader to step down.
        while (leader.isLeader()) {
            Thread.sleep(10);
        }

        // old leader try to elect self, it should fail.
        ((NodeImpl) leader).tryElectSelf();
        Thread.sleep(1500);

        assertNull(cluster.getLeader());

        // Start followers
        for (Node node : followers) {
            assertTrue(cluster.start(node.getNodeId().getPeerId().getEndpoint()));
        }

        cluster.ensureSame();
    }

    class UserReplicatorStateListener implements Replicator.ReplicatorStateListener {
        @Override
        public void onCreated(final PeerId peer) {
            int val = ITNodeTest.this.startedCounter.incrementAndGet();

            LOG.info("Replicator has been created {} {}", peer, val);
        }

        @Override
        public void onError(final PeerId peer, final Status status) {
            LOG.info("Replicator has errors {} {}", peer, status);
        }

        @Override
        public void onDestroyed(final PeerId peer) {
            int val = ITNodeTest.this.stoppedCounter.incrementAndGet();

            LOG.info("Replicator has been destroyed {} {}", peer, val);
        }
    }

    @Test
    public void testLeaderTransferWithReplicatorStateListener() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, new LinkedHashSet<>(), 300,
            opts -> opts.setReplicationStateListeners(List.of(new UserReplicatorStateListener())));

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();
        this.sendTestTaskAndWait(leader);
        Thread.sleep(100);
        final List<Node> followers = cluster.getFollowers();

        assertTrue(this.startedCounter.get() + "", waitForCondition(() -> this.startedCounter.get() == 2, 5_000));

        final PeerId targetPeer = followers.get(0).getNodeId().getPeerId().copy();
        LOG.info("Transfer leadership from {} to {}", leader, targetPeer);
        assertTrue(leader.transferLeadershipTo(targetPeer).isOk());
        Thread.sleep(1000);
        cluster.waitLeader();

        assertTrue(this.startedCounter.get() + "", waitForCondition(() -> this.startedCounter.get() == 4, 5_000));

        for (Node node : cluster.getNodes()) {
            node.clearReplicatorStateListeners();
        }
        assertEquals(0, cluster.getLeader().getReplicatorStateListeners().size());
        assertEquals(0, cluster.getFollowers().get(0).getReplicatorStateListeners().size());
        assertEquals(0, cluster.getFollowers().get(1).getReplicatorStateListeners().size());
    }

    @Test
    public void testTripleNodes() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        {
            final ByteBuffer data = ByteBuffer.wrap("no closure".getBytes());
            final Task task = new Task(data, null);
            leader.apply(task);
        }

        {
            // task with TaskClosure
            final ByteBuffer data = ByteBuffer.wrap("task closure".getBytes());
            final Vector<String> cbs = new Vector<>();
            final CountDownLatch latch = new CountDownLatch(1);
            final Task task = new Task(data, new TaskClosure() {

                @Override
                public void run(final Status status) {
                    cbs.add("apply");
                    latch.countDown();
                }

                @Override
                public void onCommitted() {
                    cbs.add("commit");

                }
            });
            leader.apply(task);
            latch.await();
            assertEquals(2, cbs.size());
            assertEquals("commit", cbs.get(0));
            assertEquals("apply", cbs.get(1));
        }

        cluster.ensureSame();
        assertEquals(2, cluster.getFollowers().size());
    }

    @Test
    public void testSingleNodeWithLearner() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final PeerId peer = new PeerId(addr, 0);

        final Endpoint learnerAddr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT + 1);
        final PeerId learnerPeer = new PeerId(learnerAddr, 0);

        final int cnt = 10;
        MockStateMachine learnerFsm;
        RaftGroupService learnerServer;
        {
            // Start learner
            final NodeOptions nodeOptions = createNodeOptions();
            learnerFsm = new MockStateMachine(learnerAddr);
            nodeOptions.setFsm(learnerFsm);
            nodeOptions.setLogUri(this.dataPath + File.separator + "log1");
            nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta1");
            nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot1");
            nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer), Collections
                .singletonList(learnerPeer)));

            learnerServer = createService("unittest", new PeerId(learnerAddr, 0), nodeOptions);
            learnerServer.start(true);
        }

        {
            // Start leader
            final NodeOptions nodeOptions = createNodeOptions();
            final MockStateMachine fsm = new MockStateMachine(addr);
            nodeOptions.setFsm(fsm);
            nodeOptions.setLogUri(this.dataPath + File.separator + "log");
            nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
            nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
            nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer), Collections
                .singletonList(learnerPeer)));

            RaftGroupService server = createService("unittest", new PeerId(addr, 0), nodeOptions);
            Node node = server.start(true);

            assertEquals(1, node.listPeers().size());
            assertTrue(node.listPeers().contains(peer));
            assertTrue(waitForCondition(() -> node.isLeader(), 1_000));

            sendTestTaskAndWait(node, cnt);
            assertEquals(cnt, fsm.getLogs().size());
            int i = 0;
            for (final ByteBuffer data : fsm.getLogs()) {
                assertEquals("hello" + i++, new String(data.array()));
            }
            Thread.sleep(1000); //wait for entries to be replicated to learner.
            server.shutdown();
        }
        {
            // assert learner fsm
            assertEquals(cnt, learnerFsm.getLogs().size());
            int i = 0;
            for (final ByteBuffer data : learnerFsm.getLogs()) {
                assertEquals("hello" + i++, new String(data.array()));
            }
            learnerServer.shutdown();
        }
    }

    @Test
    public void testResetLearners() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        final LinkedHashSet<PeerId> learners = new LinkedHashSet<>();

        for (int i = 0; i < 3; i++) {
            learners.add(new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3 + i));
        }

        cluster = new TestCluster("unittest", this.dataPath, peers, learners, 300);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }
        for (final PeerId peer : learners) {
            assertTrue(cluster.startLearner(peer));
        }

        // elect leader
        cluster.waitLeader();

        Node leader = cluster.getLeader();

        waitForCondition(() -> leader.listAlivePeers().size() == 3, 5_000);
        waitForCondition(() -> leader.listAliveLearners().size() == 3, 5_000);

        this.sendTestTaskAndWait(leader);
        Thread.sleep(500);
        List<MockStateMachine> fsms = cluster.getFsms();
        assertEquals(6, fsms.size());
        cluster.ensureSame();

        {
            // Reset learners to 2 nodes
            PeerId learnerPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);
            learners.remove(learnerPeer);
            assertEquals(2, learners.size());

            SynchronizedClosure done = new SynchronizedClosure();
            leader.resetLearners(new ArrayList<>(learners), done);
            assertTrue(done.await().isOk());
            assertEquals(2, leader.listAliveLearners().size());
            assertEquals(2, leader.listLearners().size());
            this.sendTestTaskAndWait(leader);
            Thread.sleep(500);

            assertEquals(6, fsms.size());

            MockStateMachine fsm = fsms.remove(3); // get the removed learner's fsm
            assertEquals(fsm.getAddress(), learnerPeer.getEndpoint());
            // Ensure no more logs replicated to the removed learner.
            assertTrue(cluster.getLeaderFsm().getLogs().size() > fsm.getLogs().size());
            assertEquals(cluster.getLeaderFsm().getLogs().size(), 2 * fsm.getLogs().size());
        }
        {
            // remove another learner
            PeerId learnerPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 4);
            SynchronizedClosure done = new SynchronizedClosure();
            leader.removeLearners(Arrays.asList(learnerPeer), done);
            assertTrue(done.await().isOk());

            this.sendTestTaskAndWait(leader);
            Thread.sleep(500);
            MockStateMachine fsm = fsms.remove(3); // get the removed learner's fsm
            assertEquals(fsm.getAddress(), learnerPeer.getEndpoint());
            // Ensure no more logs replicated to the removed learner.
            assertTrue(cluster.getLeaderFsm().getLogs().size() > fsm.getLogs().size());
            assertEquals(cluster.getLeaderFsm().getLogs().size(), fsm.getLogs().size() / 2 * 3);
        }

        assertEquals(3, leader.listAlivePeers().size());
        assertEquals(1, leader.listAliveLearners().size());
        assertEquals(1, leader.listLearners().size());
    }

    @Test
    public void testTripleNodesWithStaticLearners() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        LinkedHashSet<PeerId> learners = new LinkedHashSet<>();
        PeerId learnerPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);
        learners.add(learnerPeer);
        cluster.setLearners(learners);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();
        final Node leader = cluster.getLeader();

        assertEquals(3, leader.listPeers().size());
        assertEquals(leader.listLearners().size(), 1);
        assertTrue(leader.listLearners().contains(learnerPeer));
        assertTrue(leader.listAliveLearners().isEmpty());

        // start learner after cluster setup.
        assertTrue(cluster.start(learnerPeer.getEndpoint()));

        Thread.sleep(1000);

        assertEquals(3, leader.listPeers().size());
        assertEquals(leader.listLearners().size(), 1);
        assertEquals(leader.listAliveLearners().size(), 1);

        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();
        assertEquals(4, cluster.getFsms().size());
    }

    @Test
    public void testTripleNodesWithLearners() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        assertTrue(leader.listLearners().isEmpty());
        assertTrue(leader.listAliveLearners().isEmpty());

        {
            // Adds a learner
            SynchronizedClosure done = new SynchronizedClosure();
            PeerId learnerPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);
            // Start learner
            assertTrue(cluster.startLearner(learnerPeer));
            leader.addLearners(Arrays.asList(learnerPeer), done);
            assertTrue(done.await().isOk());
            assertEquals(1, leader.listAliveLearners().size());
            assertEquals(1, leader.listLearners().size());
        }

        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        {
            final ByteBuffer data = ByteBuffer.wrap("no closure".getBytes());
            final Task task = new Task(data, null);
            leader.apply(task);
        }

        {
            // task with TaskClosure
            final ByteBuffer data = ByteBuffer.wrap("task closure".getBytes());
            final Vector<String> cbs = new Vector<>();
            final CountDownLatch latch = new CountDownLatch(1);
            final Task task = new Task(data, new TaskClosure() {

                @Override
                public void run(final Status status) {
                    cbs.add("apply");
                    latch.countDown();
                }

                @Override
                public void onCommitted() {
                    cbs.add("commit");

                }
            });
            leader.apply(task);
            latch.await();
            assertEquals(2, cbs.size());
            assertEquals("commit", cbs.get(0));
            assertEquals("apply", cbs.get(1));
        }

        assertEquals(4, cluster.getFsms().size());
        assertEquals(2, cluster.getFollowers().size());
        assertEquals(1, cluster.getLearners().size());
        cluster.ensureSame();

        {
            // Adds another learner
            SynchronizedClosure done = new SynchronizedClosure();
            PeerId learnerPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 4);
            // Start learner
            assertTrue(cluster.startLearner(learnerPeer));
            leader.addLearners(Arrays.asList(learnerPeer), done);
            assertTrue(done.await().isOk());
            assertEquals(2, leader.listAliveLearners().size());
            assertEquals(2, leader.listLearners().size());
        }
        {
            // stop two followers
            for (Node follower : cluster.getFollowers()) {
                assertTrue(cluster.stop(follower.getNodeId().getPeerId().getEndpoint()));
            }
            // send a new task
            final ByteBuffer data = ByteBuffer.wrap("task closure".getBytes());
            SynchronizedClosure done = new SynchronizedClosure();
            leader.apply(new Task(data, done));
            // should fail
            assertFalse(done.await().isOk());
            assertEquals(RaftError.EPERM, done.getStatus().getRaftError());
            // One peer with two learners.
            assertEquals(3, cluster.getFsms().size());
            cluster.ensureSame();
        }
    }

    @Test
    public void testNodesWithPriorityElection() throws Exception {

        List<Integer> priorities = new ArrayList<>();
        priorities.add(100);
        priorities.add(40);
        priorities.add(40);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        assertEquals(100, leader.getNodeTargetPriority());
        assertEquals(100, leader.getLeaderId().getPriority());
        assertEquals(2, cluster.getFollowers().size());
    }

    @Test
    public void testNodesWithPartPriorityElection() throws Exception {

        List<Integer> priorities = new ArrayList<>();
        priorities.add(100);
        priorities.add(40);
        priorities.add(-1);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        assertEquals(2, cluster.getFollowers().size());
    }

    @Test
    public void testNodesWithSpecialPriorityElection() throws Exception {

        List<Integer> priorities = new ArrayList<Integer>();
        priorities.add(0);
        priorities.add(0);
        priorities.add(-1);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        assertEquals(2, cluster.getFollowers().size());
    }

    @Test
    public void testNodesWithZeroValPriorityElection() throws Exception {

        List<Integer> priorities = new ArrayList<Integer>();
        priorities.add(50);
        priorities.add(0);
        priorities.add(0);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        assertEquals(2, cluster.getFollowers().size());
        assertEquals(50, leader.getNodeTargetPriority());
        assertEquals(50, leader.getLeaderId().getPriority());
    }

    @Test
    public void testNoLeaderWithZeroValPriorityElection() throws Exception {
        List<Integer> priorities = new ArrayList<>();
        priorities.add(0);
        priorities.add(0);
        priorities.add(0);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        Thread.sleep(200);

        final List<Node> followers = cluster.getFollowers();
        assertEquals(3, followers.size());

        for (Node follower : followers) {
            assertEquals(0, follower.getNodeId().getPeerId().getPriority());
        }
    }

    @Test
    public void testLeaderStopAndReElectWithPriority() throws Exception {
        final List<Integer> priorities = new ArrayList<>();
        priorities.add(100);
        priorities.add(60);
        priorities.add(10);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        cluster.waitLeader();
        Node leader = cluster.getLeader();

        assertNotNull(leader);
        assertEquals(100, leader.getNodeId().getPeerId().getPriority());
        assertEquals(100, leader.getNodeTargetPriority());

        // apply tasks to leader
        sendTestTaskAndWait(leader);

        // stop leader
        assertTrue(cluster.stop(leader.getNodeId().getPeerId().getEndpoint()));

        // elect new leader
        cluster.waitLeader();
        leader = cluster.getLeader();

        assertNotNull(leader);

        // get current leader priority value
        int leaderPriority = leader.getNodeId().getPeerId().getPriority();

        // get current leader log size
        int peer1LogSize = cluster.getFsmByPeer(peers.get(1)).getLogs().size();
        int peer2LogSize = cluster.getFsmByPeer(peers.get(2)).getLogs().size();

        // if the leader is lower priority value
        if (leaderPriority == 10) {
            // we just compare the two peers' log size value;
            assertTrue(peer2LogSize > peer1LogSize);
        }
        else {
            assertEquals(60, leader.getNodeId().getPeerId().getPriority());
            assertEquals(100, leader.getNodeTargetPriority());
        }
    }

    @Test
    public void testRemoveLeaderWithPriority() throws Exception {
        final List<Integer> priorities = new ArrayList<Integer>();
        priorities.add(100);
        priorities.add(60);
        priorities.add(10);

        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), peer.getPriority()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(100, leader.getNodeTargetPriority());
        assertEquals(100, leader.getNodeId().getPeerId().getPriority());

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId oldLeader = leader.getNodeId().getPeerId().copy();
        final Endpoint oldLeaderAddr = oldLeader.getEndpoint();

        // remove old leader
        LOG.info("Remove old leader {}", oldLeader);
        CountDownLatch latch = new CountDownLatch(1);
        leader.removePeer(oldLeader, new ExpectClosure(latch));
        waitLatch(latch);
        assertEquals(60, leader.getNodeTargetPriority());

        // stop and clean old leader
        LOG.info("Stop and clean old leader {}", oldLeader);
        assertTrue(cluster.stop(oldLeaderAddr));
        cluster.clean(oldLeaderAddr);

        // elect new leader
        cluster.waitLeader();
        leader = cluster.getLeader();
        LOG.info("New leader is {}", leader);
        assertNotNull(leader);
        assertNotSame(leader, oldLeader);
    }

    @Test
    @Ignore // TODO asch https://issues.apache.org/jira/browse/IGNITE-14833
    public void testChecksum() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        // start with checksum validation
        {
            final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
            try {
                final RaftOptions raftOptions = new RaftOptions();
                raftOptions.setEnableLogEntryChecksum(true);
                for (final PeerId peer : peers) {
                    assertTrue(cluster.start(peer.getEndpoint(), false, 300, true, null, raftOptions));
                }

                cluster.waitLeader();
                final Node leader = cluster.getLeader();
                assertNotNull(leader);
                assertEquals(3, leader.listPeers().size());
                this.sendTestTaskAndWait(leader);
                cluster.ensureSame();
            }
            finally {
                cluster.stopAll();
            }
        }

        // restart with peer3 enable checksum validation
        {
            final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
            try {
                RaftOptions raftOptions = new RaftOptions();
                raftOptions.setEnableLogEntryChecksum(false);
                for (final PeerId peer : peers) {
                    if (peer.equals(peers.get(2))) {
                        raftOptions = new RaftOptions();
                        raftOptions.setEnableLogEntryChecksum(true);
                    }
                    assertTrue(cluster.start(peer.getEndpoint(), false, 300, true, null, raftOptions));
                }

                cluster.waitLeader();
                final Node leader = cluster.getLeader();
                assertNotNull(leader);
                assertEquals(3, leader.listPeers().size());
                this.sendTestTaskAndWait(leader);
                cluster.ensureSame();
            }
            finally {
                cluster.stopAll();
            }
        }

        // restart with no checksum validation
        {
            final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
            try {
                final RaftOptions raftOptions = new RaftOptions();
                raftOptions.setEnableLogEntryChecksum(false);
                for (final PeerId peer : peers) {
                    assertTrue(cluster.start(peer.getEndpoint(), false, 300, true, null, raftOptions));
                }

                cluster.waitLeader();
                final Node leader = cluster.getLeader();
                assertNotNull(leader);
                assertEquals(3, leader.listPeers().size());
                this.sendTestTaskAndWait(leader);
                cluster.ensureSame();
            }
            finally {
                cluster.stopAll();
            }
        }

        // restart with all peers enable checksum validation
        {
            final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
            try {
                final RaftOptions raftOptions = new RaftOptions();
                raftOptions.setEnableLogEntryChecksum(true);
                for (final PeerId peer : peers) {
                    assertTrue(cluster.start(peer.getEndpoint(), false, 300, true, null, raftOptions));
                }

                cluster.waitLeader();
                final Node leader = cluster.getLeader();
                assertNotNull(leader);
                assertEquals(3, leader.listPeers().size());
                this.sendTestTaskAndWait(leader);
                cluster.ensureSame();
            }
            finally {
                cluster.stopAll();
            }
        }

    }

    @Test
    public void testReadIndex() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, true));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        // first call will fail-fast when no connection
        if (!assertReadIndex(leader, 11)) {
            assertTrue(assertReadIndex(leader, 11));
        }

        // read from follower
        for (final Node follower : cluster.getFollowers()) {
            assertNotNull(follower);

            assertTrue(waitForCondition(() -> leader.getNodeId().getPeerId().equals(follower.getLeaderId()), 5_000));

            assertReadIndex(follower, 11);
        }

        // read with null request context
        final CountDownLatch latch = new CountDownLatch(1);
        leader.readIndex(null, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertNull(reqCtx);
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
    }

    @Test // TODO asch do we need read index timeout ? https://issues.apache.org/jira/browse/IGNITE-14832
    public void testReadIndexTimeout() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, true));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        // apply tasks to leader
        sendTestTaskAndWait(leader);

        // first call will fail-fast when no connection
        if (!assertReadIndex(leader, 11)) {
            assertTrue(assertReadIndex(leader, 11));
        }

        // read from follower
        for (final Node follower : cluster.getFollowers()) {
            assertNotNull(follower);

            assertTrue(waitForCondition(() -> leader.getNodeId().getPeerId().equals(follower.getLeaderId()), 5_000));

            assertReadIndex(follower, 11);
        }

        // read with null request context
        final CountDownLatch latch = new CountDownLatch(1);
        final long start = System.currentTimeMillis();
        leader.readIndex(null, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertNull(reqCtx);
                if (status.isOk()) {
                    System.err.println("Read-index so fast: " + (System.currentTimeMillis() - start) + "ms");
                }
                else {
                    assertEquals(status, new Status(RaftError.ETIMEDOUT, "read-index request timeout"));
                    assertEquals(index, -1);
                }
                latch.countDown();
            }
        });
        latch.await();
    }

    @Test
    public void testReadIndexFromLearner() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, true));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        {
            // Adds a learner
            SynchronizedClosure done = new SynchronizedClosure();
            PeerId learnerPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);
            // Start learner
            assertTrue(cluster.startLearner(learnerPeer));
            leader.addLearners(Arrays.asList(learnerPeer), done);
            assertTrue(done.await().isOk());
            assertEquals(1, leader.listAliveLearners().size());
            assertEquals(1, leader.listLearners().size());
        }

        Thread.sleep(100);
        // read from learner
        Node learner = cluster.getNodes().get(3);
        assertNotNull(leader);
        assertReadIndex(learner, 12);
        assertReadIndex(learner, 12);
    }

    @Test
    public void testReadIndexChaos() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, true));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());

        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 100; i++) {
                            try {
                                sendTestTaskAndWait(leader);
                            }
                            catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            readIndexRandom(cluster);
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                }

                private void readIndexRandom(final TestCluster cluster) {
                    final CountDownLatch readLatch = new CountDownLatch(1);
                    final byte[] requestContext = TestUtils.getRandomBytes();
                    cluster.getNodes().get(ThreadLocalRandom.current().nextInt(3))
                        .readIndex(requestContext, new ReadIndexClosure() {

                            @Override
                            public void run(final Status status, final long index, final byte[] reqCtx) {
                                if (status.isOk()) {
                                    assertTrue(status.toString(), status.isOk());
                                    assertTrue(index > 0);
                                    assertArrayEquals(requestContext, reqCtx);
                                }
                                readLatch.countDown();
                            }
                        });
                    try {
                        readLatch.await();
                    }
                    catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }.start();
        }

        latch.await();

        cluster.ensureSame();

        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(10000, fsm.getLogs().size());
        }
    }

    @SuppressWarnings({"unused", "SameParameterValue"})
    private boolean assertReadIndex(final Node node, final int index) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final byte[] requestContext = TestUtils.getRandomBytes();
        final AtomicBoolean success = new AtomicBoolean(false);
        node.readIndex(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long theIndex, final byte[] reqCtx) {
                if (status.isOk()) {
                    assertEquals(index, theIndex);
                    assertArrayEquals(requestContext, reqCtx);
                    success.set(true);
                }
                else {
                    assertTrue(status.getErrorMsg(), status.getErrorMsg().contains("RPC exception:Check connection["));
                    assertTrue(status.getErrorMsg(), status.getErrorMsg().contains("] fail and try to create new one"));
                }
                latch.countDown();
            }
        });
        latch.await();
        return success.get();
    }

    @Test
    public void testNodeMetrics() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, true));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        {
            final ByteBuffer data = ByteBuffer.wrap("no closure".getBytes());
            final Task task = new Task(data, null);
            leader.apply(task);
        }

        cluster.ensureSame();
        for (final Node node : cluster.getNodes()) {
            System.out.println("-------------" + node.getNodeId() + "-------------");
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(node.getNodeMetrics().getMetricRegistry())
                .build();
            reporter.report();
            reporter.close();
            System.out.println();
        }
        // TODO check http status https://issues.apache.org/jira/browse/IGNITE-14832
        assertEquals(2, cluster.getFollowers().size());
    }

    @Test
    public void testLeaderFail() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        Node leader = cluster.getLeader();
        assertNotNull(leader);
        LOG.info("Current leader is {}", leader.getLeaderId());
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        List<Node> followers = cluster.getFollowers();

        for (Node follower : followers) {
            NodeImpl follower0 = (NodeImpl) follower;
            DefaultRaftClientService rpcService = (DefaultRaftClientService) follower0.getRpcClientService();
            RpcClientEx rpcClientEx = (RpcClientEx) rpcService.getRpcClient();
            rpcClientEx.blockMessages(new BiPredicate<Object, String>() {
                @Override public boolean test(Object msg, String nodeId) {
                    if (msg instanceof RpcRequests.RequestVoteRequest) {
                        RpcRequests.RequestVoteRequest msg0 = (RpcRequests.RequestVoteRequest) msg;

                        return !msg0.getPreVote();
                    }

                    return false;
                }
            });
        }

        // stop leader
        LOG.warn("Stop leader {}", leader.getNodeId().getPeerId());
        final PeerId oldLeader = leader.getNodeId().getPeerId();
        assertTrue(cluster.stop(leader.getNodeId().getPeerId().getEndpoint()));

        // apply something when follower
        //final List<Node> followers = cluster.getFollowers();
        assertFalse(followers.isEmpty());
        this.sendTestTaskAndWait("follower apply ", followers.get(0), -1);

        for (Node follower : followers) {
            NodeImpl follower0 = (NodeImpl) follower;
            DefaultRaftClientService rpcService = (DefaultRaftClientService) follower0.getRpcClientService();
            RpcClientEx rpcClientEx = (RpcClientEx) rpcService.getRpcClient();
            rpcClientEx.stopBlock();
        }

        // elect new leader
        cluster.waitLeader();
        leader = cluster.getLeader();
        LOG.info("Elect new leader is {}", leader.getLeaderId());
        // apply tasks to new leader
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 10; i < 20; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            final Task task = new Task(data, new ExpectClosure(latch));
            leader.apply(task);
        }
        waitLatch(latch);

        // restart old leader
        LOG.info("restart old leader {}", oldLeader);
        assertTrue(cluster.start(oldLeader.getEndpoint()));
        // apply something
        latch = new CountDownLatch(10);
        for (int i = 20; i < 30; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            final Task task = new Task(data, new ExpectClosure(latch));
            leader.apply(task);
        }
        waitLatch(latch);

        // stop and clean old leader
        cluster.stop(oldLeader.getEndpoint());
        cluster.clean(oldLeader.getEndpoint());

        // restart old leader
        LOG.info("restart old leader {}", oldLeader);
        assertTrue(cluster.start(oldLeader.getEndpoint()));
        cluster.ensureSame();

        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(30, fsm.getLogs().size());
        }
    }

    @Test
    public void testJoinNodes() throws Exception {
        final PeerId peer0 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final PeerId peer1 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 1);
        final PeerId peer2 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 2);
        final PeerId peer3 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + 3);

        final ArrayList<PeerId> peers = new ArrayList<>();
        peers.add(peer0);

        // start single cluster
        cluster = new TestCluster("unittest", this.dataPath, peers);
        assertTrue(cluster.start(peer0.getEndpoint()));

        cluster.waitLeader();

        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        Assert.assertEquals(leader.getNodeId().getPeerId(), peer0);
        this.sendTestTaskAndWait(leader);

        // start peer1
        assertTrue(cluster.start(peer1.getEndpoint(), false, 300));

        // add peer1
        CountDownLatch latch = new CountDownLatch(1);
        peers.add(peer1);
        leader.addPeer(peer1, new ExpectClosure(latch));
        waitLatch(latch);

        cluster.ensureSame();
        assertEquals(2, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(10, fsm.getLogs().size());
        }

        // add peer2 but not start
        peers.add(peer2);
        latch = new CountDownLatch(1);
        leader.addPeer(peer2, new ExpectClosure(RaftError.ECATCHUP, latch));
        waitLatch(latch);

        // start peer2 after 2 seconds
        Thread.sleep(2000);
        assertTrue(cluster.start(peer2.getEndpoint(), false, 300));

        // re-add peer2
        latch = new CountDownLatch(2);
        leader.addPeer(peer2, new ExpectClosure(latch));
        // concurrent configuration change
        leader.addPeer(peer3, new ExpectClosure(RaftError.EBUSY, latch));
        waitLatch(latch);

        // re-add peer2 directly

        try {
            leader.addPeer(peer2, new ExpectClosure(latch));
            fail();
        }
        catch (final IllegalArgumentException e) {
            assertEquals("Peer already exists in current configuration", e.getMessage());
        }

        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        assertEquals(2, cluster.getFollowers().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(10, fsm.getLogs().size());
        }
    }

    private void waitLatch(final CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testRemoveFollower() throws Exception {
        List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();

        List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId followerPeer = followers.get(0).getNodeId().getPeerId();
        final Endpoint followerAddr = followerPeer.getEndpoint();

        // stop and clean follower
        LOG.info("Stop and clean follower {}", followerPeer);
        assertTrue(cluster.stop(followerAddr));
        cluster.clean(followerAddr);

        // remove follower
        LOG.info("Remove follower {}", followerPeer);
        CountDownLatch latch = new CountDownLatch(1);
        leader.removePeer(followerPeer, new ExpectClosure(latch));
        waitLatch(latch);

        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);
        followers = cluster.getFollowers();
        assertEquals(1, followers.size());

        peers = TestUtils.generatePeers(3);
        assertTrue(peers.remove(followerPeer));

        // start follower
        LOG.info("Start and add follower {}", followerPeer);
        assertTrue(cluster.start(followerAddr));
        // re-add follower
        latch = new CountDownLatch(1);
        leader.addPeer(followerPeer, new ExpectClosure(latch));
        waitLatch(latch);

        followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }
    }

    @Test
    public void testRemoveLeader() throws Exception {
        List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();

        List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId oldLeader = leader.getNodeId().getPeerId().copy();
        final Endpoint oldLeaderAddr = oldLeader.getEndpoint();

        // remove old leader
        LOG.info("Remove old leader {}", oldLeader);
        CountDownLatch latch = new CountDownLatch(1);
        leader.removePeer(oldLeader, new ExpectClosure(latch));
        waitLatch(latch);
        Thread.sleep(100);

        // elect new leader
        cluster.waitLeader();
        leader = cluster.getLeader();
        LOG.info("New leader is {}", leader);
        assertNotNull(leader);
        // apply tasks to new leader
        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);

        // stop and clean old leader
        LOG.info("Stop and clean old leader {}", oldLeader);
        assertTrue(cluster.stop(oldLeaderAddr));
        cluster.clean(oldLeaderAddr);

        // Add and start old leader
        LOG.info("Start and add old leader {}", oldLeader);
        assertTrue(cluster.start(oldLeaderAddr));

        peers = TestUtils.generatePeers(3);
        assertTrue(peers.remove(oldLeader));
        latch = new CountDownLatch(1);
        leader.addPeer(oldLeader, new ExpectClosure(latch));
        waitLatch(latch);

        followers = cluster.getFollowers();
        assertEquals(2, followers.size());
        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }
    }

    @Test
    public void testPreVote() throws Exception {
        List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        // get leader
        Node leader = cluster.getLeader();
        final long savedTerm = ((NodeImpl) leader).getCurrentTerm();
        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId followerPeer = followers.get(0).getNodeId().getPeerId();
        final Endpoint followerAddr = followerPeer.getEndpoint();

        // remove follower
        LOG.info("Remove follower {}", followerPeer);
        CountDownLatch latch = new CountDownLatch(1);
        leader.removePeer(followerPeer, new ExpectClosure(latch));
        waitLatch(latch);

        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);

        Thread.sleep(2000);

        // add follower
        LOG.info("Add follower {}", followerAddr);
        peers = TestUtils.generatePeers(3);
        assertTrue(peers.remove(followerPeer));
        latch = new CountDownLatch(1);
        leader.addPeer(followerPeer, new ExpectClosure(latch));
        waitLatch(latch);
        leader = cluster.getLeader();
        assertNotNull(leader);
        // leader term should not be changed.
        assertEquals(savedTerm, ((NodeImpl) leader).getCurrentTerm());
    }

    @Test
    public void testSetPeer1() throws Exception {
        cluster = new TestCluster("testSetPeer1", this.dataPath, new ArrayList<>());

        final PeerId bootPeer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        assertTrue(cluster.start(bootPeer.getEndpoint()));
        final List<Node> nodes = cluster.getFollowers();
        assertEquals(1, nodes.size());

        final List<PeerId> peers = new ArrayList<>();
        peers.add(bootPeer);
        // reset peers from empty
        assertTrue(nodes.get(0).resetPeers(new Configuration(peers)).isOk());
        cluster.waitLeader();
        assertNotNull(cluster.getLeader());
    }

    @Test
    public void testSetPeer2() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        // get leader
        Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId followerPeer1 = followers.get(0).getNodeId().getPeerId();
        final Endpoint followerAddr1 = followerPeer1.getEndpoint();
        final PeerId followerPeer2 = followers.get(1).getNodeId().getPeerId();
        final Endpoint followerAddr2 = followerPeer2.getEndpoint();

        LOG.info("Stop and clean follower {}", followerPeer1);
        assertTrue(cluster.stop(followerAddr1));
        cluster.clean(followerAddr1);

        // apply tasks to leader again
        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);
        // set peer when no quorum die
        final Endpoint leaderAddr = leader.getLeaderId().getEndpoint().copy();
        LOG.info("Set peers to {}", leaderAddr);

        LOG.info("Stop and clean follower {}", followerPeer2);
        assertTrue(cluster.stop(followerAddr2));
        cluster.clean(followerAddr2);

        assertTrue(waitForTopology(cluster, leaderAddr, 1, 5_000));

        // leader will step-down, become follower
        Thread.sleep(2000);
        List<PeerId> newPeers = new ArrayList<>();
        newPeers.add(new PeerId(leaderAddr, 0));

        // new peers equal to current conf
        assertTrue(leader.resetPeers(new Configuration(peers)).isOk());
        // set peer when quorum die
        LOG.warn("Set peers to {}", leaderAddr);
        assertTrue(leader.resetPeers(new Configuration(newPeers)).isOk());

        cluster.waitLeader();
        leader = cluster.getLeader();
        assertNotNull(leader);
        Assert.assertEquals(leaderAddr, leader.getNodeId().getPeerId().getEndpoint());

        LOG.info("start follower {}", followerAddr1);
        assertTrue(cluster.start(followerAddr1, true, 300));
        LOG.info("start follower {}", followerAddr2);
        assertTrue(cluster.start(followerAddr2, true, 300));

        // Make sure the leader has discovered new nodes.
        assertTrue(waitForTopology(cluster, leaderAddr, 3, 30_000)); // Discovery may take a while sometimes.

        CountDownLatch latch = new CountDownLatch(1);
        LOG.info("Add old follower {}", followerAddr1);
        leader.addPeer(followerPeer1, new ExpectClosure(latch));
        waitLatch(latch);

        latch = new CountDownLatch(1);
        LOG.info("Add old follower {}", followerAddr2);
        leader.addPeer(followerPeer2, new ExpectClosure(latch));
        waitLatch(latch);

        newPeers.add(followerPeer1);
        newPeers.add(followerPeer2);

        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testRestoreSnasphot() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        // get leader
        final Node leader = cluster.getLeader();

        LOG.info("Leader: " + leader);

        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();
        triggerLeaderSnapshot(cluster, leader);

        // stop leader
        final Endpoint leaderAddr = leader.getNodeId().getPeerId().getEndpoint().copy();
        assertTrue(cluster.stop(leaderAddr));

        // restart leader
        cluster.waitLeader();
        assertEquals(0, cluster.getLeaderFsm().getLoadSnapshotTimes());
        assertTrue(cluster.start(leaderAddr));
        cluster.ensureSame();
        assertEquals(0, cluster.getLeaderFsm().getLoadSnapshotTimes());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testRestoreSnapshotWithDelta() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        // get leader
        final Node leader = cluster.getLeader();

        LOG.info("Leader: " + leader);

        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();
        triggerLeaderSnapshot(cluster, leader);

        // stop leader
        final Endpoint leaderAddr = leader.getNodeId().getPeerId().getEndpoint().copy();
        assertTrue(cluster.stop(leaderAddr));

        // restart leader
        cluster.waitLeader();

        sendTestTaskAndWait(cluster.getLeader(), 10, RaftError.SUCCESS);

        assertEquals(0, cluster.getLeaderFsm().getLoadSnapshotTimes());
        assertTrue(cluster.start(leaderAddr));

        Node oldLeader = cluster.getNode(leaderAddr);

        cluster.ensureSame();
        assertEquals(0, cluster.getLeaderFsm().getLoadSnapshotTimes());

        MockStateMachine fsm = (MockStateMachine) oldLeader.getOptions().getFsm();
        assertEquals(1, fsm.getLoadSnapshotTimes());
    }

    private void triggerLeaderSnapshot(final TestCluster cluster, final Node leader) throws InterruptedException {
        this.triggerLeaderSnapshot(cluster, leader, 1);
    }

    private void triggerLeaderSnapshot(final TestCluster cluster, final Node leader, final int times)
        throws InterruptedException {
        // trigger leader snapshot
        // first snapshot will be triggered randomly
        int snapshotTimes = cluster.getLeaderFsm().getSaveSnapshotTimes();
        assertTrue("snapshotTimes=" + snapshotTimes + ", times=" + times, snapshotTimes == times - 1
            || snapshotTimes == times);
        final CountDownLatch latch = new CountDownLatch(1);
        leader.snapshot(new ExpectClosure(latch));
        waitLatch(latch);
        assertEquals(snapshotTimes + 1, cluster.getLeaderFsm().getSaveSnapshotTimes());
    }

    @Test
    public void testInstallSnapshotWithThrottle() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 200, false, new ThroughputSnapshotThrottle(1024, 1)));
        }

        cluster.waitLeader();
        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();

        // stop follower1
        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final Endpoint followerAddr = followers.get(0).getNodeId().getPeerId().getEndpoint();
        assertTrue(cluster.stop(followerAddr));

        cluster.waitLeader();

        // apply something more
        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);

        Thread.sleep(1000);

        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader);
        // apply something more
        this.sendTestTaskAndWait(leader, 20, RaftError.SUCCESS);
        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader, 2);

        // wait leader to compact logs
        Thread.sleep(1000);

        // restart follower.
        cluster.clean(followerAddr);
        assertTrue(cluster.start(followerAddr, true, 300, false, new ThroughputSnapshotThrottle(1024, 1)));

        Thread.sleep(2000);
        cluster.ensureSame();

        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(30, fsm.getLogs().size());
        }
    }

    @Test // TODO add test for timeout on snapshot install https://issues.apache.org/jira/browse/IGNITE-14832
    public void testInstallLargeSnapshotWithThrottle() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(4);
        cluster = new TestCluster("unitest", this.dataPath, peers.subList(0, 3));
        for (int i = 0; i < peers.size() - 1; i++) {
            final PeerId peer = peers.get(i);
            final boolean started = cluster.start(peer.getEndpoint(), false, 200, false);
            assertTrue(started);
        }
        cluster.waitLeader();
        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        sendTestTaskAndWait(leader, 0, RaftError.SUCCESS);

        cluster.ensureSame();

        // apply something more
        for (int i = 1; i < 100; i++) {
            sendTestTaskAndWait(leader, i * 10, RaftError.SUCCESS);
        }

        Thread.sleep(1000);

        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader);

        // apply something more
        for (int i = 100; i < 200; i++) {
            sendTestTaskAndWait(leader, i * 10, RaftError.SUCCESS);
        }
        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader, 2);

        // wait leader to compact logs
        Thread.sleep(1000);

        // add follower
        final PeerId newPeer = peers.get(3);
        final SnapshotThrottle snapshotThrottle = new ThroughputSnapshotThrottle(128, 1);
        final boolean started = cluster.start(newPeer.getEndpoint(), false, 300, false, snapshotThrottle);
        assertTrue(started);

        final CountDownLatch latch = new CountDownLatch(1);
        leader.addPeer(newPeer, status -> {
            assertTrue(status.toString(), status.isOk());
            latch.countDown();
        });
        waitLatch(latch);

        cluster.ensureSame();

        assertEquals(4, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(2000, fsm.getLogs().size());
        }
    }

    @Test
    public void testInstallLargeSnapshot() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(4);
        cluster = new TestCluster("unitest", this.dataPath, peers.subList(0, 3));
        for (int i = 0; i < peers.size() - 1; i++) {
            final PeerId peer = peers.get(i);
            final boolean started = cluster.start(peer.getEndpoint(), false, 200, false);
            assertTrue(started);
        }
        cluster.waitLeader();
        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        sendTestTaskAndWait(leader, 0, RaftError.SUCCESS);

        cluster.ensureSame();

        // apply something more
        for (int i = 1; i < 100; i++) {
            sendTestTaskAndWait(leader, i * 10, RaftError.SUCCESS);
        }

        Thread.sleep(1000);

        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader);

        // apply something more
        for (int i = 100; i < 200; i++) {
            sendTestTaskAndWait(leader, i * 10, RaftError.SUCCESS);
        }
        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader, 2);

        // wait leader to compact logs
        Thread.sleep(1000);

        // add follower
        final PeerId newPeer = peers.get(3);
        final RaftOptions raftOptions = new RaftOptions();
        raftOptions.setMaxByteCountPerRpc(128);
        final boolean started = cluster.start(newPeer.getEndpoint(), false, 300, false, null, raftOptions);
        assertTrue(started);

        final CountDownLatch latch = new CountDownLatch(1);
        leader.addPeer(newPeer, status -> {
            assertTrue(status.toString(), status.isOk());
            latch.countDown();
        });
        waitLatch(latch);

        cluster.ensureSame();

        assertEquals(4, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(2000, fsm.getLogs().size());
        }
    }

    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-14853")
    public void testInstallSnapshot() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        // apply tasks to leader
        this.sendTestTaskAndWait(leader);

        cluster.ensureSame();

        // stop follower1
        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final Endpoint followerAddr = followers.get(0).getNodeId().getPeerId().getEndpoint();
        assertTrue(cluster.stop(followerAddr));

        // apply something more
        sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);

        // trigger leader snapshot
        triggerLeaderSnapshot(cluster, leader);
        // apply something more
        sendTestTaskAndWait(leader, 20, RaftError.SUCCESS);
        triggerLeaderSnapshot(cluster, leader, 2);

        // wait leader to compact logs
        Thread.sleep(50);

        //restart follower.
        cluster.clean(followerAddr);
        assertTrue(cluster.start(followerAddr, false, 300));

        cluster.ensureSame();

        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(fsm.getAddress().toString(), 30, fsm.getLogs().size());
        }
    }

    @Test
    public void testNoSnapshot() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final NodeOptions nodeOptions = createNodeOptions();
        final MockStateMachine fsm = new MockStateMachine(addr);
        nodeOptions.setFsm(fsm);
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(new PeerId(addr, 0))));

        RaftGroupService service = createService("unittest", new PeerId(addr, 0), nodeOptions);
        final Node node = service.start();
        // wait node elect self as leader

        Thread.sleep(2000);

        this.sendTestTaskAndWait(node);

        assertEquals(0, fsm.getSaveSnapshotTimes());
        // do snapshot but returns error
        CountDownLatch latch = new CountDownLatch(1);
        node.snapshot(new ExpectClosure(RaftError.EINVAL, "Snapshot is not supported", latch));
        waitLatch(latch);
        assertEquals(0, fsm.getSaveSnapshotTimes());

        service.shutdown();
    }

    @Test
    public void testAutoSnapshot() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        final NodeOptions nodeOptions = createNodeOptions();
        final MockStateMachine fsm = new MockStateMachine(addr);
        nodeOptions.setFsm(fsm);
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotIntervalSecs(10);
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(new PeerId(addr, 0))));

        RaftGroupService service = createService("unittest", new PeerId(addr, 0), nodeOptions);
        final Node node = service.start();
        // wait node elect self as leader
        Thread.sleep(2000);

        sendTestTaskAndWait(node);

        // wait for auto snapshot
        Thread.sleep(10000);
        // first snapshot will be triggered randomly
        final int times = fsm.getSaveSnapshotTimes();
        assertTrue("snapshotTimes=" + times, times >= 1);
        assertTrue(fsm.getSnapshotIndex() > 0);

        service.shutdown();
    }

    @Test
    public void testLeaderShouldNotChange() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        // get leader
        final Node leader0 = cluster.getLeader();
        assertNotNull(leader0);
        final long savedTerm = ((NodeImpl) leader0).getCurrentTerm();
        LOG.info("Current leader is {}, term is {}", leader0, savedTerm);
        Thread.sleep(5000);
        cluster.waitLeader();
        final Node leader1 = cluster.getLeader();
        assertNotNull(leader1);
        LOG.info("Current leader is {}", leader1);
        assertEquals(savedTerm, ((NodeImpl) leader1).getCurrentTerm());
    }

    @Test
    public void testRecoverFollower() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();

        final Node leader = cluster.getLeader();
        assertNotNull(leader);

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        // Ensure the quorum before stopping a follower, otherwise leader can step down.
        assertTrue(waitForCondition(() -> followers.get(1).getLeaderId() != null, 5_000));

        final Endpoint followerAddr = followers.get(0).getNodeId().getPeerId().getEndpoint().copy();
        assertTrue(cluster.stop(followerAddr));

        this.sendTestTaskAndWait(leader);

        for (int i = 10; i < 30; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("no cluster" + i).getBytes());
            final Task task = new Task(data, null);
            leader.apply(task);
        }
        // wait leader to compact logs
        Thread.sleep(5000);
        // restart follower
        assertTrue(cluster.start(followerAddr));
        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(30, fsm.getLogs().size());
        }
    }

    @Test
    public void testLeaderTransfer() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 300);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();
        assertNotNull(leader);
        this.sendTestTaskAndWait(leader);

        Thread.sleep(100);

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId targetPeer = followers.get(0).getNodeId().getPeerId().copy();
        LOG.info("Transfer leadership from {} to {}", leader, targetPeer);
        assertTrue(leader.transferLeadershipTo(targetPeer).isOk());
        Thread.sleep(1000);
        cluster.waitLeader();
        leader = cluster.getLeader();
        Assert.assertEquals(leader.getNodeId().getPeerId(), targetPeer);
    }

    @Test
    public void testLeaderTransferBeforeLogIsCompleted() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 300);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 1));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();
        assertNotNull(leader);

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        // Ensure the quorum before stopping a follower, otherwise leader can step down.
        assertTrue(waitForCondition(() -> followers.get(1).getLeaderId() != null, 5_000));

        final PeerId targetPeer = followers.get(0).getNodeId().getPeerId().copy();
        assertTrue(cluster.stop(targetPeer.getEndpoint()));

        this.sendTestTaskAndWait(leader);
        LOG.info("Transfer leadership from {} to {}", leader, targetPeer);
        assertTrue(leader.transferLeadershipTo(targetPeer).isOk());

        final CountDownLatch latch = new CountDownLatch(1);
        final Task task = new Task(ByteBuffer.wrap("aaaaa".getBytes()), new ExpectClosure(RaftError.EBUSY, latch));
        leader.apply(task);
        waitLatch(latch);

        cluster.waitLeader();

        assertTrue(cluster.start(targetPeer.getEndpoint()));

        leader = cluster.getLeader();

        Assert.assertNotEquals(targetPeer, leader.getNodeId().getPeerId());
        cluster.ensureSame();
    }

    @Test
    public void testLeaderTransferResumeOnFailure() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 300);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 1));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();
        assertNotNull(leader);

        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());

        final PeerId targetPeer = followers.get(0).getNodeId().getPeerId().copy();
        assertTrue(cluster.stop(targetPeer.getEndpoint()));

        this.sendTestTaskAndWait(leader);

        assertTrue(leader.transferLeadershipTo(targetPeer).isOk());
        final Node savedLeader = leader;
        //try to apply task when transferring leadership
        CountDownLatch latch = new CountDownLatch(1);
        Task task = new Task(ByteBuffer.wrap("aaaaa".getBytes()), new ExpectClosure(RaftError.EBUSY, latch));
        leader.apply(task);
        waitLatch(latch);

        Thread.sleep(100);
        cluster.waitLeader();
        leader = cluster.getLeader();
        assertSame(leader, savedLeader);

        // restart target peer
        assertTrue(cluster.start(targetPeer.getEndpoint()));
        Thread.sleep(100);
        // retry apply task
        latch = new CountDownLatch(1);
        task = new Task(ByteBuffer.wrap("aaaaa".getBytes()), new ExpectClosure(latch));
        leader.apply(task);
        waitLatch(latch);

        cluster.ensureSame();
    }

    /**
     * mock state machine that fails to load snapshot.
     */
    static class MockFSM1 extends MockStateMachine {

        MockFSM1() {
            this(new Endpoint(Utils.IP_ANY, 0));
        }

        MockFSM1(final Endpoint address) {
            super(address);
        }

        @Override
        public boolean onSnapshotLoad(final SnapshotReader reader) {
            return false;
        }
    }

    @Test
    public void testShutdownAndJoinWorkAfterInitFails() throws Exception {
        final Endpoint addr = new Endpoint(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        {
            final NodeOptions nodeOptions = createNodeOptions();
            final MockStateMachine fsm = new MockStateMachine(addr);
            nodeOptions.setFsm(fsm);
            nodeOptions.setLogUri(this.dataPath + File.separator + "log");
            nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
            nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
            nodeOptions.setSnapshotIntervalSecs(10);
            nodeOptions.setInitialConf(new Configuration(Collections.singletonList(new PeerId(addr, 0))));

            RaftGroupService service = createService("unittest", new PeerId(addr, 0), nodeOptions);
            final Node node = service.start(true);

            Thread.sleep(1000);
            this.sendTestTaskAndWait(node);

            // save snapshot
            final CountDownLatch latch = new CountDownLatch(1);
            node.snapshot(new ExpectClosure(latch));
            waitLatch(latch);
            service.shutdown();
        }
        {
            final NodeOptions nodeOptions = createNodeOptions();
            final MockStateMachine fsm = new MockFSM1(addr);
            nodeOptions.setFsm(fsm);
            nodeOptions.setLogUri(this.dataPath + File.separator + "log");
            nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
            nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
            nodeOptions.setSnapshotIntervalSecs(10);
            nodeOptions.setInitialConf(new Configuration(Collections.singletonList(new PeerId(addr, 0))));

            RaftGroupService service = createService("unittest", new PeerId(addr, 0), nodeOptions);
            try {
                service.start(true);

                fail();
            }
            catch (Exception e) {
                // Expected.
            }
            finally {
                service.shutdown();
            }

        }
    }

    /**
     * 4.2.2 Removing the current leader
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShuttingDownLeaderTriggerTimeoutNow() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 300);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();
        assertNotNull(leader);
        final Node oldLeader = leader;

        LOG.info("Shutdown leader {}", leader);
        leader.shutdown();
        leader.join();

        cluster.waitLeader();
        leader = cluster.getLeader();

        assertNotNull(leader);
        assertNotSame(leader, oldLeader);
    }

    @Test
    public void testRemovingLeaderTriggerTimeoutNow() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 300);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();

        // Ensure the quorum before removing a leader, otherwise removePeer can be rejected.
        for (Node follower : cluster.getFollowers()) {
            assertTrue(waitForCondition(() -> follower.getLeaderId() != null, 5_000));
        }

        Node leader = cluster.getLeader();
        assertNotNull(leader);
        final Node oldLeader = leader;

        final CountDownLatch latch = new CountDownLatch(1);
        oldLeader.removePeer(oldLeader.getNodeId().getPeerId(), new ExpectClosure(latch));
        waitLatch(latch);

        cluster.waitLeader();
        leader = cluster.getLeader();
        assertNotNull(leader);
        assertNotSame(leader, oldLeader);
    }

    @Test
    public void testTransferShouldWorkAfterInstallSnapshot() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 1000);

        for (int i = 0; i < peers.size() - 1; i++) {
            assertTrue(cluster.start(peers.get(i).getEndpoint()));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();
        assertNotNull(leader);

        this.sendTestTaskAndWait(leader);

        final List<Node> followers = cluster.getFollowers();
        assertEquals(1, followers.size());

        final PeerId follower = followers.get(0).getNodeId().getPeerId();
        assertTrue(leader.transferLeadershipTo(follower).isOk());
        Thread.sleep(2000);
        leader = cluster.getLeader();
        Assert.assertEquals(follower, leader.getNodeId().getPeerId());

        CountDownLatch latch = new CountDownLatch(1);
        leader.snapshot(new ExpectClosure(latch));
        waitLatch(latch);
        latch = new CountDownLatch(1);
        leader.snapshot(new ExpectClosure(latch));
        waitLatch(latch);

        // start the last peer which should be recover with snapshot.
        final PeerId lastPeer = peers.get(2);
        assertTrue(cluster.start(lastPeer.getEndpoint()));
        Thread.sleep(5000);
        assertTrue(leader.transferLeadershipTo(lastPeer).isOk());
        Thread.sleep(2000);
        leader = cluster.getLeader();
        Assert.assertEquals(lastPeer, leader.getNodeId().getPeerId());
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(10, fsm.getLogs().size());
        }
    }

    @Test
    public void testAppendEntriesWhenFollowerIsInErrorState() throws Exception {
        // start five nodes
        final List<PeerId> peers = TestUtils.generatePeers(5);

        cluster = new TestCluster("unitest", this.dataPath, peers, 1000);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();
        final Node oldLeader = cluster.getLeader();
        assertNotNull(oldLeader);
        // apply something
        this.sendTestTaskAndWait(oldLeader);

        // set one follower into error state
        final List<Node> followers = cluster.getFollowers();
        assertEquals(4, followers.size());
        final Node errorNode = followers.get(0);
        final PeerId errorPeer = errorNode.getNodeId().getPeerId().copy();
        final Endpoint errorFollowerAddr = errorPeer.getEndpoint();
        LOG.info("Set follower {} into error state", errorNode);
        ((NodeImpl) errorNode).onError(new RaftException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, new Status(-1,
            "Follower has something wrong.")));

        // increase term  by stopping leader and electing a new leader again
        final Endpoint oldLeaderAddr = oldLeader.getNodeId().getPeerId().getEndpoint().copy();
        assertTrue(cluster.stop(oldLeaderAddr));
        cluster.waitLeader();
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        LOG.info("Elect a new leader {}", leader);
        // apply something again
        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);

        // stop error follower
        Thread.sleep(20);
        LOG.info("Stop error follower {}", errorNode);
        assertTrue(cluster.stop(errorFollowerAddr));
        // restart error and old leader
        LOG.info("Restart error follower {} and old leader {}", errorFollowerAddr, oldLeaderAddr);

        assertTrue(cluster.start(errorFollowerAddr));
        assertTrue(cluster.start(oldLeaderAddr));
        cluster.ensureSame();
        assertEquals(5, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }
    }

    @Test
    public void testFollowerStartStopFollowing() throws Exception {
        // start five nodes
        final List<PeerId> peers = TestUtils.generatePeers(5);

        cluster = new TestCluster("unitest", this.dataPath, peers, 1000);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }
        cluster.waitLeader();
        final Node firstLeader = cluster.getLeader();
        assertNotNull(firstLeader);
        // apply something
        this.sendTestTaskAndWait(firstLeader);

        // assert follow times
        final List<Node> firstFollowers = cluster.getFollowers();
        assertEquals(4, firstFollowers.size());
        for (final Node node : firstFollowers) {
            assertTrue(waitForCondition(() -> ((MockStateMachine) node.getOptions().getFsm()).getOnStartFollowingTimes() == 1, 5_000));
            assertEquals(0, ((MockStateMachine) node.getOptions().getFsm()).getOnStopFollowingTimes());
        }

        // stop leader and elect new one
        final Endpoint fstLeaderAddr = firstLeader.getNodeId().getPeerId().getEndpoint();
        assertTrue(cluster.stop(fstLeaderAddr));
        cluster.waitLeader();
        final Node secondLeader = cluster.getLeader();
        assertNotNull(secondLeader);
        this.sendTestTaskAndWait(secondLeader, 10, RaftError.SUCCESS);

        // ensure start/stop following times
        final List<Node> secondFollowers = cluster.getFollowers();
        assertEquals(3, secondFollowers.size());
        for (final Node node : secondFollowers) {
            assertEquals(2, ((MockStateMachine) node.getOptions().getFsm()).getOnStartFollowingTimes());
            assertEquals(1, ((MockStateMachine) node.getOptions().getFsm()).getOnStopFollowingTimes());
        }

        // transfer leadership to a follower
        final PeerId targetPeer = secondFollowers.get(0).getNodeId().getPeerId().copy();
        assertTrue(secondLeader.transferLeadershipTo(targetPeer).isOk());
        Thread.sleep(100);
        cluster.waitLeader();
        final Node thirdLeader = cluster.getLeader();
        Assert.assertEquals(targetPeer, thirdLeader.getNodeId().getPeerId());
        this.sendTestTaskAndWait(thirdLeader, 20, RaftError.SUCCESS);

        final List<Node> thirdFollowers = cluster.getFollowers();
        assertEquals(3, thirdFollowers.size());
        for (int i = 0; i < 3; i++) {
            if (thirdFollowers.get(i).getNodeId().getPeerId().equals(secondLeader.getNodeId().getPeerId())) {
                assertEquals(2,
                    ((MockStateMachine) thirdFollowers.get(i).getOptions().getFsm()).getOnStartFollowingTimes());
                assertEquals(1,
                    ((MockStateMachine) thirdFollowers.get(i).getOptions().getFsm()).getOnStopFollowingTimes());
                continue;
            }
            assertEquals(3, ((MockStateMachine) thirdFollowers.get(i).getOptions().getFsm()).getOnStartFollowingTimes());
            assertEquals(2, ((MockStateMachine) thirdFollowers.get(i).getOptions().getFsm()).getOnStopFollowingTimes());
        }

        cluster.ensureSame();
    }

    @Test
    public void readCommittedUserLog() throws Exception {
        // setup cluster
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster("unitest", this.dataPath, peers, 1000);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }
        cluster.waitLeader();

        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        this.sendTestTaskAndWait(leader);

        // index == 1 is a CONFIGURATION log, so real_index will be 2 when returned.
        UserLog userLog = leader.readCommittedUserLog(1);
        assertNotNull(userLog);
        assertEquals(2, userLog.getIndex());
        assertEquals("hello0", new String(userLog.getData().array()));

        // index == 5 is a DATA log(a user log)
        userLog = leader.readCommittedUserLog(5);
        assertNotNull(userLog);
        assertEquals(5, userLog.getIndex());
        assertEquals("hello3", new String(userLog.getData().array()));

        // index == 15 is greater than last_committed_index
        try {
            assertNull(leader.readCommittedUserLog(15));
            fail();
        }
        catch (final LogIndexOutOfBoundsException e) {
            assertEquals(e.getMessage(), "Request index 15 is greater than lastAppliedIndex: 11");
        }

        // index == 0 invalid request
        try {
            assertNull(leader.readCommittedUserLog(0));
            fail();
        }
        catch (final LogIndexOutOfBoundsException e) {
            assertEquals(e.getMessage(), "Request index is invalid: 0");
        }
        LOG.info("Trigger leader snapshot");
        CountDownLatch latch = new CountDownLatch(1);
        leader.snapshot(new ExpectClosure(latch));
        waitLatch(latch);

        // remove and add a peer to add two CONFIGURATION logs
        final List<Node> followers = cluster.getFollowers();
        assertEquals(2, followers.size());
        final Node testFollower = followers.get(0);
        latch = new CountDownLatch(1);
        leader.removePeer(testFollower.getNodeId().getPeerId(), new ExpectClosure(latch));
        waitLatch(latch);
        latch = new CountDownLatch(1);
        leader.addPeer(testFollower.getNodeId().getPeerId(), new ExpectClosure(latch));
        waitLatch(latch);

        this.sendTestTaskAndWait(leader, 10, RaftError.SUCCESS);

        // trigger leader snapshot for the second time, after this the log of index 1~11 will be deleted.
        LOG.info("Trigger leader snapshot");
        latch = new CountDownLatch(1);
        leader.snapshot(new ExpectClosure(latch));
        waitLatch(latch);
        Thread.sleep(100);

        // index == 5 log has been deleted in log_storage.
        try {
            leader.readCommittedUserLog(5);
            fail();
        }
        catch (final LogNotFoundException e) {
            assertEquals("User log is deleted at index: 5", e.getMessage());
        }

        // index == 12、index == 13、index=14、index=15 are 4 CONFIGURATION logs(joint consensus), so real_index will be 16 when returned.
        userLog = leader.readCommittedUserLog(12);
        assertNotNull(userLog);
        assertEquals(16, userLog.getIndex());
        assertEquals("hello10", new String(userLog.getData().array()));

        // now index == 17 is a user log
        userLog = leader.readCommittedUserLog(17);
        assertNotNull(userLog);
        assertEquals(17, userLog.getIndex());
        assertEquals("hello11", new String(userLog.getData().array()));

        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
            for (int i = 0; i < 20; i++) {
                assertEquals("hello" + i, new String(fsm.getLogs().get(i).array()));
            }
        }
    }

    @Test
    public void testBootStrapWithSnapshot() throws Exception {
        final Endpoint addr = JRaftUtils.getEndPoint("127.0.0.1:5006");
        final MockStateMachine fsm = new MockStateMachine(addr);

        for (char ch = 'a'; ch <= 'z'; ch++) {
            fsm.getLogs().add(ByteBuffer.wrap(new byte[] {(byte) ch}));
        }

        final BootstrapOptions opts = new BootstrapOptions();
        opts.setServiceFactory(new DefaultJRaftServiceFactory());
        opts.setLastLogIndex(fsm.getLogs().size());
        opts.setRaftMetaUri(this.dataPath + File.separator + "meta");
        opts.setLogUri(this.dataPath + File.separator + "log");
        opts.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        opts.setGroupConf(JRaftUtils.getConfiguration("127.0.0.1:5006"));
        opts.setFsm(fsm);

        final NodeOptions nodeOpts = createNodeOptions();
        opts.setNodeOptions(nodeOpts);

        assertTrue(JRaftUtils.bootstrap(opts));

        nodeOpts.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOpts.setLogUri(this.dataPath + File.separator + "log");
        nodeOpts.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOpts.setFsm(fsm);

        RaftGroupService service = createService("test", new PeerId(addr, 0), nodeOpts);
        try {
            Node node = service.start(true);
            assertEquals(26, fsm.getLogs().size());

            for (int i = 0; i < 26; i++) {
                assertEquals('a' + i, fsm.getLogs().get(i).get());
            }

            // Group configuration will be restored from snapshot meta.
            while (!node.isLeader()) {
                Thread.sleep(20);
            }
            this.sendTestTaskAndWait(node);
            assertEquals(36, fsm.getLogs().size());
        }
        finally {
            service.shutdown();
        }
    }

    @Test
    public void testBootStrapWithoutSnapshot() throws Exception {
        final Endpoint addr = JRaftUtils.getEndPoint("127.0.0.1:5006");
        final MockStateMachine fsm = new MockStateMachine(addr);

        final BootstrapOptions opts = new BootstrapOptions();
        opts.setServiceFactory(new DefaultJRaftServiceFactory());
        opts.setLastLogIndex(0);
        opts.setRaftMetaUri(this.dataPath + File.separator + "meta");
        opts.setLogUri(this.dataPath + File.separator + "log");
        opts.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        opts.setGroupConf(JRaftUtils.getConfiguration("127.0.0.1:5006"));
        opts.setFsm(fsm);
        final NodeOptions nodeOpts = createNodeOptions();
        opts.setNodeOptions(nodeOpts);

        assertTrue(JRaftUtils.bootstrap(opts));

        nodeOpts.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOpts.setLogUri(this.dataPath + File.separator + "log");
        nodeOpts.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOpts.setFsm(fsm);

        RaftGroupService service = createService("test", new PeerId(addr, 0), nodeOpts);
        try {
            Node node = service.start(true);
            while (!node.isLeader()) {
                Thread.sleep(20);
            }
            this.sendTestTaskAndWait(node);
            assertEquals(10, fsm.getLogs().size());
        }
        finally {
            service.shutdown();
        }
    }

    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-14852")
    public void testChangePeers() throws Exception {
        final PeerId peer0 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        cluster = new TestCluster("testChangePeers", this.dataPath, Collections.singletonList(peer0));
        assertTrue(cluster.start(peer0.getEndpoint()));

        cluster.waitLeader();
        Node leader = cluster.getLeader();
        this.sendTestTaskAndWait(leader);

        for (int i = 1; i < 10; i++) {
            final PeerId peer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + i);
            assertTrue(cluster.start(peer.getEndpoint(), false, 300));
        }
        for (int i = 0; i < 9; i++) {
            cluster.waitLeader();
            leader = cluster.getLeader();
            assertNotNull(leader);
            PeerId peer = new PeerId(TestUtils.getMyIp(), peer0.getEndpoint().getPort() + i);
            Assert.assertEquals(peer, leader.getNodeId().getPeerId());
            peer = new PeerId(TestUtils.getMyIp(), peer0.getEndpoint().getPort() + i + 1);
            final SynchronizedClosure done = new SynchronizedClosure();
            leader.changePeers(new Configuration(Collections.singletonList(peer)), done);
            Status status = done.await();
            assertTrue(status.getRaftError().toString(), status.isOk());
        }
        cluster.ensureSame();
    }

    @Test
    public void testChangePeersAddMultiNodes() throws Exception {
        final PeerId peer0 = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT);
        cluster = new TestCluster("testChangePeers", this.dataPath, Collections.singletonList(peer0));
        assertTrue(cluster.start(peer0.getEndpoint()));

        cluster.waitLeader();
        final Node leader = cluster.getLeader();
        this.sendTestTaskAndWait(leader);

        final Configuration conf = new Configuration();
        for (int i = 0; i < 3; i++) {
            final PeerId peer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + i);
            conf.addPeer(peer);
        }

        PeerId peer = new PeerId(TestUtils.getMyIp(), peer0.getEndpoint().getPort() + 1);
        // fail, because the peers are not started.
        final SynchronizedClosure done = new SynchronizedClosure();
        leader.changePeers(new Configuration(Collections.singletonList(peer)), done);
        Assert.assertEquals(RaftError.ECATCHUP, done.await().getRaftError());

        // start peer1
        assertTrue(cluster.start(peer.getEndpoint()));
        // still fail, because peer2 is not started
        done.reset();
        leader.changePeers(conf, done);
        Assert.assertEquals(RaftError.ECATCHUP, done.await().getRaftError());
        // start peer2
        peer = new PeerId(TestUtils.getMyIp(), peer0.getEndpoint().getPort() + 2);
        assertTrue(cluster.start(peer.getEndpoint()));
        done.reset();
        // works
        leader.changePeers(conf, done);
        assertTrue(done.await().isOk());

        cluster.ensureSame();
        assertEquals(3, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(10, fsm.getLogs().size());
        }
    }

    @Test
    public void testChangePeersStepsDownInJointConsensus() throws Exception {
        final List<PeerId> peers = new ArrayList<>();

        final PeerId peer0 = JRaftUtils.getPeerId(TestUtils.getMyIp() + ":5006");
        final PeerId peer1 = JRaftUtils.getPeerId(TestUtils.getMyIp() + ":5007");
        final PeerId peer2 = JRaftUtils.getPeerId(TestUtils.getMyIp() + ":5008");
        final PeerId peer3 = JRaftUtils.getPeerId(TestUtils.getMyIp() + ":5009");

        // start single cluster
        peers.add(peer0);
        cluster = new TestCluster("testChangePeersStepsDownInJointConsensus", this.dataPath, peers);
        assertTrue(cluster.start(peer0.getEndpoint()));

        cluster.waitLeader();
        Node leader = cluster.getLeader();
        assertNotNull(leader);
        this.sendTestTaskAndWait(leader);

        // start peer1-3
        assertTrue(cluster.start(peer1.getEndpoint()));
        assertTrue(cluster.start(peer2.getEndpoint()));
        assertTrue(cluster.start(peer3.getEndpoint()));

        // Make sure the topology is ready before adding peers.
        assertTrue(waitForTopology(cluster, leader.getNodeId().getPeerId().getEndpoint(), 4, 3_000));

        final Configuration conf = new Configuration();
        conf.addPeer(peer0);
        conf.addPeer(peer1);
        conf.addPeer(peer2);
        conf.addPeer(peer3);

        // change peers
        final SynchronizedClosure done = new SynchronizedClosure();
        leader.changePeers(conf, done);
        assertTrue(done.await().isOk());

        // stop peer3
        assertTrue(cluster.stop(peer3.getEndpoint()));

        conf.removePeer(peer0);
        conf.removePeer(peer1);

        // Change peers to [peer2, peer3], which must fail since peer3 is stopped
        done.reset();
        leader.changePeers(conf, done);
        Assert.assertEquals(RaftError.EPERM, done.await().getRaftError());
        LOG.info(done.getStatus().toString());

        assertFalse(((NodeImpl) leader).getConf().isStable());

        leader = cluster.getLeader();
        assertNull(leader);

        assertTrue(cluster.start(peer3.getEndpoint()));
        Thread.sleep(1000);
        cluster.waitLeader();
        leader = cluster.getLeader();
        final List<PeerId> thePeers = leader.listPeers();
        assertTrue(!thePeers.isEmpty());
        assertEquals(conf.getPeerSet(), new HashSet<>(thePeers));
    }

    static class ChangeArg {
        TestCluster c;
        List<PeerId> peers;
        volatile boolean stop;
        boolean dontRemoveFirstPeer;

        ChangeArg(final TestCluster c, final List<PeerId> peers, final boolean stop,
            final boolean dontRemoveFirstPeer) {
            super();
            this.c = c;
            this.peers = peers;
            this.stop = stop;
            this.dontRemoveFirstPeer = dontRemoveFirstPeer;
        }

    }

    private Future<?> startChangePeersThread(final ChangeArg arg) {
        final Set<RaftError> expectedErrors = new HashSet<>();
        expectedErrors.add(RaftError.EBUSY);
        expectedErrors.add(RaftError.EPERM);
        expectedErrors.add(RaftError.ECATCHUP);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        return Utils.runInThread(executor, () -> {
            try {
                while (!arg.stop) {
                    arg.c.waitLeader();
                    final Node leader = arg.c.getLeader();
                    if (leader == null) {
                        continue;
                    }
                    // select peers in random
                    final Configuration conf = new Configuration();
                    if (arg.dontRemoveFirstPeer) {
                        conf.addPeer(arg.peers.get(0));
                    }
                    for (int i = 0; i < arg.peers.size(); i++) {
                        final boolean select = ThreadLocalRandom.current().nextInt(64) < 32;
                        if (select && !conf.contains(arg.peers.get(i))) {
                            conf.addPeer(arg.peers.get(i));
                        }
                    }
                    if (conf.isEmpty()) {
                        LOG.warn("No peer has been selected");
                        continue;
                    }
                    final SynchronizedClosure done = new SynchronizedClosure();
                    leader.changePeers(conf, done);
                    done.await();
                    assertTrue(done.getStatus().toString(),
                        done.getStatus().isOk() || expectedErrors.contains(done.getStatus().getRaftError()));
                }
            }
            catch (final InterruptedException e) {
                LOG.error("ChangePeersThread is interrupted", e);
            }
        });
    }

    @Test
    public void testChangePeersChaosWithSnapshot() throws Exception {
        // start cluster
        final List<PeerId> peers = new ArrayList<>();
        peers.add(new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT));
        cluster = new TestCluster("unittest", this.dataPath, peers, 1000);
        assertTrue(cluster.start(peers.get(0).getEndpoint(), false, 2));
        // start other peers
        for (int i = 1; i < 10; i++) {
            final PeerId peer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + i);
            peers.add(peer);
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        final ChangeArg arg = new ChangeArg(cluster, peers, false, false);

        final Future<?> future = startChangePeersThread(arg);
        for (int i = 0; i < 5000; ) {
            cluster.waitLeader();
            final Node leader = cluster.getLeader();
            if (leader == null) {
                continue;
            }
            final SynchronizedClosure done = new SynchronizedClosure();
            final Task task = new Task(ByteBuffer.wrap(("hello" + i).getBytes()), done);
            leader.apply(task);
            final Status status = done.await();
            if (status.isOk()) {
                if (++i % 100 == 0) {
                    System.out.println("Progress:" + i);
                }
            }
            else {
                assertEquals(RaftError.EPERM, status.getRaftError());
            }
        }
        arg.stop = true;
        future.get();
        cluster.waitLeader();
        final SynchronizedClosure done = new SynchronizedClosure();
        final Node leader = cluster.getLeader();
        leader.changePeers(new Configuration(peers), done);
        final Status st = done.await();
        assertTrue(st.getErrorMsg(), st.isOk());
        cluster.ensureSame();
        assertEquals(10, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertTrue(fsm.getLogs().size() >= 5000);
        }
    }

    @Test
    public void testChangePeersChaosWithoutSnapshot() throws Exception {
        // start cluster
        final List<PeerId> peers = new ArrayList<>();
        peers.add(new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT));
        cluster = new TestCluster("unittest", this.dataPath, peers, 1000);
        assertTrue(cluster.start(peers.get(0).getEndpoint(), false, 100000));
        // start other peers
        for (int i = 1; i < 10; i++) {
            final PeerId peer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + i);
            peers.add(peer);
            assertTrue(cluster.start(peer.getEndpoint(), false, 10000));
        }

        final ChangeArg arg = new ChangeArg(cluster, peers, false, true);

        final Future<?> future = startChangePeersThread(arg);
        final int tasks = 5000;
        for (int i = 0; i < tasks; ) {
            cluster.waitLeader();
            final Node leader = cluster.getLeader();
            if (leader == null) {
                continue;
            }
            final SynchronizedClosure done = new SynchronizedClosure();
            final Task task = new Task(ByteBuffer.wrap(("hello" + i).getBytes()), done);
            leader.apply(task);
            final Status status = done.await();
            if (status.isOk()) {
                if (++i % 100 == 0) {
                    System.out.println("Progress:" + i);
                }
            }
            else {
                assertEquals(RaftError.EPERM, status.getRaftError());
            }
        }
        arg.stop = true;
        future.get();
        cluster.waitLeader();
        final SynchronizedClosure done = new SynchronizedClosure();
        final Node leader = cluster.getLeader();
        leader.changePeers(new Configuration(peers), done);
        assertTrue(done.await().isOk());
        cluster.ensureSame();
        assertEquals(10, cluster.getFsms().size());
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertTrue(fsm.getLogs().size() >= tasks);
            assertTrue(fsm.getLogs().size() - tasks < 100);
        }
    }

    @Test
    public void testChangePeersChaosApplyTasks() throws Exception {
        // start cluster
        final List<PeerId> peers = new ArrayList<>();
        peers.add(new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT));
        cluster = new TestCluster("unittest", this.dataPath, peers, 1000);
        assertTrue(cluster.start(peers.get(0).getEndpoint(), false, 100000));
        // start other peers
        for (int i = 1; i < 10; i++) {
            final PeerId peer = new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + i);
            peers.add(peer);
            assertTrue(cluster.start(peer.getEndpoint(), false, 100000));
        }

        final int threads = 3;
        final List<ChangeArg> args = new ArrayList<>();
        final List<Future<?>> futures = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(threads);

        Executor executor = Executors.newFixedThreadPool(threads);

        for (int t = 0; t < threads; t++) {
            final ChangeArg arg = new ChangeArg(cluster, peers, false, true);
            args.add(arg);
            futures.add(startChangePeersThread(arg));

            Utils.runInThread(executor, () -> {
                try {
                    for (int i = 0; i < 5000; ) {
                        cluster.waitLeader();
                        final Node leader = cluster.getLeader();
                        if (leader == null) {
                            continue;
                        }
                        final SynchronizedClosure done = new SynchronizedClosure();
                        final Task task = new Task(ByteBuffer.wrap(("hello" + i).getBytes()), done);
                        leader.apply(task);
                        final Status status = done.await();
                        if (status.isOk()) {
                            if (++i % 100 == 0) {
                                System.out.println("Progress:" + i);
                            }
                        }
                        else {
                            assertEquals(RaftError.EPERM, status.getRaftError());
                        }
                    }
                }
                catch (final Exception e) {
                    LOG.error("Failed to run tasks", e);
                }
                finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        for (final ChangeArg arg : args) {
            arg.stop = true;
        }
        for (final Future<?> future : futures) {
            future.get();
        }

        cluster.waitLeader();
        final SynchronizedClosure done = new SynchronizedClosure();
        final Node leader = cluster.getLeader();
        leader.changePeers(new Configuration(peers), done);
        assertTrue(done.await().isOk());
        cluster.ensureSame();
        assertEquals(10, cluster.getFsms().size());

        for (final MockStateMachine fsm : cluster.getFsms()) {
            final int logSize = fsm.getLogs().size();
            assertTrue("logSize= " + logSize, logSize >= 5000 * threads);
            assertTrue("logSize= " + logSize, logSize - 5000 * threads < 100);
        }
    }

    @Test
    public void testBlockedElection() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        cluster = new TestCluster("unittest", this.dataPath, peers);

        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }

        cluster.waitLeader();

        Node leader = cluster.getLeader();

        LOG.warn("Current leader {}, electTimeout={}", leader.getNodeId().getPeerId(), ((NodeImpl) leader).getOptions().getElectionTimeoutMs());

        List<Node> followers = cluster.getFollowers();

        for (Node follower : followers) {
            NodeImpl follower0 = (NodeImpl) follower;
            DefaultRaftClientService rpcService = (DefaultRaftClientService) follower0.getRpcClientService();
            RpcClientEx rpcClientEx = (RpcClientEx) rpcService.getRpcClient();
            rpcClientEx.blockMessages(new BiPredicate<Object, String>() {
                @Override public boolean test(Object msg, String nodeId) {
                    if (msg instanceof RpcRequests.RequestVoteRequest) {
                        RpcRequests.RequestVoteRequest msg0 = (RpcRequests.RequestVoteRequest) msg;

                        return !msg0.getPreVote();
                    }

                    return false;
                }
            });
        }

        LOG.warn("Stop leader {}, curTerm={}", leader.getNodeId().getPeerId(), ((NodeImpl) leader).getCurrentTerm());

        assertTrue(cluster.stop(leader.getNodeId().getPeerId().getEndpoint()));

        assertNull(cluster.getLeader());

        Thread.sleep(3000);

        assertNull(cluster.getLeader());

        for (Node follower : followers) {
            NodeImpl follower0 = (NodeImpl) follower;
            DefaultRaftClientService rpcService = (DefaultRaftClientService) follower0.getRpcClientService();
            RpcClientEx rpcClientEx = (RpcClientEx) rpcService.getRpcClient();
            rpcClientEx.stopBlock();
        }

        // elect new leader
        cluster.waitLeader();
        leader = cluster.getLeader();
        LOG.info("Elect new leader is {}, curTerm={}", leader.getLeaderId(), ((NodeImpl) leader).getCurrentTerm());
    }

    private NodeOptions createNodeOptions() {
        final NodeOptions options = new NodeOptions();

        options.setCommonExecutor(JRaftUtils.createCommonExecutor(options));
        options.setStripedExecutor(JRaftUtils.createAppendEntriesExecutor(options));

        return options;
    }

    /**
     * {@inheritDoc}
     */
    private boolean waitForTopology(TestCluster cluster, Endpoint addr, int expected, long timeout) {
        RaftGroupService grp = cluster.getServer(addr);

        if (grp == null) {
            LOG.warn("Node has not been found {}", addr);

            return false;
        }

        RpcServer rpcServer = grp.getRpcServer();

        if (!(rpcServer instanceof IgniteRpcServer))
            return true;

        ClusterService service = ((IgniteRpcServer) grp.getRpcServer()).clusterService();

        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().size() >= expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    private boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param groupId Group id.
     * @param peerId Peer id.
     * @param nodeOptions Node options.
     * @return Raft group service.
     */
    private RaftGroupService createService(String groupId, PeerId peerId, NodeOptions nodeOptions) {
        NodeManager nodeManager = new NodeManager();

        List<String> servers = new ArrayList<>();

        Configuration initialConf = nodeOptions.getInitialConf();

        if (initialConf != null) {
            for (PeerId id : initialConf.getPeers()) {
                servers.add(id.getEndpoint().toString());
            }

            for (PeerId id : initialConf.getLearners()) {
                servers.add(id.getEndpoint().toString());
            }
        }

        final IgniteRpcServer rpcServer = new TestIgniteRpcServer(peerId.getEndpoint(), servers, nodeManager);
        nodeOptions.setRpcClient(new IgniteRpcClient(rpcServer.clusterService(), true));

        return new RaftGroupService(groupId, peerId, nodeOptions, rpcServer, nodeManager);
    }
}
