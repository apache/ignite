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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.ReplicatorGroupOptions;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;

@RunWith(value = MockitoJUnitRunner.class)
public class ReplicatorGroupTest {

    static final Logger LOG = LoggerFactory.getLogger(ReplicatorGroupTest.class);

    private TimerManager timerManager;
    private ReplicatorGroupImpl replicatorGroup;
    @Mock
    private BallotBox ballotBox;
    @Mock
    private LogManager logManager;
    @Mock
    private NodeImpl node;
    @Mock
    private RaftClientService rpcService;
    @Mock
    private SnapshotStorage snapshotStorage;
    private final NodeOptions options = new NodeOptions();
    private final RaftOptions raftOptions = new RaftOptions();
    private final PeerId peerId1 = new PeerId("localhost", 8082);
    private final PeerId peerId2 = new PeerId("localhost", 8083);
    private final PeerId peerId3 = new PeerId("localhost", 8084);
    private final AtomicInteger errorCounter = new AtomicInteger(0);
    private final AtomicInteger stoppedCounter = new AtomicInteger(0);
    private final AtomicInteger startedCounter = new AtomicInteger(0);

    @Before
    public void setup() {
        this.timerManager = new TimerManager(5);
        this.replicatorGroup = new ReplicatorGroupImpl();
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this.node);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(this.snapshotStorage);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);
        Mockito.when(this.logManager.getLastLogIndex()).thenReturn(10L);
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("test", new PeerId("localhost", 8081)));
        mockSendEmptyEntries();
        assertTrue(this.replicatorGroup.init(this.node.getNodeId(), rgOpts));
    }

    @Test
    public void testAddReplicatorAndFailed() {
        this.replicatorGroup.resetTerm(1);
        assertFalse(this.replicatorGroup.addReplicator(this.peerId1));
        assertEquals(this.replicatorGroup.getFailureReplicators().get(this.peerId1), ReplicatorType.Follower);
    }

    @Test
    public void testAddLearnerFailure() {
        this.replicatorGroup.resetTerm(1);
        assertFalse(this.replicatorGroup.addReplicator(this.peerId1, ReplicatorType.Learner));
        assertEquals(this.replicatorGroup.getFailureReplicators().get(this.peerId1), ReplicatorType.Learner);
    }

    @Test
    public void testAddLearnerSuccess() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        assertTrue(this.replicatorGroup.addReplicator(this.peerId1, ReplicatorType.Learner));
        assertNotNull(this.replicatorGroup.getReplicatorMap().get(this.peerId1));
        assertNull(this.replicatorGroup.getFailureReplicators().get(this.peerId1));
    }

    @Test
    public void testAddReplicatorSuccess() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        assertTrue(this.replicatorGroup.addReplicator(this.peerId1));
        assertNull(this.replicatorGroup.getFailureReplicators().get(this.peerId1));
    }

    @Test
    public void testStopReplicator() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        assertTrue(this.replicatorGroup.stopReplicator(this.peerId1));
    }

    @Test
    public void testStopAllReplicator() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        assertTrue(this.replicatorGroup.contains(this.peerId1));
        assertTrue(this.replicatorGroup.contains(this.peerId2));
        assertTrue(this.replicatorGroup.contains(this.peerId3));
        assertTrue(this.replicatorGroup.stopAll());
    }

    @Test
    public void testReplicatorWithNoRepliactorStateListener() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        assertTrue(this.replicatorGroup.stopAll());
        assertEquals(0, this.startedCounter.get());
        assertEquals(0, this.errorCounter.get());
        assertEquals(0, this.stoppedCounter.get());

    }

    class UserReplicatorStateListener implements Replicator.ReplicatorStateListener {
        @Override
        public void onCreated(final PeerId peer) {
            LOG.info("Replicator has created");
            ReplicatorGroupTest.this.startedCounter.incrementAndGet();
        }

        @Override
        public void onError(final PeerId peer, final Status status) {
            LOG.info("Replicator has errors");
            ReplicatorGroupTest.this.errorCounter.incrementAndGet();
        }

        @Override
        public void onDestroyed(final PeerId peer) {
            LOG.info("Replicator has been destroyed");
            ReplicatorGroupTest.this.stoppedCounter.incrementAndGet();
        }
    }

    @Test
    public void testTransferLeadershipToAndStop() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        long logIndex = 8;
        assertTrue(this.replicatorGroup.transferLeadershipTo(this.peerId1, 8));
        final Replicator r = (Replicator) this.replicatorGroup.getReplicator(this.peerId1).lock();
        assertEquals(r.getTimeoutNowIndex(), logIndex);
        this.replicatorGroup.getReplicator(this.peerId1).unlock();
        assertTrue(this.replicatorGroup.stopTransferLeadership(this.peerId1));
        assertEquals(r.getTimeoutNowIndex(), 0);
    }

    @Test
    public void testFindTheNextCandidateWithPriority1() {
        final PeerId p1 = new PeerId("localhost", 18881, 0, 60);
        final PeerId p2 = new PeerId("localhost", 18882, 0, 80);
        final PeerId p3 = new PeerId("localhost", 18883, 0, 100);
        Mockito.when(this.rpcService.connect(p1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(p2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(p3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(p1);
        this.replicatorGroup.addReplicator(p2);
        this.replicatorGroup.addReplicator(p3);
        final ConfigurationEntry conf = new ConfigurationEntry();
        conf.setConf(new Configuration(Arrays.asList(p1, p2, p3)));
        final PeerId p = this.replicatorGroup.findTheNextCandidate(conf);
        assertEquals(p3, p);
    }

    @Test
    public void testFindTheNextCandidateWithPriority2() {
        final PeerId p1 = new PeerId("localhost", 18881, 0, 0);
        final PeerId p2 = new PeerId("localhost", 18882, 0, 0);
        final PeerId p3 = new PeerId("localhost", 18883, 0, -1);
        Mockito.when(this.rpcService.connect(p1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(p2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(p3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(p1);
        this.replicatorGroup.addReplicator(p2);
        this.replicatorGroup.addReplicator(p3);
        final ConfigurationEntry conf = new ConfigurationEntry();
        conf.setConf(new Configuration(Arrays.asList(p1, p2, p3)));
        final PeerId p = this.replicatorGroup.findTheNextCandidate(conf);
        assertEquals(p3, p);
    }

    @After
    public void teardown() {
        this.timerManager.shutdown();
        this.errorCounter.set(0);
        this.stoppedCounter.set(0);
        this.startedCounter.set(0);
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private void mockSendEmptyEntries() {
        final RpcRequests.AppendEntriesRequest request1 = createEmptyEntriesRequestToPeer(this.peerId1);
        final RpcRequests.AppendEntriesRequest request2 = createEmptyEntriesRequestToPeer(this.peerId2);
        final RpcRequests.AppendEntriesRequest request3 = createEmptyEntriesRequestToPeer(this.peerId3);

        Mockito
            .when(this.rpcService.appendEntries(eq(this.peerId1.getEndpoint()), eq(request1), eq(-1), Mockito.any()))
            .thenAnswer(new Answer<Object>() {
                @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                    return new CompletableFuture<>();
                }
            });
        Mockito
            .when(this.rpcService.appendEntries(eq(this.peerId2.getEndpoint()), eq(request2), eq(-1), Mockito.any()))
            .thenReturn(new CompletableFuture<>());
        Mockito
            .when(this.rpcService.appendEntries(eq(this.peerId3.getEndpoint()), eq(request3), eq(-1), Mockito.any()))
            .thenReturn(new CompletableFuture<>());
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequestToPeer(final PeerId peerId) {
        return RpcRequests.AppendEntriesRequest.newBuilder() //
            .setGroupId("test") //
            .setServerId(new PeerId("localhost", 8081).toString()) //
            .setPeerId(peerId.toString()) //
            .setTerm(1) //
            .setPrevLogIndex(10) //
            .setPrevLogTerm(1) //
            .setCommittedIndex(0) //
            .setData(ByteString.EMPTY) //
            .build();
    }
}
