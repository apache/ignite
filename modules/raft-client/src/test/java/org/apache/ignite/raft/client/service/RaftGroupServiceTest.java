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

package org.apache.ignite.raft.client.service;

import java.net.ConnectException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

/**
 * Test methods of raft group service.
 */
@ExtendWith(MockitoExtension.class)
public class RaftGroupServiceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftGroupServiceTest.class);

    /** */
    private static final List<Peer> NODES = Stream.of(20000, 20001, 20002)
        .map(port -> new NetworkAddress("localhost", port))
        .map(Peer::new)
        .collect(Collectors.toUnmodifiableList());

    /** */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    /** */
    private volatile Peer leader = NODES.get(0);

    /** Call timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Mock cluster. */
    @Mock
    private ClusterService cluster;

    /** Mock messaging service */
    @Mock
    private MessagingService messagingService;

    /**
     * @param testInfo Test info.
     */
    @BeforeEach
    void before(TestInfo testInfo) {
        when(cluster.messagingService()).thenReturn(messagingService);

        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testRefreshLeaderStable() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(leader, service.leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testRefreshLeaderNotElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        assertNull(service.leader());

        try {
            service.refreshLeader().get();

            fail("Should fail");
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testRefreshLeaderElectedAfterDelay() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override public void run() {
                leader = NODES.get(0);
            }
        }, 500);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(NODES.get(0), service.leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testRefreshLeaderWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(true);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        try {
            service.refreshLeader().get(500, TimeUnit.MILLISECONDS);

            fail();
        }
        catch (TimeoutException e) {
            // Expected.
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        service.refreshLeader().get();

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLazyInitLeader() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        assertNull(service.leader());

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(leader, service.leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(true, null);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        try {
            service.run(new TestCommand()).get(500, TimeUnit.MILLISECONDS);

            fail();
        }
        catch (TimeoutException e) {
            // Expected.
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderNotElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, true, DELAY);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        try {
            service.run(new TestCommand()).get();

            fail("Expecting timeout");
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderElectedAfterDelay() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, true, DELAY);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override public void run() {
                RaftGroupServiceTest.this.leader = NODES.get(0);
            }
        }, 500);

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(NODES.get(0), service.leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderElectedAfterDelayWithFailedNode() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false, NODES.get(0));

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT * 3, NODES, true, DELAY);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override public void run() {
                LOG.info("Set leader {}", NODES.get(1));

                RaftGroupServiceTest.this.leader = NODES.get(1);
            }
        }, 500);

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(NODES.get(1), service.leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderChanged() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, true, DELAY);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        Peer newLeader = NODES.get(1);

        this.leader = newLeader;

        assertEquals(leader, service.leader());
        assertNotEquals(leader, newLeader);

        // Runs the command on an old leader. It should respond with leader changed error, when transparently retry.
        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(newLeader, service.leader());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSnapshotExecutionException() throws Exception {
        String groupId = "test";

        mockSnapshotRequest(1);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        var addr = new NetworkAddress("localhost", 8082);

        CompletableFuture<Void> fut = service.snapshot(new Peer(addr));

        try {
            fut.get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IgniteInternalException);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSnapshotExecutionFailedResponse() throws Exception {
        String groupId = "test";

        mockSnapshotRequest(0);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        var addr = new NetworkAddress("localhost", 8082);

        CompletableFuture<Void> fut = service.snapshot(new Peer(addr));

        try {
            fut.get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    /**
     * @param delay {@code True} to create a delay before response.
     * @param peer Fail the request targeted to given peer.
     */
    private void mockUserInput(boolean delay, @Nullable Peer peer) {
        when(messagingService.invoke(
            any(NetworkAddress.class),
            argThat(new ArgumentMatcher<ActionRequest>() {
                @Override public boolean matches(ActionRequest arg) {
                    return arg.command() instanceof TestCommand;
                }
            }),
            anyLong()
        )).then(invocation -> {
            NetworkAddress target = invocation.getArgument(0);

            if (peer != null && target.equals(peer.address()))
                return failedFuture(new IgniteInternalException(new ConnectException()));

            if (delay) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    return FACTORY.actionResponse().result(new TestResponse()).build();
                });
            }

            Object resp;

            if (leader == null)
                resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build();
            else if (target != leader.address())
                resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.LEADER_CHANGED).newLeader(leader).build();
            else
                resp = FACTORY.actionResponse().result(new TestResponse()).build();

            return completedFuture(resp);
        });
    }

    /**
     * @param delay {@code True} to delay response.
     */
    private void mockLeaderRequest(boolean delay) {
        when(messagingService.invoke(any(NetworkAddress.class), any(GetLeaderRequest.class), anyLong()))
            .then(invocation -> {
                if (delay) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e) {
                            fail();
                        }

                        return FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build();
                    });
                }

                Peer leader0 = leader;

                Object resp = leader0 == null ?
                    FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build() :
                    FACTORY.getLeaderResponse().leader(leader0).build();

                return completedFuture(resp);
            });
    }

    /**
     * @param mode Mock mode.
     */
    private void mockSnapshotRequest(int mode) {
        when(messagingService.invoke(any(NetworkAddress.class), any(SnapshotRequest.class), anyLong()))
            .then(invocation -> {
                if (mode == 0) {
                    return completedFuture(FACTORY.raftErrorResponse().errorCode(RaftErrorCode.SNAPSHOT).
                        errorMessage("Failed to create a snapshot").build());
                }
                else
                    return failedFuture(new IgniteInternalException("Very bad"));
            });
    }

    /** */
    private static class TestCommand implements WriteCommand {
    }

    /** */
    private static class TestResponse {
    }
}
