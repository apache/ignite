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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.List.of;
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
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftGroupServiceTest.class);

    /** */
    private static final List<Peer> NODES = of(
        new Peer(new ClusterNode("node1", "foobar", 123)),
        new Peer(new ClusterNode("node2", "foobar", 123)),
        new Peer(new ClusterNode("node3", "foobar", 123))
    );

    /** */
    private static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

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

        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());
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
            service.refreshLeader().get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false);

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
        mockUserInput(false);

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
        mockUserInput(true);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY);

        try {
            service.run(new TestCommand()).get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserRequestLeaderNotElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false);

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
        mockUserInput(false);

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
    public void testUserRequestLeaderChanged() throws Exception {
        String groupId = "test";

        mockLeaderRequest(false);
        mockUserInput(false);

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
     * @param simulateTimeout {@code True} to simulate request timeout.
     */
    private void mockUserInput(boolean simulateTimeout) {
        Mockito.doAnswer(invocation -> {
            ClusterNode target = invocation.getArgument(0);

            if (simulateTimeout)
                return failedFuture(new TimeoutException());

            Object resp;

            if (leader == null)
                resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build();
            else if (target != leader.getNode())
                resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.LEADER_CHANGED).newLeader(leader).build();
            else
                resp = FACTORY.actionResponse().result(new TestResponse()).build();

            return completedFuture(resp);
        })
            .when(messagingService)
            .invoke(
                any(),
                argThat(new ArgumentMatcher<ActionRequest>() {
                    @Override public boolean matches(ActionRequest arg) {
                        return arg.command() instanceof TestCommand;
                    }
                }),
                anyLong()
            );
    }

    /**
     * @param simulateTimeout {@code True} to simulate request timeout.
     */
    private void mockLeaderRequest(boolean simulateTimeout) {
        Mockito.doAnswer(invocation -> {
            if (simulateTimeout)
                return failedFuture(new TimeoutException());

            Object resp;

            Peer leader0 = leader;

            if (leader0 == null) {
                resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build();
            }
            else {
                resp = FACTORY.getLeaderResponse().leader(leader0).build();
            }

            return completedFuture(resp);
        })
            .when(messagingService)
            .invoke(any(), any(GetLeaderRequest.class), anyLong());
    }

    /** */
    private static class TestCommand implements WriteCommand {
    }

    /** */
    private static class TestResponse {
    }
}
