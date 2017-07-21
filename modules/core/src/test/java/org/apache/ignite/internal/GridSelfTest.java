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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.messaging.MessagingListenActor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test for {@link org.apache.ignite.IgniteCluster}.
 */
@GridCommonTest(group = "Kernal Self")
public class GridSelfTest extends ClusterGroupAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT > 2;

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected ClusterGroup projection() {
        return grid(0).cluster();
    }

    /** {@inheritDoc} */
    @Override protected UUID localNodeId() {
        return grid(0).localNode().id();
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> remoteNodeIds() {
        return F.nodeIds(grid(0).cluster().forRemotes().nodes());
    }

    /** {@inheritDoc} */
    @Override public void testRemoteNodes() throws Exception {
        int size = remoteNodeIds().size();

        String name = "oneMoreGrid";

        try {
            Ignite g = startGrid(name);

            UUID joinedId = g.cluster().localNode().id();

            assert projection().forRemotes().nodes().size() == size + 1;

            assert F.nodeIds(projection().forRemotes().nodes()).contains(joinedId);
        }
        finally {
            stopGrid(name);
        }
    }

    /** {@inheritDoc} */
    @Override public void testRemoteProjection() throws Exception {
        ClusterGroup remotePrj = projection().forRemotes();

        int size = remotePrj.nodes().size();

        String name = "oneMoreGrid";

        try {
            Ignite g = startGrid(name);

            UUID joinedId = g.cluster().localNode().id();

            assert remotePrj.nodes().size() == size + 1;

            assert F.nodeIds(remotePrj.nodes()).contains(joinedId);
        }
        finally {
            stopGrid(name);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testAsyncListen() throws Exception {
        final String hello = "HELLO!";

        final String bye = "BYE!";

        final Ignite g = grid(0);

        final UUID locNodeId = g.cluster().localNode().id();

        g.message().remoteListen(null, new MessagingListenActor<String>() {
            @Override protected void receive(UUID nodeId, String rcvMsg) throws Throwable {
                if (hello.equals(rcvMsg)) {
                    assertEquals(locNodeId, nodeId);
                    assertEquals(hello, rcvMsg);

                    stop(bye);
                }
            }
        });

        final AtomicInteger cnt = new AtomicInteger();

        g.message().localListen(null, new P2<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                if (msg.equals(bye))
                    cnt.incrementAndGet();

                return true;
            }
        });

        g.message().send(null, hello);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cnt.get() == g.cluster().nodes().size();
            }
        }, 5000);

        assertEquals(cnt.get(), g.cluster().nodes().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testForOthers() throws Exception {
        ClusterNode node0 = grid(0).localNode();
        ClusterNode node1 = grid(1).localNode();
        ClusterNode node2 = grid(2).localNode();
        ClusterNode node3 = grid(3).localNode();

        ClusterGroup p1 = grid(0).cluster().forOthers(node0);

        assertEquals(3, p1.nodes().size());

        assertEquals(2, p1.forOthers(node1).nodes().size());

        assertEquals(1, p1.forOthers(node1, node2).nodes().size());

        assertEquals(1, grid(0).cluster().forOthers(node1, node2, node3).nodes().size());
    }
}