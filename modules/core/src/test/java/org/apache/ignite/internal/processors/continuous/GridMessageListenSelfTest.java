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

package org.apache.ignite.internal.processors.continuous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.messaging.MessagingListenActor;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Message listen test.
 */
public class GridMessageListenSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final String INC_ATTR = "include";

    /** */
    private static final String MSG = "Message";

    /** */
    private static final String TOPIC = "Topic";

    /** */
    private static final int MSG_CNT = 3;

    /** */
    private static final String TOPIC_CLS_NAME = "org.apache.ignite.tests.p2p.GridTestMessageTopic";

    /** */
    private static final String LSNR_CLS_NAME = "org.apache.ignite.tests.p2p.GridTestMessageListener";

    /** */
    private static boolean include;

    /** */
    private static final List<UUID> allNodes = new ArrayList<>();

    /** */
    private static final List<UUID> rmtNodes = new ArrayList<>();

    /** */
    private static final List<UUID> incNodes = new ArrayList<>();

    /** */
    private static final Collection<UUID> nodes = new GridConcurrentHashSet<>();

    /** */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** */
    private static CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (include)
            cfg.setUserAttributes(F.asMap(INC_ATTR, true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        nodes.clear();
        cnt.set(0);

        include = true;

        startGridsMultiThreaded(GRID_CNT - 1);

        include = false;

        Thread.sleep(500);

        startGrid(GRID_CNT - 1);

        allNodes.clear();
        rmtNodes.clear();
        incNodes.clear();

        for (int i = 0; i < GRID_CNT; i++) {
            UUID id = grid(i).localNode().id();

            allNodes.add(id);

            if (i != 0)
                rmtNodes.add(id);

            if (i != GRID_CNT - 1)
                incNodes.add(id);
        }

        Collections.sort(allNodes);
        Collections.sort(rmtNodes);
        Collections.sort(incNodes);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullTopic() throws Exception {
        latch = new CountDownLatch(MSG_CNT * GRID_CNT);

        listen(grid(0).cluster(), null, true);

        send();

        assert latch.await(2, SECONDS);

        Thread.sleep(500);

        assertEquals(MSG_CNT * GRID_CNT, cnt.get());

        checkNodes(allNodes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonNullTopic() throws Exception {
        latch = new CountDownLatch(MSG_CNT * GRID_CNT);

        listen(grid(0).cluster(), null, true);

        send();

        assert latch.await(2, SECONDS);

        Thread.sleep(500);

        assertEquals(MSG_CNT * GRID_CNT, cnt.get());

        checkNodes(allNodes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopListen() throws Exception {
        latch = new CountDownLatch(GRID_CNT);

        listen(grid(0).cluster(), null, false);

        send();

        assert latch.await(2, SECONDS);

        Thread.sleep(500);

        int expCnt = cnt.get();

        send();

        Thread.sleep(1000);

        assertEquals(expCnt, cnt.get());

        checkNodes(allNodes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjection() throws Exception {
        latch = new CountDownLatch(MSG_CNT * (GRID_CNT - 1));

        listen(grid(0).cluster().forRemotes(), null, true);

        send();

        assert latch.await(2, SECONDS);

        Thread.sleep(500);

        assertEquals(MSG_CNT * (GRID_CNT - 1), cnt.get());

        checkNodes(rmtNodes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoin() throws Exception {
        latch = new CountDownLatch(MSG_CNT * (GRID_CNT + 1));

        listen(grid(0).cluster(), null, true);

        try {
            Ignite g = startGrid("anotherGrid");

            send();

            assert latch.await(2, SECONDS);

            Thread.sleep(500);

            assertEquals(MSG_CNT * (GRID_CNT + 1), cnt.get());

            List<UUID> allNodes0 = new ArrayList<>(allNodes);

            allNodes0.add(g.cluster().localNode().id());

            Collections.sort(allNodes0);

            checkNodes(allNodes0);
        }
        finally {
            stopGrid("anotherGrid");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinWithProjection() throws Exception {
        latch = new CountDownLatch(MSG_CNT * GRID_CNT);

        listen(grid(0).cluster().forAttribute(INC_ATTR, null), null, true);

        try {
            include = true;

            Ignite g = startGrid("anotherGrid1");

            include = false;

            startGrid("anotherGrid2");

            send();

            assert latch.await(2, SECONDS);

            Thread.sleep(500);

            assertEquals(MSG_CNT * GRID_CNT, cnt.get());

            List<UUID> incNodes0 = new ArrayList<>(incNodes);

            incNodes0.add(g.cluster().localNode().id());

            Collections.sort(incNodes0);

            checkNodes(incNodes0);
        }
        finally {
            stopGrid("anotherGrid1");
            stopGrid("anotherGrid2");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullTopicWithDeployment() throws Exception {
        Class<?> cls = getExternalClassLoader().loadClass(LSNR_CLS_NAME);

        grid(0).message().remoteListen(null, (IgniteBiPredicate<UUID, Object>)cls.newInstance());

        send();

        boolean s = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return checkDeployedListeners(GRID_CNT);
            }
        }, 2000);

        assertTrue(s);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonNullTopicWithDeployment() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class<?> topicCls = ldr.loadClass(TOPIC_CLS_NAME);
        Class<?> lsnrCls = ldr.loadClass(LSNR_CLS_NAME);

        Object topic = topicCls.newInstance();

        grid(0).message().remoteListen(topic, (IgniteBiPredicate<UUID, Object>)lsnrCls.newInstance());

        send(topic);

        boolean s = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return checkDeployedListeners(GRID_CNT);
            }
        }, 2000);

        assertTrue(s);
    }

    /**
     * @throws Exception If failed.
     */
    public void testListenActor() throws Exception {
        latch = new CountDownLatch(MSG_CNT * (GRID_CNT + 1));

        grid(0).message().remoteListen(null, new Actor(grid(0).cluster()));

        try {
            Ignite g = startGrid("anotherGrid");

            send();

            assert latch.await(2, SECONDS);

            Thread.sleep(500);

            assertEquals(MSG_CNT * (GRID_CNT + 1), cnt.get());

            List<UUID> allNodes0 = new ArrayList<>(allNodes);

            allNodes0.add(g.cluster().localNode().id());

            Collections.sort(allNodes0);

            checkNodes(allNodes0);
        }
        finally {
            stopGrid("anotherGrid");
        }
    }

    /**
     * @param prj Projection.
     * @param topic Topic.
     * @param ret Value returned from listener.
     * @throws Exception In case of error.
     */
    private void listen(final ClusterGroup prj, @Nullable Object topic, final boolean ret) throws Exception {
        assert prj != null;

        message(prj).remoteListen(topic, new Listener(prj, ret));
    }

    /**
     * @throws Exception In case of error.
     */
    private void send() throws Exception {
        send(TOPIC);
    }

    /**
     * @param topic Non-null topic.
     * @throws Exception In case of error.
     */
    private void send(Object topic) throws Exception {
        assert topic != null;

        for (int i = 0; i < MSG_CNT; i++)
            grid(0).message().send(null, MSG);

        for (int i = 0; i < MSG_CNT; i++)
            grid(0).message().send(topic, MSG);
    }

    /**
     * @param expCnt Expected messages count.
     * @return If check passed.
     */
    private boolean checkDeployedListeners(int expCnt) {
        for (Ignite g : G.allGrids()) {
            AtomicInteger cnt = g.cluster().<String, AtomicInteger>nodeLocalMap().get("msgCnt");

            if (cnt == null || cnt.get() != expCnt)
                return false;
        }

        return true;
    }

    /**
     * @param expNodes Expected nodes.
     */
    private void checkNodes(List<UUID> expNodes) {
        List<UUID> nodes0 = new ArrayList<>(nodes);

        Collections.sort(nodes0);

        assertEquals(expNodes, nodes0);
    }

    /** */
    private static class Listener implements P2<UUID, Object> {
        /** */
        private final ClusterGroup prj;

        /** */
        private final boolean ret;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param prj Projection.
         * @param ret Return value.
         */
        private Listener(ClusterGroup prj, boolean ret) {
            this.prj = prj;
            this.ret = ret;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(UUID nodeId, Object msg) {
            assertNotNull(ignite);
            assertNotNull(ignite.configuration().getNodeId());

            X.println("Received message [nodeId=" + nodeId + ", locNodeId=" + ignite.cluster().localNode().id() + ']');

            assertEquals(prj.ignite().cluster().localNode().id(), nodeId);
            assertEquals(MSG, msg);

            nodes.add(ignite.configuration().getNodeId());
            cnt.incrementAndGet();
            latch.countDown();

            return ret;
        }
    }

    /** */
    private static class Actor extends MessagingListenActor<Object> {
        /** */
        private final ClusterGroup prj;

        /**
         * @param prj Projection.
         */
        private Actor(ClusterGroup prj) {
            this.prj = prj;
        }

        /** {@inheritDoc} */
        @Override protected void receive(UUID nodeId, Object msg) throws Throwable {
            assertNotNull(ignite());

            UUID locNodeId = ignite().cluster().localNode().id();

            X.println("Received message [nodeId=" + nodeId + ", locNodeId=" + locNodeId + ']');

            assertEquals(prj.ignite().cluster().localNode().id(), nodeId);
            assertEquals(MSG, msg);

            nodes.add(locNodeId);
            cnt.incrementAndGet();
            latch.countDown();
        }
    }
}