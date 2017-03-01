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

package org.apache.ignite.internal.managers.communication;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link GridIoManager}.
 */
public class GridIoManagerSelfTest extends GridCommonAbstractTest {
    /** Grid test context. */
    private GridTestKernalContext ctx;

    /** Test local node. */
    private GridTestNode locNode = new GridTestNode(UUID.randomUUID());

    /** Test remote node. */
    private GridTestNode rmtNode = new GridTestNode(UUID.randomUUID());

    /**
     * @throws IgniteCheckedException In case of error.
     */
    public GridIoManagerSelfTest() throws IgniteCheckedException {
        ctx = new GridTestKernalContext(log);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx.config().setCommunicationSpi(new TcpCommunicationSpi());
        ctx.config().setMarshaller(new JdkMarshaller());

        // Turn off peer class loading to simplify mocking.
        ctx.config().setPeerClassLoadingEnabled(false);

        // Register local and remote nodes in discovery manager.
        GridDiscoveryManager mockedDiscoveryMgr = Mockito.mock(GridDiscoveryManager.class);

        when(mockedDiscoveryMgr.localNode()).thenReturn(locNode);
        when(mockedDiscoveryMgr.remoteNodes()).thenReturn(F.<ClusterNode>asList(rmtNode));

        ctx.add(mockedDiscoveryMgr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendIfOneOfNodesIsLocalAndTopicIsEnum() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                new GridIoManager(ctx).send(F.asList(locNode, rmtNode), GridTopic.TOPIC_CACHE, new TestMessage(),
                    GridIoPolicy.P2P_POOL);

                return null;
            }
        }, AssertionError.class, "Internal Ignite code should never call the method with local node in a node list.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendIfOneOfNodesIsLocalAndTopicIsObject() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                new GridIoManager(ctx).send(F.asList(locNode, rmtNode), new Object(), new TestMessage(),
                    GridIoPolicy.P2P_POOL);

                return null;
            }
        }, AssertionError.class, "Internal Ignite code should never call the method with local node in a node list.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendUserMessageThinVersionIfOneOfNodesIsLocal() throws Exception {
        Object msg = new Object();

        GridIoManager ioMgr = spy(new TestGridIoManager(ctx));

        try {
            ioMgr.sendUserMessage(F.asList(locNode, rmtNode), msg);
        }
        catch (IgniteCheckedException ignored) {
            // No-op. We are using mocks so real sending is impossible.
        }

        verify(ioMgr).send(eq(locNode), eq(GridTopic.TOPIC_COMM_USER), any(GridIoUserMessage.class),
            eq(GridIoPolicy.PUBLIC_POOL));

        Collection<? extends ClusterNode> rmtNodes = F.view(F.asList(rmtNode), F.remoteNodes(locNode.id()));

        verify(ioMgr).send(argThat(new IsEqualCollection(rmtNodes)), eq(GridTopic.TOPIC_COMM_USER),
            any(GridIoUserMessage.class), eq(GridIoPolicy.PUBLIC_POOL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendUserMessageUnorderedThickVersionIfOneOfNodesIsLocal() throws Exception {
        Object msg = new Object();

        GridIoManager ioMgr = spy(new TestGridIoManager(ctx));

        try {
            ioMgr.sendUserMessage(F.asList(locNode, rmtNode), msg, GridTopic.TOPIC_IGFS, false, 123L, false);
        }
        catch (IgniteCheckedException ignored) {
            // No-op. We are using mocks so real sending is impossible.
        }

        verify(ioMgr).send(eq(locNode), eq(GridTopic.TOPIC_COMM_USER), any(GridIoUserMessage.class),
            eq(GridIoPolicy.PUBLIC_POOL));

        Collection<? extends ClusterNode> rmtNodes = F.view(F.asList(rmtNode), F.remoteNodes(locNode.id()));

        verify(ioMgr).send(argThat(new IsEqualCollection(rmtNodes)), eq(GridTopic.TOPIC_COMM_USER),
            any(GridIoUserMessage.class), eq(GridIoPolicy.PUBLIC_POOL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendUserMessageOrderedThickVersionIfOneOfNodesIsLocal() throws Exception {
        Object msg = new Object();

        GridIoManager ioMgr = spy(new TestGridIoManager(ctx));

        try {
            ioMgr.sendUserMessage(F.asList(locNode, rmtNode), msg, GridTopic.TOPIC_IGFS, true, 123L, false);
        }
        catch (Exception ignored) {
            // No-op. We are using mocks so real sending is impossible.
        }

        verify(ioMgr).sendOrderedMessage(
            argThat(new IsEqualCollection(F.asList(locNode, rmtNode))),
            eq(GridTopic.TOPIC_COMM_USER),
            any(GridIoUserMessage.class),
            eq(GridIoPolicy.PUBLIC_POOL),
            eq(123L),
            false);
    }

    /**
     * Test-purposed extension of {@code GridIoManager} with no-op {@code send(...)} methods.
     */
    private static class TestGridIoManager extends GridIoManager {
        /**
         * @param ctx Grid kernal context.
         */
        TestGridIoManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public void send(ClusterNode node, GridTopic topic, Message msg, byte plc, boolean async)
            throws IgniteCheckedException {
            // No-op.
        }
    }

    /**
     * Mockito argument matcher to compare collections produced by {@code F.view()} methods.
     */
    private static class IsEqualCollection extends ArgumentMatcher<Collection<? extends ClusterNode>> {
        /** Expected collection. */
        private final Collection<? extends ClusterNode> expCol;

        /**
         * Default constructor.
         *
         * @param expCol Expected collection.
         */
        IsEqualCollection(Collection<? extends ClusterNode> expCol) {
            this.expCol = expCol;
        }

        /**
         * Matches a given collection to the specified in constructor expected one
         * with Apache {@code CollectionUtils.isEqualCollection()}.
         *
         * @param colToCheck Collection to be matched against the expected one.
         * @return True if collections matches.
         */
        @Override public boolean matches(Object colToCheck) {
            return CollectionUtils.isEqualCollection(expCol, (Collection)colToCheck);
        }
    }

    /** */
    private static class TestMessage implements Message {
        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }
    }
}