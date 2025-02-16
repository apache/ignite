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

package org.apache.ignite.spi.communication;

import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Super class for all communication self tests.
 * @param <T> Type of communication SPI.
 */
public abstract class GridAbstractCommunicationSelfTest<T extends CommunicationSpi<Message>> extends GridSpiAbstractTest<T> {
    /** */
    private static final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** */
    protected static final Map<UUID, CommunicationSpi<Message>> spis = new HashMap<>();

    /** */
    protected static final List<ClusterNode> nodes = new ArrayList<>();

    /** */
    private static GridTimeoutProcessor timeoutProcessor;

    /** */
    protected GridAbstractCommunicationSelfTest() {
        super(false);
    }

    /**
     * @param idx Node index.
     * @return Spi.
     */
    protected abstract CommunicationSpi<Message> getSpi(int idx);

    /** */
    protected abstract CommunicationListener<Message> createMessageListener(UUID nodeId);

    /** */
    protected abstract Map<Short, Supplier<Message>> customMessageTypes();

    /** */
    protected boolean isSslEnabled() {
        return false;
    }

    /**
     * @return Spi count.
     */
    protected int getSpiCount() {
        return 2;
    }

    /**
     * @return Max time for message delivery.
     */
    protected int getMaxTransmitMessagesTime() {
        return 20000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < 3; i++) {
            try {
                startSpis();

                break;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(BindException.class)) {
                    if (i < 2) {
                        info("Failed to start SPIs because of BindException, will retry after delay.");

                        afterTestsStopped();

                        U.sleep(30_000);
                    }
                    else
                        throw e;
                }
                else
                    throw e;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void startSpis() throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        timeoutProcessor = new GridTimeoutProcessor(new GridTestKernalContext(log));

        timeoutProcessor.start();

        timeoutProcessor.onKernalStart(true);

        for (int i = 0; i < getSpiCount(); i++) {
            CommunicationSpi<Message> spi = getSpi(i);

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            GridSpiTestContext ctx = initSpiContext();

            MessageFactoryProvider testMsgFactory = new MessageFactoryProvider() {
                @Override public void registerAll(MessageFactory factory) {
                    customMessageTypes().forEach(factory::register);
                }
            };

            ctx.messageFactory(new IgniteMessageFactoryImpl(new MessageFactoryProvider[] {new GridIoMessageFactory(), testMsgFactory}));

            ctx.setLocalNode(node);

            ctx.timeoutProcessor(timeoutProcessor);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName", "grid-" + i);

            if (isSslEnabled()) {
                IgniteMock ignite = GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "ignite");

                IgniteConfiguration cfg = ignite.configuration()
                    .setSslContextFactory(GridTestUtils.sslFactory());

                ignite.setStaticCfg(cfg);
            }

            spi.setListener(createMessageListener(rsrcs.getNodeId()));

            node.setAttribute(ATTR_MACS, F.concat(U.allLocalMACs(), ", "));

            node.order(i + 1);

            nodes.add(node);

            spi.spiStart(getTestIgniteInstanceName() + (i + 1));

            node.setAttributes(spi.getNodeAttributes());

            spis.put(rsrcs.getNodeId(), spi);

            spi.onContextInitialized(ctx);

            ctxs.put(node, ctx);
        }

        // For each context set remote nodes.
        for (Entry<ClusterNode, GridSpiTestContext> e : ctxs.entrySet()) {
            for (ClusterNode n : nodes) {
                if (!n.equals(e.getKey()))
                    e.getValue().remoteNodes().add(n);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (timeoutProcessor != null) {
            timeoutProcessor.onKernalStop(true);

            timeoutProcessor.stop(true);

            timeoutProcessor = null;
        }

        for (CommunicationSpi<Message> spi : spis.values()) {
            spi.onContextDestroyed();

            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rsrcs : spiRsrcs)
            rsrcs.stopThreads();
    }
}
