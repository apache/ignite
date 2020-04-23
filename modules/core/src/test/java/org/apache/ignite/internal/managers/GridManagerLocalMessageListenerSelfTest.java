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

package org.apache.ignite.internal.managers;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoUserMessage;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Test Managers to add and remove local message listener.
 */
public class GridManagerLocalMessageListenerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final short DIRECT_TYPE = 210;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setPluginProviders(new TestPluginProvider());

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        c.setCommunicationSpi(commSpi);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSendMessage() throws Exception {
        startGridsMultiThreaded(2);

        IgniteSpiContext ctx0 = ((IgniteSpiAdapter)grid(0).context().io().getSpi()).getSpiContext();
        IgniteSpiContext ctx1 = ((IgniteSpiAdapter)grid(1).context().io().getSpi()).getSpiContext();

        String topic = "test-topic";

        final CountDownLatch latch = new CountDownLatch(1);

        ctx1.addLocalMessageListener(topic, new IgniteBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                assertEquals("Message", msg);

                latch.countDown();

                return true;
            }
        });

        long time = System.nanoTime();

        ctx0.send(grid(1).localNode(), "Message", topic);

        assert latch.await(3, SECONDS);

        time = System.nanoTime() - time;

        info(">>>");
        info(">>> send() time (ms): " + MILLISECONDS.convert(time, NANOSECONDS));
        info(">>>");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddLocalMessageListener() throws Exception {
        startGrid();

        Manager mgr = new Manager(grid().context(), new Spi());

        mgr.start();

        mgr.onKernalStart(true);

        assertTrue(mgr.enabled());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveLocalMessageListener() throws Exception {
        startGrid();

        Manager mgr = new Manager(grid().context(), new Spi());

        assertTrue(mgr.enabled());

        mgr.onKernalStart(true);

        mgr.onKernalStop(false);

        mgr.stop(false);

        assertTrue(mgr.enabled());
    }

    /** */
    private static class Manager extends GridManagerAdapter<IgniteSpi> {
        /**
         * @param ctx Kernal context.
         * @param spis Specific SPI instance.
         */
        protected Manager(GridKernalContext ctx, IgniteSpi... spis) {
            super(ctx, spis);
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws IgniteCheckedException {
            // No-op.
        }
    }

    /**
     * Test Spi.
     */
    private static interface TestSpi extends IgniteSpi {
        // No-op.
    }

    /**
     * Spi
     */
    private static class Spi extends IgniteSpiAdapter implements TestSpi {
        /** Ignite Spi Context. **/
        private IgniteSpiContext spiCtx;

        /** Test message topic. **/
        private static final String TEST_TOPIC = "test_topic";

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
            this.spiCtx = spiCtx;

            spiCtx.addLocalMessageListener(TEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    return true;
                }
            });

        }

        /** {@inheritDoc} */
        @Override public void onContextDestroyed0() {
            spiCtx.removeLocalMessageListener(TEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    return true;
                }
            });
        }
    }

    /** */
    public static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_PLUGIN";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(MessageFactory.class, new MessageFactoryProvider() {
                @Override public void registerAll(IgniteMessageFactory factory) {
                    factory.register(DIRECT_TYPE, GridIoUserMessage::new);
                }
            });
        }
    }
}
