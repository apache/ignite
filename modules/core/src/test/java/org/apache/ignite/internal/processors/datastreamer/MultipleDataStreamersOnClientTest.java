/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastreamer;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOsConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests parallel creation of data streamer for the same cache on client node.
 */
public class MultipleDataStreamersOnClientTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE_NAME = "multiple-datastreamers-on-client";

    /** Latch indicates that a test cache is starting on client. */
    private static final CountDownLatch CACHE_START_LATCH = new CountDownLatch(1);

    /** Latch that allows to continue cache starting. */
    private static final CountDownLatch CONTINUE_LATCH = new CountDownLatch(1);

    /** Default timeout for an operation. */
    private static final int TIMEOUT = 5;

    /** Boolean parameters indicates that a node should be started in client mode. */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        TestDataStreamerPluginProvider.enable(true);
    }

    /** */
    @After
    public void after() throws Exception {
        TestDataStreamerPluginProvider.enable(false);

        stopAllGrids();
    }

    /**
     * Tests multiple creations of data streamer for the same cache on the client node.
     * Note that one of streamer uses an internal api {@link DataStreamerImpl#addDataInternal(KeyCacheObject, CacheObject)}
     * that emulates some aspects of internal usage of DataStreamer.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleDatastreamersOnClientNode() throws Exception {
        IgniteEx srv = startGrid(0);

        clientMode = true;

        srv.getOrCreateCache(TEST_CACHE_NAME);

        final IgniteEx client = startGrid(1);

        IgniteInternalFuture dataStreamerFut1 = GridTestUtils.runAsync(() -> {
            try (IgniteDataStreamer streamer1 = client.dataStreamer(TEST_CACHE_NAME)) {
                streamer1.addData(42, 42);
            }
        });

        assertTrue(
            "Could not wait for a cache start in " + TIMEOUT + " sec.",
            CACHE_START_LATCH.await(TIMEOUT, TimeUnit.SECONDS));

        assert !dataStreamerFut1.isDone();

        IgniteInternalFuture dataStreamerFut2 = GridTestUtils.runAsync(() -> {
            BinaryObjectImpl key = (BinaryObjectImpl)client.binary().builder("TestKey")
                .setField("id", 100).build();

            BinaryObjectImpl val = (BinaryObjectImpl)client.binary().builder("TestValue")
                .setField("val", "Value").build();

            try (DataStreamerImpl streamer1 = (DataStreamerImpl)client.dataStreamer(TEST_CACHE_NAME)) {
                streamer1.addDataInternal(key, val);
            }
        });

        try {
            dataStreamerFut2.get(5, TimeUnit.SECONDS);

            fail("Data streamer should not be created at this moment.");
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            // Expected exception.
        }
        catch (IgniteCheckedException e) {
            fail("Unexpected exception [exc=" + e + ']');
        }
        finally {
            // Continue cache start procedure.
            CONTINUE_LATCH.countDown();
        }

        dataStreamerFut1.get();
        dataStreamerFut2.get();
    }

    /**
     * Test plugin provider that creates a custom conflict resolver manager,
     * which is used in order to block cache initialization on the client node.
     */
    public static class TestDataStreamerPluginProvider implements PluginProvider, IgnitePlugin {
        /** Flag indicates that this plugin provider is enabled. */
        private static volatile boolean enabled;

        /**
         * Enables this plugin provider.
         * @param enable {@code true} if plugin provider should be enabled.
         */
        public static void enable(boolean enable) {
            enabled = enable;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "MultipleDataStreamersOnClientTestProvider";
        }

        /** {@inheritDoc} */
        @Override public String version() {
            return "1.0";
        }

        /** {@inheritDoc} */
        @Override public String copyright() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgnitePlugin plugin() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public Object createComponent(PluginContext ctx, Class cls) {
            if (enabled && cls.equals(CacheConflictResolutionManager.class) && ctx.grid().configuration().isClientMode()) {
                return new CacheOsConflictResolutionManager() {
                    /** {@inheritDoc} */
                    @Override public void start(GridCacheContext cctx) throws IgniteCheckedException {
                        if (TEST_CACHE_NAME.equals(cctx.name())) {
                            CACHE_START_LATCH.countDown();

                            try {
                                CONTINUE_LATCH.await();
                            }
                            catch (InterruptedException e) {
                                throw new IgniteCheckedException(e);
                            }
                        }

                        super.start(cctx);
                    }
                };
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStop(boolean cancel) {
        }

        /** {@inheritDoc} */
        @Override public Serializable provideDiscoveryData(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        }

        /** {@inheritDoc} */
        @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        }
    }
}
