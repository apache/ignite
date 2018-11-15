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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Cache + conditional deployment test.
 */
public class GridCacheConditionalDeploymentSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    static {
        GridIoMessageFactory.registerCustom(TestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new TestMessage();
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setRebalanceMode(SYNC);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite ignite0 = startGrid(0);

        startGrid(1);

        awaitPartitionMapExchange();

        ignite0.cache(DEFAULT_CACHE_NAME).put(1, new TestValue());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        Ignition.stopAll(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testNoDeploymentInfo() throws Exception {
        GridCacheIoManager ioMgr = cacheIoManager();

        TestMessage msg = new TestMessage();

        assertNull(msg.deployInfo());

        msg.addDepInfo = false;

        IgniteUtils.invoke(GridCacheIoManager.class, ioMgr, "onSend", msg, grid(1).cluster().localNode().id());

        assertNull(msg.deployInfo());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testAddedDeploymentInfo() throws Exception {
        GridCacheContext ctx = cacheContext();

        if (grid(0).configuration().getMarshaller() instanceof BinaryMarshaller)
            assertFalse(ctx.deploymentEnabled());
        else {
            GridCacheIoManager ioMgr = cacheIoManager();

            TestMessage msg = new TestMessage();

            assertNull(msg.deployInfo());

            msg.addDepInfo = true;

            IgniteUtils.invoke(GridCacheIoManager.class, ioMgr, "onSend", msg, grid(1).cluster().localNode().id());

            assertNotNull(msg.deployInfo());
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testAddedDeploymentInfo2() throws Exception {
        GridCacheContext ctx = cacheContext();

        if (grid(0).configuration().getMarshaller() instanceof BinaryMarshaller)
            assertFalse(ctx.deploymentEnabled());
        else {
            assertTrue(ctx.deploymentEnabled());

            GridCacheIoManager ioMgr = cacheIoManager();

            TestMessage msg = new TestMessage();

            assertNull(msg.deployInfo());

            msg.addDepInfo = false;

            IgniteUtils.invoke(GridCacheIoManager.class, ioMgr, "onSend", msg, grid(1).cluster().localNode().id());

            assertNull(msg.deployInfo());
        }
    }

    /**
     * @return Cache context.
     */
    protected GridCacheContext cacheContext() {
        return ((IgniteCacheProxy)grid(0).cache(DEFAULT_CACHE_NAME)).context();
    }

    /**
     * @return IO manager.
     */
    protected GridCacheIoManager cacheIoManager() {
        return grid(0).context().cache().context().io();
    }

    /**
     * Test message class.
     */
    public static class TestMessage  extends GridCacheMessage implements GridCacheDeployable {
        /** */
        public static final short DIRECT_TYPE = 302;

        /** {@inheritDoc} */
        @Override public int handlerId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheGroupMessage() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 3;
        }

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return addDepInfo;
        }
    }

    /** */
    private static class TestValue {

    }
}
