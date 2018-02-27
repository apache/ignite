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

package org.apache.ignite.internal.binary;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for binary object affinity key.
 */
public class GridBinaryAffinityKeySelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicReference<UUID> nodeId = new AtomicReference<>();

    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static int GRID_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();

        typeCfg.setTypeName(TestObject.class.getName());

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setTypeConfigurations(Collections.singleton(typeCfg));

        cfg.setBinaryConfiguration(bCfg);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration(TestObject.class.getName(), "affKey");
        CacheKeyConfiguration keyCfg2 = new CacheKeyConfiguration("TestObject2", "affKey");

        cfg.setCacheKeyConfiguration(keyCfg, keyCfg2);

        cfg.setMarshaller(new BinaryMarshaller());

        if (!gridName.equals(getTestGridName(GRID_CNT))) {
            CacheConfiguration cacheCfg = new CacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);

            cfg.setCacheConfiguration(cacheCfg);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinity() throws Exception {
        checkAffinity(grid(0));

        try (Ignite igniteNoCache = startGrid(GRID_CNT)) {
            try {
                igniteNoCache.cache(null);
            }
            catch (IllegalArgumentException ignore) {
                // Expected error.
            }

            checkAffinity(igniteNoCache);
        }
    }

    /**
     * @param ignite Ignite.
     * @throws Exception If failed.
     */
    private void checkAffinity(Ignite ignite) throws Exception {
        Affinity<Object> aff = ignite.affinity(null);

        GridAffinityProcessor affProc = ((IgniteKernal)ignite).context().affinity();

        IgniteCacheObjectProcessor cacheObjProc = ((IgniteKernal)ignite).context().cacheObjects();

        CacheObjectContext cacheObjCtx = cacheObjProc.contextForCache(
            ignite.cache(null).getConfiguration(CacheConfiguration.class));

        for (int i = 0; i < 1000; i++) {
            assertEquals(i, aff.affinityKey(i));

            assertEquals(i, aff.affinityKey(new TestObject(i)));

            assertEquals(i, aff.affinityKey(ignite.binary().toBinary(new TestObject(i))));

            assertEquals(i, aff.affinityKey(new AffinityKey(0, i)));

            BinaryObjectBuilder bldr = ignite.binary().builder("TestObject2");

            bldr.setField("affKey", i);

            assertEquals(i, aff.affinityKey(bldr.build()));

            CacheObject cacheObj = cacheObjProc.toCacheObject(cacheObjCtx, new TestObject(i), true);

            assertEquals(i, aff.affinityKey(cacheObj));

            assertEquals(aff.mapKeyToNode(i), aff.mapKeyToNode(new TestObject(i)));

            assertEquals(aff.mapKeyToNode(i), aff.mapKeyToNode(cacheObj));

            assertEquals(i, affProc.affinityKey(null, i));

            assertEquals(i, affProc.affinityKey(null, new TestObject(i)));

            assertEquals(i, affProc.affinityKey(null, cacheObj));

            assertEquals(affProc.mapKeyToNode(null, i), affProc.mapKeyToNode(null, new TestObject(i)));

            assertEquals(affProc.mapKeyToNode(null, i), affProc.mapKeyToNode(null, cacheObj));

            assertEquals(affProc.mapKeyToNode(null, new AffinityKey(0, i)), affProc.mapKeyToNode(null, i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        Affinity<Object> aff = grid(0).affinity(null);

        for (int i = 0; i < 1000; i++) {
            nodeId.set(null);

            grid(0).compute().affinityRun((String)null, new TestObject(i), new IgniteRunnable() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public void run() {
                    nodeId.set(ignite.configuration().getNodeId());
                }
            });

            assertEquals(aff.mapKeyToNode(i).id(), nodeId.get());

            grid(0).compute().affinityRun((String)null, new AffinityKey(0, i), new IgniteRunnable() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public void run() {
                    nodeId.set(ignite.configuration().getNodeId());
                }
            });

            assertEquals(aff.mapKeyToNode(i).id(), nodeId.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        Affinity<Object> aff = grid(0).affinity(null);

        for (int i = 0; i < 1000; i++) {
            nodeId.set(null);

            grid(0).compute().affinityCall((String)null, new TestObject(i), new IgniteCallable<Object>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object call() {
                    nodeId.set(ignite.configuration().getNodeId());

                    return null;
                }
            });

            assertEquals(aff.mapKeyToNode(i).id(), nodeId.get());
        }
    }

    /**
     */
    private static class TestObject {
        /** */
        @SuppressWarnings("UnusedDeclaration")
        private int affKey;

        /**
         */
        private TestObject() {
            // No-op.
        }

        /**
         * @param affKey Affinity key.
         */
        private TestObject(int affKey) {
            this.affKey = affKey;
        }
    }
}
