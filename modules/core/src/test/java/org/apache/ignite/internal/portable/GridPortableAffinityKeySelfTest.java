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

package org.apache.ignite.internal.portable;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cacheobject.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test for portable object affinity key.
 */
public class GridPortableAffinityKeySelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicReference<UUID> nodeId = new AtomicReference<>();

    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static int GRID_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableTypeConfiguration typeCfg = new PortableTypeConfiguration();

        typeCfg.setClassName(TestObject.class.getName());
        typeCfg.setAffinityKeyFieldName("affKey");

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Collections.singleton(typeCfg));

        cfg.setMarshaller(marsh);

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

            CacheObject cacheObj = cacheObjProc.toCacheObject(cacheObjCtx, new TestObject(i), true);

            assertEquals(i, aff.affinityKey(cacheObj));

            assertEquals(aff.mapKeyToNode(i), aff.mapKeyToNode(new TestObject(i)));

            assertEquals(aff.mapKeyToNode(i), aff.mapKeyToNode(cacheObj));

            assertEquals(i, affProc.affinityKey(null, i));

            assertEquals(i, affProc.affinityKey(null, new TestObject(i)));

            assertEquals(i, affProc.affinityKey(null, cacheObj));

            assertEquals(affProc.mapKeyToNode(null, i), affProc.mapKeyToNode(null, new TestObject(i)));

            assertEquals(affProc.mapKeyToNode(null, i), affProc.mapKeyToNode(null, cacheObj));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        Affinity<Object> aff = grid(0).affinity(null);

        for (int i = 0; i < 1000; i++) {
            nodeId.set(null);

            grid(0).compute().affinityRun(null, new TestObject(i), new IgniteRunnable() {
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

            grid(0).compute().affinityCall(null, new TestObject(i), new IgniteCallable<Object>() {
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
