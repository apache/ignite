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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests situation when two nodes in cluster simultaneously propose different classes with the same typeId
 * (which is actually class name's <b>hashCode</b> ).
 *
 * In that case one of the propose requests should be rejected
 * and {@link org.apache.ignite.internal.processors.marshaller.MappingProposedMessage} is sent
 * with not-null <b>conflictingClsName</b> field.
 */
public class IgniteMarshallerCacheClassNameConflictTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean bbClsRejected;

    /** */
    private volatile boolean aaClsRejected;

    /** */
    private volatile boolean rejectObserved;

    /**
     * Latch used to synchronize two nodes on sending mapping requests for classes with conflicting names.
     */
    private static final CountDownLatch startLatch = new CountDownLatch(3);

    /** */
    private static volatile boolean busySpinFlag;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TestTcpDiscoverySpi();
        disco.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(REPLICATED);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        // Use case sensitive mapper
        BinaryConfiguration binaryCfg = new BinaryConfiguration().setIdMapper(new BinaryBasicIdMapper(false));

        cfg.setBinaryConfiguration(binaryCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCachePutGetClassesWithNameConflict() throws Exception {
        Ignite srv1 = startGrid(0);
        Ignite srv2 = startGrid(1);
        ExecutorService exec1 = srv1.executorService();
        ExecutorService exec2 = srv2.executorService();

        final AtomicInteger trickCompilerVar = new AtomicInteger(1);

        // "Aa" and "BB" have same hash code
        final Aa aOrg1 = new Aa(1, "Microsoft", "One Microsoft Way Redmond, WA 98052-6399, USA");
        final BB bOrg2 = new BB(2, "Apple", "1 Infinite Loop, Cupertino, CA 95014, USA");

        exec1.submit(new Runnable() {
            @Override public void run() {
                startLatch.countDown();

                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //busy spinning after waking up from startLatch.await
                // to reduce probability that one thread starts significantly earlier than the other
                while (!busySpinFlag) {
                    if (trickCompilerVar.get() < 0)
                        break;
                }

                Ignition.localIgnite().cache(DEFAULT_CACHE_NAME).put(1, aOrg1);
            }
        });

        exec2.submit(new Runnable() {
            @Override public void run() {
                startLatch.countDown();

                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //busy spinning after waking up from startLatch.await
                // to reduce probability that one thread starts significantly earlier than the other
                while (!busySpinFlag) {
                    if (trickCompilerVar.get() < 0)
                        break;
                }

                Ignition.localIgnite().cache(DEFAULT_CACHE_NAME).put(2, bOrg2);
            }
        });
        startLatch.countDown();

        busySpinFlag = true;

        exec1.shutdown();
        exec2.shutdown();

        exec1.awaitTermination(100, TimeUnit.MILLISECONDS);
        exec2.awaitTermination(100, TimeUnit.MILLISECONDS);

        Ignite ignite = startGrid(2);

        int cacheSize = ignite.cache(DEFAULT_CACHE_NAME).size(CachePeekMode.PRIMARY);

        assertTrue("Expected cache size 1 but was " + cacheSize, cacheSize == 1);

        if (rejectObserved)
            assertTrue(aaClsRejected || bbClsRejected);
    }

    /** */
    private class TestTcpDiscoverySpi extends TcpDiscoverySpi {

        /** */
        private class DiscoverySpiListenerWrapper implements DiscoverySpiListener {
            /** */
            private DiscoverySpiListener delegate;

            /**
             * @param delegate Delegate.
             */
            private DiscoverySpiListenerWrapper(DiscoverySpiListener delegate) {
                this.delegate = delegate;
            }

            /** {@inheritDoc} */
            @Override public IgniteFuture<?> onDiscovery(
                    int type,
                    long topVer,
                    ClusterNode node,
                    Collection<ClusterNode> topSnapshot,
                    @Nullable Map<Long, Collection<ClusterNode>> topHist,
                    @Nullable DiscoverySpiCustomMessage spiCustomMsg
            ) {
                DiscoveryCustomMessage customMsg = spiCustomMsg == null ? null
                        : (DiscoveryCustomMessage) U.field(spiCustomMsg, "delegate");

                if (customMsg != null) {
                    //don't want to make this class public, using equality of class name instead of instanceof operator
                    if ("MappingProposedMessage".equals(customMsg.getClass().getSimpleName())) {
                        String conflClsName = U.field(customMsg, "conflictingClsName");
                        if (conflClsName != null && !conflClsName.isEmpty()) {
                            rejectObserved = true;
                            if (conflClsName.contains(Aa.class.getSimpleName()))
                                bbClsRejected = true;
                            else if (conflClsName.contains(BB.class.getSimpleName()))
                                aaClsRejected = true;
                        }
                    }
                }

                if (delegate != null)
                    return delegate.onDiscovery(type, topVer, node, topSnapshot, topHist, spiCustomMsg);

                return new IgniteFinishedFutureImpl<>();
            }

            /** {@inheritDoc} */
            @Override public void onLocalNodeInitialized(ClusterNode locNode) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
            super.setListener(new DiscoverySpiListenerWrapper(lsnr));
        }
    }

}

/**
 * Class name is chosen to be in conflict with other class name this test put to cache.
 */
class Aa {
    /** */
    private final int id;

    /** */
    private final String name;

    /** */
    private final String addr;

    /**
     * @param id Id.
     * @param name Name.
     * @param addr Address.
     */
    Aa(int id, String name, String addr) {
        this.id = id;
        this.name = name;
        this.addr = addr;
    }
}

/**
 * Class name is chosen to be in conflict with other class name this test put to cache.
 */
class BB {
    /** */
    private final int id;

    /** */
    private final String name;

    /** */
    private final String addr;

    /**
     * @param id Id.
     * @param name Name.
     * @param addr Address.
     */
    BB(int id, String name, String addr) {
        this.id = id;
        this.name = name;
        this.addr = addr;
    }
}
