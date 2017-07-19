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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteBinaryMetadataUpdateFromInvoke extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setMarshaller(null);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration("cache");

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetadataUpdateFromAtomicInvoke() throws Exception {
        for (int i = 0; i < 2; i++) {
            client = false;

            log.info("Iteration: " + i);

            final int SRVS = 4;

            startGrids(SRVS);

            awaitPartitionMapExchange();

            final CyclicBarrier b = new CyclicBarrier(3);

            final AtomicBoolean stop = new AtomicBoolean();

            final int META_PRIMARY_NODE = 0;
            final int META_UPDATE_FROM_NODE = 1;

            BinaryMarshaller marsh = (BinaryMarshaller)ignite(META_PRIMARY_NODE).configuration().getMarshaller();

            marsh.getContext().registerClass(
                marsh.binaryMarshaller().context().typeId(TestEnum1.class.getName()), TestEnum1.class);

            TestRecordingCommunicationSpi testSpi =
                (TestRecordingCommunicationSpi)ignite(META_UPDATE_FROM_NODE).configuration().getCommunicationSpi();

            testSpi.blockMessages(GridNearTxPrepareRequest.class, getTestGridName(META_PRIMARY_NODE));

            IgniteInternalFuture<?> sFut1 = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCache<Object, Object> cache = ignite(META_UPDATE_FROM_NODE).cache("cache");

                    List<Integer> keys = primaryKeys(cache, 1);

                    b.await();

                    cache.invoke(keys.get(0), new TestEntryProcessor());

                    return null;
                }
            }, "async-node-0");

            IgniteInternalFuture<?> sFut2 = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCache<Object, Object> cache = ignite(META_PRIMARY_NODE).cache("cache");

                    List<Integer> keys = primaryKeys(cache, 1);

                    b.await();

                    Thread.sleep(2000);

                    cache.invoke(keys.get(0), new TestEntryProcessor());

                    return null;
                }
            }, "async-node-1");

            IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    client = true;

                    b.await();

                    for (int i = 0; i < 1; i++)
                        startGrid(SRVS + i);

                    stop.set(true);

                    return null;
                }
            });

            testSpi.waitForBlocked(GridNearTxPrepareRequest.class, getTestGridName(META_PRIMARY_NODE));

            U.sleep(5000);

            testSpi.stopBlock();

            sFut1.get();
            sFut2.get();
            fut2.get();

            stopAllGrids();
        }
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            e.setValue(TestEnum1.ENUM);

            return null;
        }
    }

    /**
     *
     */
    enum TestEnum1 {
        /** */
        ENUM
    }
}
