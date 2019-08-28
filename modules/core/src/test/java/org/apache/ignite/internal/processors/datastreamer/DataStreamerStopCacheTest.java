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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests that closing cache during uploading does not cause grid hang.
 */
public class DataStreamerStopCacheTest extends GridCommonAbstractTest {
    /** Default timeout for operations. */
    private static final long TIMEOUT = 5_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setCacheConfiguration(cacheConfiguration());

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        ccfg.setCacheStoreFactory(FactoryBuilder.factoryOf(TestCacheStore.class));

        return ccfg;
    }

    @Test
    public void testLoadAllAndCacheStop() throws Exception {
        final AtomicReference<Exception> fail = new AtomicReference<>();

        final IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, String> c = ig0.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Set<Integer> keys = new HashSet<>();

        keys.add(42);

        final CountDownLatch loadFinished = new CountDownLatch(1);

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            c.loadAll(keys, true, new CompletionListener() {
                @Override public void onCompletion() {
                    loadFinished.countDown();
                }

                @Override public void onException(Exception e) {
                    fail.compareAndSet(null, e);

                    loadFinished.countDown();
                }
            });
        });

        assertTrue(
            "loadAll() has not finished in " + TIMEOUT + " millis",
            loadFinished.await(TIMEOUT, TimeUnit.MILLISECONDS));

        assertTrue("Expected CacheException is not thrown", X.hasCause(fail.get(), CacheException.class));
    }

    /**
     * Test cache store implementation.
     */
    public static class TestCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            // It is guaranteed that at this point cache gate is already acquired.

            IgniteEx ig0 = (IgniteEx)ignite;

            final CountDownLatch exchangeLatch = new CountDownLatch(1);

            ig0.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
                /** {@inheritDoc} */
                @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                    exchangeLatch.countDown();
                }
            });

            GridTestUtils.runAsync(() -> ignite.destroyCache(DEFAULT_CACHE_NAME));

            try {
                exchangeLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new CacheLoaderException("Failed to wait partition map exchange in 10 sec", e);
            }

            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
