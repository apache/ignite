/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test that in {@link CacheMode#PARTITIONED} mode cache writes values only to the near cache store. <p/> This check
 * is needed because in current implementation if {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore} assumes that and user store is
 * wrapped only in near cache (see {@link GridCacheProcessor} init logic).
 */
@SuppressWarnings({"unchecked"})
@RunWith(JUnit4.class)
public class GridCachePartitionedWritesTest extends GridCommonAbstractTest {
    /** Cache store. */
    private CacheStore store;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);

        assert store != null;

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        store = null;

        super.afterTest();
    }

    /** @throws Exception If test fails. */
    @Test
    public void testWrite() throws Exception {
        final AtomicInteger putCnt = new AtomicInteger();
        final AtomicInteger rmvCnt = new AtomicInteger();

        store = new CacheStoreAdapter<Object, Object>() {
            @Override public Object load(Object key) {
                info(">>> Get [key=" + key + ']');

                return null;
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Object, ? extends Object> entry) {
                putCnt.incrementAndGet();
            }

            @Override public void delete(Object key) {
                rmvCnt.incrementAndGet();
            }
        };

        startGrid();

        IgniteCache<Integer, String> cache = jcache();

        try {
            cache.get(1);

            Transaction tx = grid().transactions().txStart();

            try {
                for (int i = 1; i <= 10; i++)
                    cache.put(i, Integer.toString(i));

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert cache.size() == 10;

            assert putCnt.get() == 10;

            tx = grid().transactions().txStart();

            try {
                for (int i = 1; i <= 10; i++) {
                    String val = cache.getAndRemove(i);

                    assert val != null;
                    assert val.equals(Integer.toString(i));
                }

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert rmvCnt.get() == 10;
        }
        finally {
            stopGrid();
        }
    }
}
