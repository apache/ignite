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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.jta.*;
import org.objectweb.jotm.*;

import javax.transaction.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 * Abstract class for cache tests.
 */
public class GridCacheJtaSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** Java Open Transaction Manager facade. */
    private static Jotm jotm;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        jotm = new Jotm(true, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        jotm.stop();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setTransactionManagerLookupClassName(TestTmLookup.class.getName());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cfg1 = cacheConfiguration(gridName);

        GridCacheConfiguration cfg2 = cacheConfiguration(gridName);

        cfg2.setName("cache-2");

        cfg.setCacheConfiguration(cfg1, cfg2);

        return cfg;
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTmLookup implements GridCacheTmLookup {
        /** {@inheritDoc} */
        @Override public TransactionManager getTm() throws IgniteCheckedException {
            return jotm.getTransactionManager();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testJta() throws Exception {
        UserTransaction jtaTx = jotm.getUserTransaction();

        assert cache().tx() == null;

        jtaTx.begin();

        try {
            assert cache().tx() == null;

            assert cache().put("key", 1) == null;

            IgniteTx tx = cache().tx();

            assert tx != null;
            assert tx.state() == ACTIVE;

            Integer one = 1;

            assertEquals(one, cache().get("key"));

            tx = cache().tx();

            assert tx != null;
            assert tx.state() == ACTIVE;

            jtaTx.commit();

            assert cache().tx() == null;
        }
        finally {
            if (jtaTx.getStatus() == Status.STATUS_ACTIVE)
                jtaTx.rollback();
        }

        assertEquals((Integer)1, cache().get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void _testJtaTwoCaches() throws Exception { // TODO GG-9141
        UserTransaction jtaTx = jotm.getUserTransaction();

        GridCache<String, Integer> cache1 = cache();

        GridCache<String, Integer> cache2 = grid(0).cache("cache-2");

        assertNull(cache1.tx());
        assertNull(cache2.tx());

        jtaTx.begin();

        try {
            cache1.put("key", 1);
            cache2.put("key", 1);

            assertEquals(1, (int)cache1.get("key"));
            assertEquals(1, (int)cache2.get("key"));

            assertEquals(cache1.tx().state(), ACTIVE);
            assertEquals(cache2.tx().state(), ACTIVE);

            jtaTx.commit();

            assertNull(cache1.tx());
            assertNull(cache2.tx());

            assertEquals(1, (int)cache1.get("key"));
            assertEquals(1, (int)cache2.get("key"));
        }
        finally {
            if (jtaTx.getStatus() == Status.STATUS_ACTIVE)
                jtaTx.rollback();
        }

        assertEquals(1, (int)cache1.get("key"));
        assertEquals(1, (int)cache2.get("key"));
    }
}
