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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionException;
import org.junit.Test;

/**
 * Tests transaction savepoint functionality.
 */
public class TxSavepointItTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setCacheMode(CacheMode.PARTITIONED));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.cache(DEFAULT_CACHE_NAME).clear();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollabackSeveralSavepoints() {
        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int key = 1;

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(key, 1);

            tx.savepoint("sp1");

            cache.put(key, 2);

            tx.savepoint("sp2");

            cache.put(key, 3);

            tx.savepoint("sp3");

            cache.put(key, 4);

            tx.rollbackToSavepoint("sp1");

            tx.commit();
        }

        assertEquals(Integer.valueOf(1), cache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDuplicateSavepointWithoutOverwriteThrows() {
        try (Transaction tx = ignite.transactions().txStart()) {
            tx.savepoint("sp");

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> {
                    tx.savepoint("sp");

                    return null;
                },
                TransactionException.class,
                "already exists"
            );

            tx.savepoint("sp", true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseSavepointReleasesNestedSavepoints() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int key = 1;

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(key, 1);
            tx.savepoint("sp1");

            cache.put(key, 2);
            tx.savepoint("sp2");

            cache.put(key, 3);
            tx.savepoint("sp3");

            cache.put(key, 4);

            tx.releaseSavepoint("sp2");

            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    tx.rollbackToSavepoint("sp3");

                    return null;
                },
                TransactionException.class,
                "Savepoint does not exist");

            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    tx.rollbackToSavepoint("sp2");

                    return null;
                },
                TransactionException.class,
                "Savepoint does not exist");

            tx.rollbackToSavepoint("sp1");

            tx.commit();
        }

        assertEquals(Integer.valueOf(1), cache.get(key));
    }
}
