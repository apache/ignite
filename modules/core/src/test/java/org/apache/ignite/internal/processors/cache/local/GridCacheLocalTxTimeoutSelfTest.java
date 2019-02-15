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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class GridCacheLocalTxTimeoutSelfTest extends GridCommonAbstractTest {
    /** Grid. */
    private static Ignite ignite;

    /**
     * Start grid by default.
     */
    public GridCacheLocalTxTimeoutSelfTest() {
        super(true /*start grid. */);
    }

    /** */
    @Before
    public void beforeGridCacheLocalTxTimeoutSelfTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        IgniteConfiguration c = super.getConfiguration();

        c.getTransactionConfiguration().setTxSerializableEnabled(true);
        c.getTransactionConfiguration().setDefaultTxTimeout(50);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);

        c.setCacheConfiguration(cc);

        c.setNetworkTimeout(1000);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticReadCommitted() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticSerializable() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testOptimisticReadCommitted() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testOptimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testOptimisticSerializable() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If test failed.
     */
    private void checkTransactionTimeout(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {

        boolean wasEx = false;

        Transaction tx = null;

        try {
            IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

            tx = ignite.transactions().txStart(concurrency, isolation, 50, 0);

            cache.put(1, "1");

            Thread.sleep(100);

            cache.put(1, "2");

            tx.commit();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, TransactionTimeoutException.class));

            info("Received expected optimistic exception: " + e.getMessage());

            wasEx = true;

            tx.rollback();
        }

        assert wasEx;
    }
}
