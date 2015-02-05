/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.transactions.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 * Tests various scenarios for {@code containsKey()} method.
 */
public abstract class IgniteCacheContainsKeyAbstractSelfTest extends GridCacheAbstractSelfTest {
    /**
     * @return Number of grids to start.
     */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache(0).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        TransactionsConfiguration tcfg = new TransactionsConfiguration();

        tcfg.setTxSerializableEnabled(true);

        cfg.setTransactionsConfiguration(tcfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedContains() throws Exception {
        String key = "1";

        cache(0).put(key, 1);

        for (int i = 0; i < gridCount(); i++) {
            assertTrue("Invalid result on grid: " + i, cache(i).containsKey(key));

            assertFalse("Invalid result on grid: " + i, cache(i).containsKey("2"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testContainsInTx() throws Exception {
        if (atomicityMode() == TRANSACTIONAL) {
            String key = "1";

            for (int i = 0; i < gridCount(); i++)
                assertFalse("Invalid result on grid: " + i, cache(i).containsKey(key));

            GridCache<String, Integer> cache = cache(0);

            for (IgniteTxConcurrency conc : IgniteTxConcurrency.values()) {
                for (IgniteTxIsolation iso : IgniteTxIsolation.values()) {
                    try (IgniteTx tx = grid(0).transactions().txStart(conc, iso)) {
                        assertFalse("Invalid result on grid inside tx", cache.containsKey(key));

                        assertFalse("Key was enlisted to transaction: " + tx, txContainsKey(tx, key));

                        cache.put(key, 1);

                        assertTrue("Invalid result on grid inside tx", cache.containsKey(key));

                        // Do not commit.
                    }

                    for (int i = 0; i < gridCount(); i++)
                        assertFalse("Invalid result on grid: " + i, cache(i).containsKey(key));
                }
            }
        }
    }

    /**
     * Checks if transaction has given key enlisted.
     *
     * @param tx Transaction to check.
     * @param key Key to check.
     * @return {@code True} if key was enlisted.
     */
    private boolean txContainsKey(IgniteTx tx, String key) {
        IgniteTxProxyImpl<String, Integer> proxy = (IgniteTxProxyImpl<String, Integer>)tx;

        IgniteInternalTx<String, Integer> txEx = proxy.tx();

        IgniteTxEntry entry = txEx.entry(context(0).txKey(key));

        return entry != null;
    }
}
