package org.apache.ignite.internal.processors.security.events;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

public class TxOptimisticRemoveValTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("srv");
        startGrid("additional_srv");

        startClientGrid("clnt");
    }

    @Test
    public void test() {
        grid("srv").createCache(
            new CacheConfiguration<String, String>("test_cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
        ).put("key", "value");

        Ignite clnt = grid("clnt");
        IgniteCache<String, String> clntCache = clnt.cache("test_cache");

        try (Transaction tx = clnt.transactions()
            .txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
            clntCache.removeAsync("key", "value").get();

            tx.commit();
        }
    }
}
