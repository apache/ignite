package org.apache.ignite.internal.processors.cache.consistency;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

/**
 *
 */
public class IsolationRestrictionsCacheConsistencyTest extends AbstractCacheConsistencyTest {
    /**
     *
     */
    @Test
    public void test() throws Exception {
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     *
     */
    public void test(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        for (Ignite initiator : G.allGrids()) {
            Boolean[] booleans = {Boolean.TRUE, Boolean.FALSE};
            Integer cnt = 20;

            for (Boolean raw : booleans) {
                // Get -> Get with Consistency -> Commit
                // Consistency get allowed only in case value was not cached inside tx during previous read.
                prepareAndCheck(initiator, cnt, raw, (ConsistencyRecoveryData data) -> {
                    IgniteCache<Integer, Integer> cache = data.cache;

                    for (Map.Entry<Integer, InconsistencyValuesMapping> entry : data.data.entrySet()) {
                        try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                            Integer key = entry.getKey();
                            Integer latest = entry.getValue().latest;

                            Integer loc = raw ? // Successful for any isolation mode.
                                cache.getEntry(key).getValue() :
                                cache.get(key);

                            assertTrue(loc > 0);

                            try {
                                Integer res = raw ?
                                    cache.withConsistency().getEntry(key).getValue() :
                                    cache.withConsistency().get(key);

                                assertTrue(isolation == TransactionIsolation.READ_COMMITTED);

                                assertEquals(latest, res);

                                loc = raw ? // Successful for any isolation mode.
                                    cache.getEntry(key).getValue() :
                                    cache.get(key);

                                assertEquals(latest, loc);

                            }
                            catch (CacheException e) {
                                assertTrue(isolation != TransactionIsolation.READ_COMMITTED);
                            }

                            try {
                                tx.commit();

                                assertTrue(isolation == TransactionIsolation.READ_COMMITTED);
                            }
                            catch (TransactionRollbackException e) {
                                assertTrue(isolation != TransactionIsolation.READ_COMMITTED);
                            }
                        }
                    }
                });

                // Put -> Get with Consistency
                // Consistency get allowed only in case value was not cached inside tx during previous put.
                prepareAndCheck(initiator, cnt, raw, (ConsistencyRecoveryData data) -> {
                    IgniteCache<Integer, Integer> cache = data.cache;

                    for (Map.Entry<Integer, InconsistencyValuesMapping> entry : data.data.entrySet()) {
                        Integer updated = 42;

                        try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                            Integer key = entry.getKey();

                            if (ThreadLocalRandom.current().nextBoolean())
                                cache.withConsistency().put(key, updated);
                            else
                                cache.put(key, updated);

                            try {
                                Integer res = raw ?
                                    cache.withConsistency().getEntry(key).getValue() :
                                    cache.withConsistency().get(key);

                                fail("Should not happen." + res);
                            }
                            catch (CacheException ignored) {
                                // No-op.
                            }

                            try {
                                tx.commit();

                                fail("Should not happen.");
                            }
                            catch (TransactionRollbackException ignored) {
                                // No-op.
                            }
                        }
                    }
                });
            }
        }
    }
}

