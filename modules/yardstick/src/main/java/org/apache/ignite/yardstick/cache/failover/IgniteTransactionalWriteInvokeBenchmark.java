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

package org.apache.ignite.yardstick.cache.failover;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Atomic retries failover benchmark. Client generates continuous load to the cluster
 * (random get, put, invoke, remove operations)
 */
public class IgniteTransactionalWriteInvokeBenchmark extends IgniteFailoverAbstractBenchmark<String, Long> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int k = nextRandom(100_000);

        final String[] keys = new String[5]; // TODO impl number.

        assert keys.length > 0 : "Count of keys = " + keys.length;

        final String masterKey = "key-" + k + "-master";

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-"+k+"-"+i;

        final int scenario = nextRandom(2);

        return doInTransaction(ignite(), new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                switch (scenario) {
                    case 0: // Read scenario.

                        Map<String, Long> map = new HashMap<>();

                        map.put(masterKey, cache.get(masterKey));

                        for (String key : keys)
                            map.put(key, cache.get(key));

                        Set<Long> values = new HashSet<>(map.values());

                        if (values.size() != 1) {
                            println(cfg, "Got different values for keys [map="+map+"]");

                            return false;
                        }

                        break;
                    case 1: // Invoke scenario.
                        Long val = cache.get(masterKey);

                        cache.put(masterKey, val == null ? 0 : val + 1);

                        for (String key : keys)
                            cache.invoke(key, new IncrementCacheEntryProcessor());

                        break;
                    }

                return true;
            }
        });
    }

    /**
     * @param ignite Ignite instance.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception
     */
    private static <T> T doInTransaction(Ignite ignite, Callable<T> clo) throws Exception {
        while (true) {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                T res = clo.call();

                tx.commit();

                return res;
            }
            catch (CacheException e) {
                if (e.getCause() instanceof ClusterTopologyException) {
                    ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                    topEx.retryReadyFuture().get();
                }
                else
                    throw e;
            }
            catch (ClusterTopologyException e) {
                e.retryReadyFuture().get();
            }
            catch (TransactionRollbackException ignore) {
                // Safe to retry right away.
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<String, Long> cache() {
        return ignite().cache("tx");
    }

    /**
     */
    private static class IncrementCacheEntryProcessor implements CacheEntryProcessor<String, Long, Void> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<String, Long> entry, Object... arguments) throws EntryProcessorException {
            entry.setValue(entry.getValue() == null ? 0 : entry.getValue() + 1);

            return null;
        }
    }
}
