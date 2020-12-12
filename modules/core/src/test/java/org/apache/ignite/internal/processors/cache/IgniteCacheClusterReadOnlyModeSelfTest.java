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

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 * Tests most of public API methods of {@link IgniteCache} when cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class IgniteCacheClusterReadOnlyModeSelfTest extends IgniteCacheClusterReadOnlyModeAbstractTest {
    /** */
    @Test
    public void testGetAndPutIfAbsentDeniedIfKeyIsPresented() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsent(KEY, VAL));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentDeniedIfKeyIsAbsent() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsent(UNKNOWN_KEY, VAL));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentAsyncDeniedIfKeyIsPresented() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsentAsync(KEY, VAL).get());
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentAsyncDeniedIfKeyIsAbsent() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsentAsync(UNKNOWN_KEY, VAL).get());
    }

    /** */
    @Test
    public void testLockDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.lock(KEY).lock(), TX_CACHES_PRED);
    }

    /** */
    @Test
    public void testLockAllDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.lockAll(asList(KEY, KEY_2)).lock(), TX_CACHES_PRED);
    }

    /** */
    @Test
    public void testScanQueryAllowed() {
        performAction(cache -> {
            try (QueryCursor qry = cache.query(new ScanQuery<>())) {
                for (Object o : qry.getAll()) {
                    IgniteBiTuple<Integer, Integer> tuple = (IgniteBiTuple<Integer, Integer>)o;

                    assertEquals(o.toString(), kvMap.get(tuple.getKey()), tuple.getValue());
                }
            }
        });
    }

    /** */
    @Test
    public void testSizeAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), cache.size()));
    }

    /** */
    @Test
    public void testSizeAsyncAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), (long)cache.sizeAsync().get()));
    }

    /** */
    @Test
    public void testSizeLongAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), cache.sizeLong()));
    }

    /** */
    @Test
    public void testSizeLongAsyncAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), (long)cache.sizeLongAsync().get()));
    }

    /** */
    @Test
    public void testGetAllowed() {
        performAction(cache -> assertEquals(VAL, (int)cache.get(KEY)));
        performAction(cache -> assertNull(cache.get(UNKNOWN_KEY)));
    }

    /** */
    @Test
    public void testGetAsyncAllowed() {
        performAction(cache -> assertEquals(VAL, (int)cache.getAsync(KEY).get()));
        performAction(cache -> assertNull(cache.getAsync(UNKNOWN_KEY).get()));
    }

    /** */
    @Test
    public void testGetEntryAllowed() {
        performAction(cache -> {
            CacheEntry entry = cache.getEntry(KEY);

            assertEquals(KEY, entry.getKey());
            assertEquals(VAL, entry.getValue());
        });

        performAction(cache -> assertNull(cache.getEntry(UNKNOWN_KEY)));
    }

    /** */
    @Test
    public void testGetEntryAsyncAllowed() {
        performAction(cache -> {
            CacheEntry entry = cache.getEntryAsync(KEY).get();

            assertEquals(KEY, entry.getKey());
            assertEquals(VAL, entry.getValue());
        });

        performAction(cache -> assertNull(cache.getEntryAsync(UNKNOWN_KEY).get()));
    }

    /** */
    @Test
    public void testGetAllAllowed() {
        performAction(cache -> assertEquals(kvMap, cache.getAll(kvMap.keySet())));
    }

    /** */
    @Test
    public void testGetAllAsyncAllowed() {
        performAction(cache -> assertEquals(kvMap, (cache.getAllAsync(kvMap.keySet()).get())));
    }

    /** */
    @Test
    public void testGetEntriesAllowed() {
        performAction(cache -> {
            for (Object o : cache.getEntries(kvMap.keySet())) {
                CacheEntry<Integer, Integer> entry = (CacheEntry<Integer, Integer>)o;

                assertEquals(kvMap.get(entry.getKey()), entry.getValue());
            }
        });
    }

    /** */
    @Test
    public void testGetEntriesAsyncAllowed() {
        performAction(cache -> {
            for (Object o : cache.getEntriesAsync(kvMap.keySet()).get()) {
                CacheEntry<Integer, Integer> entry = (CacheEntry<Integer, Integer>)o;

                assertEquals(kvMap.get(entry.getKey()), entry.getValue());
            }
        });
    }

    /** */
    @Test
    public void testGetAllOutTxAllowed() {
        performAction(
            (node, cache) -> {
                for (TransactionConcurrency level : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

                        if (level == OPTIMISTIC && cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
                            // Only pessimistic transactions are supported when MVCC is enabled.
                            continue;
                        }

                        Transaction tx = node.transactions().txStart(level, isolation);

                        try {
                            cache.get(UNKNOWN_KEY);

                            assertEquals(kvMap, cache.getAllOutTx(kvMap.keySet()));

                            tx.commit();
                        }
                        catch (Exception e) {
                            RuntimeException ex = new RuntimeException(new AssertionError(
                                "Got exception on node: " + node.name() + " cache: " + cache.getName() + " isolation: " + isolation + " txLevel: " + level,
                                e
                            ));

                            log.error("", ex);

                            tx.rollback();

                            throw ex;
                        }
                    }
                }
            },
            TX_CACHES_PRED.or(MVCC_CACHES_PRED)
        );
    }

    /** */
    @Test
    public void testGetAllOutTxAsyncAllowed() {
        performAction(
            (node, cache) -> {
                for (TransactionConcurrency level : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

                        if (level == OPTIMISTIC && cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
                            // Only pessimistic transactions are supported when MVCC is enabled.
                            continue;
                        }

                        Transaction tx = node.transactions().txStart(level, isolation);

                        try {
                            cache.get(UNKNOWN_KEY);

                            assertEquals(kvMap, cache.getAllOutTxAsync(kvMap.keySet()).get());

                            tx.commit();
                        }
                        catch (Exception e) {
                            RuntimeException ex = new RuntimeException(new AssertionError(
                                "Got exception on node: " + node.name() + " cache: " + cache.getName() + " isolation: " + isolation + " txLevel: " + level,
                                e
                            ));

                            log.error("", ex);

                            tx.rollback();

                            throw ex;
                        }
                    }
                }
            },
            TX_CACHES_PRED.or(MVCC_CACHES_PRED)
        );
    }

    /** */
    @Test
    public void testContainsKeyAllowed() {
        performAction(cache -> assertTrue(KEY + "", cache.containsKey(KEY)));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKey(UNKNOWN_KEY)));
    }

    /** */
    @Test
    public void testContainsKeyAsyncAllowed() {
        performAction(cache -> assertTrue(KEY + "", cache.containsKeyAsync(KEY).get()));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKeyAsync(UNKNOWN_KEY).get()));
    }

    /** */
    @Test
    public void testContainsKeysAllowed() {
        performAction(cache -> assertTrue(valueOf(kvMap.keySet()), cache.containsKeys(kvMap.keySet())));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKeys(singleton(UNKNOWN_KEY))));
    }

    /** */
    @Test
    public void testContainsKeysAsyncAllowed() {
        performAction(cache -> assertTrue(valueOf(kvMap.keySet()), cache.containsKeysAsync(kvMap.keySet()).get()));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKeysAsync(singleton(UNKNOWN_KEY)).get()));
    }

    /** */
    @Test
    public void testPutDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.put(KEY, VAL + 1));
        performActionReadOnlyExceptionExpected(cache -> cache.put(UNKNOWN_KEY, 777));
    }

    /** */
    @Test
    public void testPutAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.putAsync(KEY, VAL + 1).get());
        performActionReadOnlyExceptionExpected(cache -> cache.putAsync(UNKNOWN_KEY, 777).get());
    }

    /** */
    @Test
    public void testGetAndPutDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPut(KEY, VAL + 1));
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPut(UNKNOWN_KEY, 777));
    }

    /** */
    @Test
    public void testGetAndPutAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutAsync(KEY, VAL + 1).get());
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutAsync(UNKNOWN_KEY, 777).get());
    }

    /** */
    @Test
    public void testPutAllDenied() {
        Map<Integer, Integer> newMap = kvMap.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue() + 1));

        performActionReadOnlyExceptionExpected(cache -> cache.putAll(newMap));
    }

    /** */
    @Test
    public void testPutAllAsyncDenied() {
        Map<Integer, Integer> newMap = kvMap.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue() + 1));

        performActionReadOnlyExceptionExpected(cache -> cache.putAllAsync(newMap).get());
    }

    /** */
    @Test
    public void testPutIfAbsentDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsent(KEY, VAL + 1));
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsent(UNKNOWN_KEY, 777));
    }

    /** */
    @Test
    public void testPutIfAbsentAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsentAsync(KEY, VAL + 1).get());
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsentAsync(UNKNOWN_KEY, 777).get());
    }

    /** */
    @Test
    public void testRemoveDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.remove(KEY));
        performActionReadOnlyExceptionExpected(cache -> cache.remove(UNKNOWN_KEY));
    }

    /** */
    @Test
    public void testRemoveAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.removeAsync(KEY).get());
        performActionReadOnlyExceptionExpected(cache -> cache.removeAsync(UNKNOWN_KEY).get());
    }

    /** */
    @Test
    public void testRemoveWithOldValueDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.remove(KEY, VAL));
    }

    /** */
    @Test
    public void testRemoveWithOldValueAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.removeAsync(KEY, VAL).get());
    }

    /** */
    @Test
    public void testGetAndRemoveDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndRemove(KEY));
        performActionReadOnlyExceptionExpected(cache -> cache.getAndRemove(UNKNOWN_KEY));
    }

    /** */
    @Test
    public void testGetAndRemoveAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndRemoveAsync(KEY).get());
        performActionReadOnlyExceptionExpected(cache -> cache.getAndRemoveAsync(UNKNOWN_KEY).get());
    }

    /** */
    @Test
    public void testReplaceWithOldValueDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.replace(KEY, VAL, VAL + 1));
    }

    /** */
    @Test
    public void testReplaceWithOldValueAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.replaceAsync(KEY, VAL, VAL + 1).get());
    }

    /** */
    @Test
    public void testReplaceDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.replace(KEY,VAL + 1));
    }

    /** */
    @Test
    public void testReplaceAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.replaceAsync(KEY, VAL + 1).get());
    }

    /** */
    @Test
    public void testGetAndReplaceDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndReplace(KEY,VAL + 1));
    }

    /** */
    @Test
    public void testGetAndReplaceAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndReplaceAsync(KEY, VAL + 1).get());
    }

    /** */
    @Test
    public void testRemoveAllWithKeysDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.removeAll(kvMap.keySet()));
    }

    /** */
    @Test
    public void testRemoveAllWithKeysAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.removeAllAsync(kvMap.keySet()).get());
    }

    /** */
    @Test
    public void testRemoveAllDenied() {
        performActionReadOnlyExceptionExpected(IgniteCache::removeAll);
    }

    /** */
    @Test
    public void testRemoveAllAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.removeAllAsync().get());
    }

    /** */
    @Test
    public void testClearDenied() {
        performActionReadOnlyExceptionExpected(IgniteCache::clear, NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testClearAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.clearAsync().get(), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testClearWithKeyDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.clear(KEY), NO_MVCC_CACHES_PRED);
        performActionReadOnlyExceptionExpected(cache -> cache.clear(UNKNOWN_KEY), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testClearWithKeyAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.clearAsync(KEY).get(), NO_MVCC_CACHES_PRED);
        performActionReadOnlyExceptionExpected(cache -> cache.clearAsync(UNKNOWN_KEY).get(), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testClearAllDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.clearAll(kvMap.keySet()), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testClearAllAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.clearAllAsync(kvMap.keySet()).get(), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testLocalClearDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.localClear(KEY), NO_MVCC_CACHES_PRED);
        performActionReadOnlyExceptionExpected(cache -> cache.localClear(UNKNOWN_KEY), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testLocalClearAllDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.localClearAll(kvMap.keySet()), NO_MVCC_CACHES_PRED);
    }

    /** */
    @Test
    public void testCloseAllowed() {
        performAction((node, cache) -> {
            assertFalse(cache.isClosed());

            cache.close();

            assertTrue(cache.isClosed());
        });
    }
}
