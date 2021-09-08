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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collections;
import com.google.common.collect.ImmutableMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class MvccUnsupportedTxModesTest extends GridCommonAbstractTest {
    /** */
    private static IgniteCache<Object, Object> cache;

    /** */
    private static final CacheEntryProcessor<Object, Object, Object> testEntryProcessor = (entry, arguments) -> null;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx ign = startGrid(0);

        cache = ign.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsent() {
        checkOperation(() -> cache.getAndPutIfAbsent(1, 1));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentAsync() {
        checkOperation(() -> cache.getAndPutIfAbsentAsync(1, 1));
    }

    /** */
    @Test
    public void testGet() {
        checkOperation(() -> cache.get(1));
    }

    /** */
    @Test
    public void testGetAsync() {
        checkOperation(() -> cache.getAsync(1));
    }

    /** */
    @Test
    public void testGetEntry() {
        checkOperation(() -> cache.getEntry(1));
    }

    /** */
    @Test
    public void testGetEntryAsync() {
        checkOperation(() -> cache.getEntryAsync(1));
    }

    /** */
    @Test
    public void testGetAll() {
        checkOperation(() -> cache.getAll(singleton(1)));
    }

    /** */
    @Test
    public void testGetAllAsync() {
        checkOperation(() -> cache.getAllAsync(singleton(1)));
    }

    /** */
    @Test
    public void testGetEntries() {
        checkOperation(() -> cache.getEntries(singleton(1)));
    }

    /** */
    @Test
    public void testGetEntriesAsync() {
        checkOperation(() -> cache.getEntriesAsync(singleton(1)));
    }

    /** */
    @Test
    public void testContainsKey() {
        checkOperation(() -> cache.containsKey(1));
    }

    /** */
    @Test
    public void testContainsKeyAsync() {
        checkOperation(() -> cache.containsKeyAsync(1));
    }

    /** */
    @Test
    public void testContainsKeys() {
        checkOperation(() -> cache.containsKeys(singleton(1)));
    }

    /** */
    @Test
    public void testContainsKeysAsync() {
        checkOperation(() -> cache.containsKeysAsync(singleton(1)));
    }

    /** */
    @Test
    public void testPut() {
        checkOperation(() -> cache.put(1, 1));
    }

    /** */
    @Test
    public void testPutAsync() {
        checkOperation(() -> cache.putAsync(1, 1));
    }

    /** */
    @Test
    public void testGetAndPut() {
        checkOperation(() -> cache.getAndPut(1, 1));
    }

    /** */
    @Test
    public void testGetAndPutAsync() {
        checkOperation(() -> cache.getAndPutAsync(1, 1));
    }

    /** */
    @Test
    public void testPutAll() {
        checkOperation(() -> cache.putAll(ImmutableMap.of(1, 1)));
    }

    /** */
    @Test
    public void testPutAllAsync() {
        checkOperation(() -> cache.putAllAsync(ImmutableMap.of(1, 1)));
    }

    /** */
    @Test
    public void testPutIfAbsent() {
        checkOperation(() -> cache.putIfAbsent(1, 1));
    }

    /** */
    @Test
    public void testPutIfAbsentAsync() {
        checkOperation(() -> cache.putIfAbsentAsync(1, 1));
    }

    /** */
    @Test
    public void testRemove1() {
        checkOperation(() -> cache.remove(1));
    }

    /** */
    @Test
    public void testRemoveAsync1() {
        checkOperation(() -> cache.removeAsync(1));
    }

    /** */
    @Test
    public void testRemove2() {
        checkOperation(() -> cache.remove(1, 1));
    }

    /** */
    @Test
    public void testRemoveAsync2() {
        checkOperation(() -> cache.removeAsync(1, 1));
    }

    /** */
    @Test
    public void testGetAndRemove() {
        checkOperation(() -> cache.getAndRemove(1));
    }

    /** */
    @Test
    public void testGetAndRemoveAsync() {
        checkOperation(() -> cache.getAndRemoveAsync(1));
    }

    /** */
    @Test
    public void testReplace1() {
        checkOperation(() -> cache.replace(1, 1, 1));
    }

    /** */
    @Test
    public void testReplaceAsync1() {
        checkOperation(() -> cache.replaceAsync(1, 1, 1));
    }

    /** */
    @Test
    public void testReplace2() {
        checkOperation(() -> cache.replace(1, 1));
    }

    /** */
    @Test
    public void testReplaceAsync2() {
        checkOperation(() -> cache.replaceAsync(1, 1));
    }

    /** */
    @Test
    public void testGetAndReplace() {
        checkOperation(() -> cache.getAndReplace(1, 1));
    }

    /** */
    @Test
    public void testGetAndReplaceAsync() {
        checkOperation(() -> cache.getAndReplaceAsync(1, 1));
    }

    /** */
    @Test
    public void testRemoveAll1() {
        checkOperation(() -> cache.removeAll(singleton(1)));
    }

    /** */
    @Test
    public void testRemoveAllAsync1() {
        checkOperation(() -> cache.removeAllAsync(singleton(1)));
    }

    /** */
    @Test
    public void testInvoke1() {
        checkOperation(() -> cache.invoke(1, testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAsync1() {
        checkOperation(() -> cache.invokeAsync(1, testEntryProcessor));
    }

    /** */
    @Test
    public void testInvoke2() {
        checkOperation(() -> cache.invoke(1, testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAsync2() {
        checkOperation(() -> cache.invokeAsync(1, testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAll1() {
        checkOperation(() -> cache.invokeAll(singleton(1), testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAllAsync1() {
        checkOperation(() -> cache.invokeAllAsync(singleton(1), testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAll2() {
        checkOperation(() -> cache.invokeAll(singleton(1), testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAllAsync2() {
        checkOperation(() -> cache.invokeAllAsync(singleton(1), testEntryProcessor));
    }

    /** */
    @Test
    public void testInvokeAll3() {
        checkOperation(() -> cache.invokeAll(Collections.singletonMap(1, testEntryProcessor)));
    }

    /** */
    @Test
    public void testInvokeAllAsync3() {
        checkOperation(() -> cache.invokeAllAsync(Collections.singletonMap(1, testEntryProcessor)));
    }

    /**
     * @param action Action.
     */
    private void checkOperation(Runnable action) {
        assertNotSupportedInTx(action, OPTIMISTIC, READ_COMMITTED);
        assertNotSupportedInTx(action, OPTIMISTIC, REPEATABLE_READ);
        assertNotSupportedInTx(action, OPTIMISTIC, SERIALIZABLE);

        assertSupportedInTx(action, PESSIMISTIC, READ_COMMITTED);
        assertSupportedInTx(action, PESSIMISTIC, REPEATABLE_READ);
        assertSupportedInTx(action, PESSIMISTIC, SERIALIZABLE);
    }

    /** */
    private void assertNotSupportedInTx(Runnable action, TransactionConcurrency conc, TransactionIsolation iso) {
        try (Transaction ignored = grid(0).transactions().txStart(conc, iso)) {
            action.run();

            fail("Action failure is expected.");
        }
        catch (TransactionException e) {
            assertEquals("Only pessimistic transactions are supported when MVCC is enabled.", e.getMessage());
        }
    }

    /** */
    private void assertSupportedInTx(Runnable action, TransactionConcurrency conc, TransactionIsolation iso) {
        try (Transaction tx = grid(0).transactions().txStart(conc, iso)) {
            action.run();

            tx.commit();
        }
    }
}
