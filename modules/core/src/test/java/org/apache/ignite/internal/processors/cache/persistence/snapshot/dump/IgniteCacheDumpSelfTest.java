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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class IgniteCacheDumpSelfTest extends AbstractCacheDumpTest {
    /** */
    @Test
    public void testCacheDump() throws Exception {
        snpPoolSz = 4;

        try {
            IgniteEx ign = startGridAndFillCaches();

            createDump(ign);

            checkDump(ign);

            assertThrows(
                null,
                () -> createDump(ign),
                IgniteException.class,
                "Create snapshot request has been rejected. Snapshot with given name already exists on local node"
            );

            createDump(ign, DMP_NAME + 2);

            checkDump(ign, DMP_NAME + 2);

            if (persistence) {
                assertThrows(
                    null,
                    () -> ign.snapshot().createSnapshot(DMP_NAME).get(),
                    IgniteException.class,
                    "Create snapshot request has been rejected. Snapshot with given name already exists on local node"
                );

                ign.snapshot().createSnapshot(DMP_NAME + 3).get();
            }
            else {
                assertThrows(
                    null,
                    () -> ign.snapshot().createSnapshot(DMP_NAME + 3).get(),
                    IgniteException.class,
                    "Create snapshot request has been rejected. Snapshots on an in-memory clusters are not allowed."
                );
            }
        }
        finally {
            snpPoolSz = 1;
        }
    }

    /** */
    @Test
    public void testWithConcurrentInserts() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = KEYS_CNT; i < KEYS_CNT + 3; i++) {
                assertFalse(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertFalse(ignite.cache(CACHE_0).containsKey(i));
                assertFalse(ignite.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(ignite, i);
            }

            for (int i = KEYS_CNT + 3; i < KEYS_CNT + 6; i++) {
                assertFalse(cli.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertFalse(cli.cache(CACHE_0).containsKey(i));
                assertFalse(cli.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(cli, i);
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentUpdates() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertTrue(ignite.cache(CACHE_0).containsKey(i));
                assertTrue(ignite.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(ignite, i);
            }

            for (int i = 3; i < 6; i++) {
                assertTrue(cli.cache(DEFAULT_CACHE_NAME).containsKey(i));
                assertTrue(cli.cache(CACHE_0).containsKey(i));
                assertTrue(cli.cache(CACHE_1).containsKey(new Key(i)));

                insertOrUpdate(cli, i);
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentRemovals() throws Exception {
        doTestConcurrentOperations(ignite -> {
            for (int i = 0; i < 3; i++)
                remove(ignite, i);

            for (int i = 3; i < 6; i++)
                remove(cli, i);
        });
    }

    /** */
    private void insertOrUpdate(IgniteEx ignite, int i) {
        ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_0);
        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_1);

        IntConsumer moreInserts = j -> {
            cache.put(j, USER_FACTORY.apply(j));
            cache1.put(new Key(j), new Value(String.valueOf(j)));
        };

        if (mode == CacheAtomicityMode.TRANSACTIONAL) {
            try (Transaction tx = ignite.transactions().txStart()) {
                moreInserts.accept(i);

                tx.commit();
            }
        }
        else
            moreInserts.accept(i);
    }

    /** */
    private void remove(IgniteEx ignite, int i) {
        assertTrue(ignite.cache(DEFAULT_CACHE_NAME).containsKey(i));

        ignite.cache(DEFAULT_CACHE_NAME).remove(i);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_0);
        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_1);

        IntConsumer moreRemovals = j -> {
            cache.remove(j);
            cache1.remove(new Key(j));
        };

        if (mode == CacheAtomicityMode.TRANSACTIONAL) {
            try (Transaction tx = ignite.transactions().txStart()) {
                moreRemovals.accept(i);

                tx.commit();
            }
        }
        else
            moreRemovals.accept(i);
    }

    /** */
    private void doTestConcurrentOperations(Consumer<IgniteEx> op) throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart();

        // This operations will be catched by change listeners. Old value must be stored in dump.
        op.accept(ign);

        latchAndFut.get1().countDown();

        latchAndFut.get2().get(10 * 1000);

        checkDump(ign);
    }
}
