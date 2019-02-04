/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.PA;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 */
public class IgniteCachePartitionedBackupNodeFailureRecoveryTest extends IgniteCacheAbstractTest {
  /** {@inheritDoc}*/
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc}*/
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc}*/
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc}*/
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc}*/
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setRebalanceMode(SYNC);

        return ccfg;
    }

    /**
     * Test stops and restarts backup node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBackUpFail() throws Exception {
        final IgniteEx node1 = grid(0);
        final IgniteEx node2 = grid(1);
        final IgniteEx node3 = grid(2);

        awaitPartitionMapExchange();

        final IgniteCache<Integer, Integer> cache1 = node1.cache(DEFAULT_CACHE_NAME);

        Affinity<Integer> aff = node1.affinity(DEFAULT_CACHE_NAME);

        Integer key0 = null;

        for (int key = 0; key < 10_000; key++) {
            if (aff.isPrimary(node2.cluster().localNode(), key) && aff.isBackup(node3.cluster().localNode(), key)) {
                key0 = key;

                break;
            }
        }

        assertNotNull(key0);

        cache1.put(key0, 0);

        final AtomicBoolean finished = new AtomicBoolean();

        final ReentrantLock lock = new ReentrantLock();

        final AtomicInteger cntr = new AtomicInteger();

        final Integer finalKey = key0;

        IgniteInternalFuture<Void> primaryFut;
        IgniteInternalFuture<Void> backupFut;

        try {
            primaryFut = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!finished.get()) {
                        lock.lock();

                        try {
                            cache1.invoke(finalKey, new TestEntryProcessor());

                            cntr.getAndIncrement();
                        }
                        finally {
                            lock.unlock();
                        }
                    }

                    return null;
                }
            });

            backupFut = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!finished.get()) {
                        stopGrid(2);

                        IgniteEx backUp = startGrid(2);

                        final IgniteCache<Integer, Integer> cache3 = backUp.cache(DEFAULT_CACHE_NAME);

                        lock.lock();

                        try {
                            boolean res = waitForCondition(new PA() {
                                @Override public boolean apply() {
                                    Integer actl = cache3.localPeek(finalKey);

                                    Integer exp = cntr.get();

                                    return exp.equals(actl);
                                }
                            }, 1000);

                            assertTrue(res);
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                    return null;
                }
            });

            Thread.sleep(30_000);
        }
        finally {
            finished.set(true);
        }

        primaryFut.get();
        backupFut.get();
    }

    /**
     *
     */
    static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Void> {
        /** {@inheritDoc}*/
        @Override public Void process(MutableEntry<Integer, Integer> entry, Object... args) {
            Integer v = entry.getValue() + 1;

            entry.setValue(v);

            return null;
        }
    }
}
