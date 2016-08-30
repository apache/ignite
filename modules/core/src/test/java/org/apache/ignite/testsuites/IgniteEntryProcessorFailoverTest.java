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

package org.apache.ignite.testsuites;

import java.util.BitSet;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridRollingRestartAbstractTest;

/**
 * Test that asserts that {@link CacheEntryProcessor} invocations always
 * occur when partitions are in flight.
 */
public class IgniteEntryProcessorFailoverTest extends GridRollingRestartAbstractTest {

    /** Test cache name */
    public static final String CACHE_NAME = "testCache";

    /** Target key for entry processor. */
    public static final int KEY = 1;

    /** Number of entry processor invocations */
    public static final int INVOCATION_COUNT = 200000;

    /** {@inheritDoc} */
    @Override public int serverCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRestarts() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, BitSet> getCacheConfiguration() {
        return new CacheConfiguration<Integer, BitSet>(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1);
    }

    public void testRun() throws InterruptedException {
        IgniteCache<Integer, BitSet> cache = ignite(0).getOrCreateCache(CACHE_NAME);
        cache.put(KEY, new BitSet(INVOCATION_COUNT));

        assertTrue(cache.get(KEY).nextClearBit(0) == 0);

        Thread even = new Thread(new Client(true));
        Thread odd = new Thread(new Client(false));

        even.start();
        odd.start();

        even.join();
        odd.join();

        BitSet bitSet = cache.get(KEY);
        int clearBit = bitSet.nextClearBit(0);

        // if a clear bit is found, this indicates that an entry processor invocation was lost
        assertTrue("Unexpected clear bit: " + clearBit, clearBit == INVOCATION_COUNT);
        assertTrue(rollingRestartThread.getRestartTotal() > 5);
    }

    /**
     * {@link Runnable} that sends {@link CacheEntryProcessor} invocations.
     */
    class Client implements Runnable {

        /**
         * If true, invoke processor with even parameters; otherwise invoke with odd.
         */
        private final boolean even;

        public Client(boolean even) {
            this.even = even;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache<Integer, BitSet> cache = ignite(0).getOrCreateCache(CACHE_NAME);
            BitSetProcessor processor = new BitSetProcessor();
            for (int i = 0; i < INVOCATION_COUNT; i++) {
                if (this.even && i % 2 == 0 || !this.even && i % 2 == 1)
                    cache.invoke(KEY, processor, i);
                }
            }
        }

    public static final class BitSetProcessor implements CacheEntryProcessor<Integer, BitSet, Boolean> {

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Integer, BitSet> entry, Object... arguments) throws EntryProcessorException {
            int i = (int) arguments[0];
            BitSet bitSet = entry.getValue();
            bitSet.set(i);
            entry.setValue(bitSet);
            return true;
        }
    }
}
