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


package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Checks an ATOMIC update of several keys that has to register a new class after some keys are already written.
 * {@code GridDhtAtomicCache.updateAllAsyncInternal0()} registers the class and retries the update, and here the
 * registration fails because the thread is interrupted.
 */
public class AtomicUpdateRetryInterruptTest extends GridCommonAbstractTest {
    /** While set, holds back the messages that complete the registration of a class name. */
    private volatile CountDownLatch mappingAcceptedLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(new DiscoveryHook() {
            @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
                CountDownLatch latch = mappingAcceptedLatch;

                if (latch != null && customMsg instanceof MappingAcceptedMessage) {
                    // Waits with a time limit. If the update is not interrupted, it waits for this message, and the
                    // test must then fail instead of hanging.
                    try {
                        U.await(latch, 10, TimeUnit.SECONDS);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        // The discovery thread is stopping. U.await() keeps its interrupt flag set.
                    }
                }
            }
        });

        return cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Removes the class names registered by earlier runs.
        cleanPersistenceDir();

        startGrids(2);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * The first key is written, then the entry processor of the second key returns a value of a new class. The retry
     * fails to register that class, so the update fails. The backup must still have the same value of the first key as
     * the primary.
     */
    @Test
    public void testInterruptedRegistrationOnRetryAfterFirstKeyIsWritten() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        List<Integer> keys = primaryKeys(cache, 2);

        Integer firstKey = keys.get(0);

        // Holds back the registration of new class names, so the retry always has to wait for it and is interrupted
        // there.
        mappingAcceptedLatch = new CountDownLatch(1);

        Thread.currentThread().interrupt();

        try {
            // Sorted, so the first key is written before the second one.
            cache.invokeAll(new TreeSet<>(keys), new SetNewClassValueForKey(keys.get(1)));
        }
        catch (Exception e) {
            log.info("The update failed as expected: " + e);
        }
        finally {
            // Clears the interrupt flag, so it does not affect the rest of the test.
            Thread.interrupted();

            // Lets the held registration finish.
            mappingAcceptedLatch.countDown();

            mappingAcceptedLatch = null;
        }

        Object primaryVal = cache.localPeek(firstKey, CachePeekMode.PRIMARY);

        IgniteCache<Integer, Object> backupCache = grid(1).cache(DEFAULT_CACHE_NAME);

        // The backup is updated after the update returns, so this waits for it.
        boolean same = GridTestUtils.waitForCondition(
            () -> Objects.equals(primaryVal, backupCache.localPeek(firstKey, CachePeekMode.BACKUP)), 5_000);

        assertTrue("The backup has a different value of the first key [primary=" + primaryVal +
            ", backup=" + backupCache.localPeek(firstKey, CachePeekMode.BACKUP) + ']', same);
    }

    /** Sets 1 for every key except one, which gets a value of a new class. */
    private static class SetNewClassValueForKey implements CacheEntryProcessor<Integer, Object, Void> {
        /** Key that gets a value of a new class. */
        private final Integer newClassKey;

        /** */
        SetNewClassValueForKey(Integer newClassKey) {
            this.newClassKey = newClassKey;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Object> entry, Object... args) {
            entry.setValue(entry.getKey().equals(newClassKey) ? new NewClassValue(1) : 1);

            return null;
        }
    }

    /** Class that is first seen inside the cache update, so the update has to register it. */
    private static class NewClassValue {
        /** */
        private final int val;

        /** */
        NewClassValue(int val) {
            this.val = val;
        }
    }
}
