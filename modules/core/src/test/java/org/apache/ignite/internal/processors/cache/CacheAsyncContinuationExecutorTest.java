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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/**
 * Tests {@link IgniteConfiguration#setAsyncContinuationExecutor(Executor)}.
 */
@SuppressWarnings("rawtypes")
public class CacheAsyncContinuationExecutorTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 0;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        // Use cache store with a write delay to make sure future does not complete before we register a listener.
        ccfg.setCacheStoreFactory(new DelayedStoreFactory());
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);

        return ccfg;
    }

    /**
     * Gets the expected thread name prefix.
     * @return Prefix.
     */
    protected String expectedThreadNamePrefix() {
        return "ForkJoinPool.commonPool-worker";
    }

    /**
     * Gets a value indicating whether continuation thread can execute cache operations.
     * @return Value indicating whether continuation thread can execute cache operations.
     */
    protected boolean allowCacheOperationsInContinuation() {
        return true;
    }

    /**
     * Tests future listen with default executor.
     */
    @Test
    public void testRemoteOperationListenContinuesOnDefaultExecutor() throws Exception {
        testRemoteOperationContinuesOnDefaultExecutor(false);
    }

    /**
     * Tests future chain with default executor.
     */
    @Test
    public void testRemoteOperationChainContinuesOnDefaultExecutor() throws Exception {
        testRemoteOperationContinuesOnDefaultExecutor(true);
    }

    /**
     * Tests that an operation on a local key executes synchronously, and listener is called immediately on the current
     * thread.
     */
    @Test
    public void testLocalOperationListenerExecutesSynchronously() {
        final String key = getPrimaryKey(0);

        IgniteCache<String, Integer> cache = jcache(0);
        AtomicReference<String> listenThreadName = new AtomicReference<>("");

        cache.putAsync(key, 1).listen(f -> listenThreadName.set(Thread.currentThread().getName()));

        assertEquals(Thread.currentThread().getName(), listenThreadName.get());
    }

    /**
     * Tests that an operation on a remote key executes on striped pool directly when a syncronous executor is provided.
     * This demonstrates that default safe behavior can be overridden with a faster, but unsafe old behavior
     * for an individual operation.
     */
    @Test
    public void testRemoteOperationListenerExecutesOnStripedPoolWhenCustomExecutorIsProvided() throws Exception {
        final String key = getPrimaryKey(1);

        IgniteCache<String, Integer> cache = jcache(0);
        AtomicReference<String> listenThreadName = new AtomicReference<>("");
        CyclicBarrier barrier = new CyclicBarrier(2);

        cache.putAsync(key, 1).listenAsync(f -> {
            listenThreadName.set(Thread.currentThread().getName());

            try {
                barrier.await(10, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }, Runnable::run);

        barrier.await(10, TimeUnit.SECONDS);

        assertTrue(listenThreadName.get(), listenThreadName.get().startsWith("sys-stripe-"));
    }

    /**
     * Tests that an operation on a local key executes synchronously, and chain is called immediately on the current
     * thread.
     */
    @Test
    public void testLocalOperationChainExecutesSynchronously() {
        final String key = getPrimaryKey(0);

        IgniteCache<String, Integer> cache = jcache(0);
        AtomicReference<String> listenThreadName = new AtomicReference<>("");

        cache.putAsync(key, 1).chain(f -> {
            listenThreadName.set(Thread.currentThread().getName());

            return new IgniteFinishedFutureImpl<>();
        });

        assertEquals(Thread.currentThread().getName(), listenThreadName.get());
    }

    /**
     * Tests future chain / listen with default executor.
     *
     * This test would hang before {@link IgniteConfiguration#setAsyncContinuationExecutor(Executor)}
     * was introduced, or if we set {@link Runnable#run()} as the executor.
     */
    private void testRemoteOperationContinuesOnDefaultExecutor(boolean chain) throws Exception {
        final String key = getPrimaryKey(1);

        IgniteCache<String, Integer> cache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<String> listenThreadName = new AtomicReference<>("");

        IgniteInClosure<IgniteFuture<Void>> clos = f -> {
            listenThreadName.set(Thread.currentThread().getName());

            if (allowCacheOperationsInContinuation()) {
                // Check that cache operations do not deadlock.
                cache.replace(key, 2);
            }

            try {
                barrier.await(10, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        };

        IgniteFuture<Void> fut = cache.putAsync(key, 1);

        if (chain)
            fut.chain(f -> {
                clos.apply(f);
                return new IgniteFinishedFutureImpl<>();
            });
        else
            fut.listen(clos);

        barrier.await(10, TimeUnit.SECONDS);

        assertEquals(allowCacheOperationsInContinuation() ? 2 : 1, cache.get(key).intValue());
        assertTrue(listenThreadName.get(), listenThreadName.get().startsWith(expectedThreadNamePrefix()));
    }

    /**
     * Gets the primary key.
     * @param nodeIdx Node index.
     * @return Key.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private String getPrimaryKey(int nodeIdx) {
        return IntStream.range(0, 1000)
                .mapToObj(String::valueOf)
                .filter(x -> belongs(x, nodeIdx))
                .findFirst()
                .get();
    }

    /** */
    private static class DelayedStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new CacheStoreAdapter() {
                /** {@inheritDoc} */
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public void write(Cache.Entry entry) {
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                /** {@inheritDoc} */
                @Override public void delete(Object key) {
                    // No-op.
                }
            };
        }
    }
}
