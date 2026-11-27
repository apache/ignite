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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureHandlerWithCallback;
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

/**
 * Checks that an interrupt during an ATOMIC put whose interceptor returns a value of a new class fails only the put.
 */
public class AtomicInterceptorPutInterruptTest extends GridCommonAbstractTest {
    /** Error passed to the failure handler. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /** While set, holds back the messages that complete the registration of a class name. */
    private volatile CountDownLatch mappingAcceptedLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(new DiscoveryHook() {
            @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
                CountDownLatch latch = mappingAcceptedLatch;

                if (latch != null && customMsg instanceof MappingAcceptedMessage) {
                    // Waits with a time limit. If the put is not interrupted, it waits for this message, and the test
                    // must then fail instead of hanging.
                    try {
                        U.await(latch, 10, TimeUnit.SECONDS);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        // The discovery thread is stopping. U.await() keeps its interrupt flag set.
                    }
                }
            }
        });

        return cfg.setCacheConfiguration(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setInterceptor(new ExternalizableInterceptor()));
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new FailureHandlerWithCallback(ctx -> failure.compareAndSet(null, ctx.error()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Removes the class names registered by earlier runs.
        cleanPersistenceDir();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** The interceptor replaces the new value with an Externalizable value of a new class inside the cache update. */
    @Test
    public void testInterruptedPutWithInterceptorReturningUnregisteredExternalizableClass() {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Holds back the registration of new class names, so the put always has to wait for it and is interrupted
        // there, however fast the discovery is.
        mappingAcceptedLatch = new CountDownLatch(1);

        Thread.currentThread().interrupt();

        try {
            GridTestUtils.assertThrowsWithCause(() -> cache.put(1, 1), IgniteInterruptedCheckedException.class);
        }
        finally {
            // Clears the interrupt flag, so it does not affect the rest of the test.
            Thread.interrupted();

            // Lets the held registration finish.
            mappingAcceptedLatch.countDown();

            mappingAcceptedLatch = null;
        }

        assertNull("Failure handler was called: " + failure.get(), failure.get());

        cache.put(1, 2);

        assertEquals(2, ((ExternalizableValue)cache.get(1)).val);
    }

    /** Externalizable value, so it is written by the optimized marshaller inside the cache update. */
    private static class ExternalizableValue implements Externalizable {
        /** */
        private int val;

        /** Required by {@link Externalizable}. */
        public ExternalizableValue() {
            // No-op.
        }

        /** */
        ExternalizableValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            val = in.readInt();
        }
    }

    /** Replaces the new value with an Externalizable value, so the value is created inside the cache update. */
    private static class ExternalizableInterceptor extends CacheInterceptorAdapter<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object onBeforePut(Cache.Entry<Integer, Object> entry, Object newVal) {
            return new ExternalizableValue((Integer)newVal);
        }
    }
}
