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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureHandlerWithCallback;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Checks that an interrupt during an ATOMIC update that registers the class of the value fails only the update.
 */
public class AtomicPutInterruptTest extends GridCommonAbstractTest {
    /** Error passed to the failure handler. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(ATOMIC));
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

    /** */
    @Test
    public void testInterruptedPutOfUnregisteredExternalizableClass() throws Exception {
        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Thread.currentThread().interrupt();

        try {
            cache.put(1, new ExternalizableValue(1));
        }
        catch (Exception e) {
            log.info("Put failed as expected: " + e);
        }
        finally {
            // Clears the interrupt flag, so it does not affect the rest of the test.
            Thread.interrupted();
        }

        assertNull("Failure handler was called: " + failure.get(), failure.get());
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
}
