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

package org.apache.ignite.internal.processors.cache.binary;

import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 * Tests that an error thrown on a cache update in a transaction, that follows an {@code EntryProcessor}
 * invocation for the same key, is not masked by a key deserialization failure when the key is a binary
 * object without a Java class.
 */
public class IgniteBinaryTxPutAfterInvokeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Ensures that a {@code put()} after an {@code invoke()} for a binary key, that has no Java class,
     * fails with the "cannot have update value" error instead of the binary key deserialization error.
     */
    @Test
    public void testPutAfterInvokeWithBinaryKeyWithoutJavaClass() throws Exception {
        Ignite ignite = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

        try {
            BinaryObjectBuilder keyBuilder = ignite.binary().builder("org.apache.ignite.nonexistent.TestKeyType");

            keyBuilder.setField("id", 42);

            BinaryObject key = keyBuilder.build();

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.withKeepBinary().invoke(key, new SetValueEntryProcessor<>(1));

                cache.withKeepBinary().put(key, 2);

                fail("Update in transaction after EntryProcessor must fail");
            }
            catch (CacheException e) {
                Throwable root = e;

                while (root.getCause() != null)
                    root = root.getCause();

                assertTrue("Unexpected root cause: " + root,
                    root.getMessage().contains("cannot have update value in transaction after EntryProcessor is applied"));
            }
        }
        finally {
            ignite.destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /** */
    private static class SetValueEntryProcessor<K, V> implements EntryProcessor<K, V, Void> {
        /** */
        private final V val;

        /** */
        private SetValueEntryProcessor(V val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<K, V> entry, Object... args) throws EntryProcessorException {
            entry.setValue(val);

            return null;
        }
    }
}
