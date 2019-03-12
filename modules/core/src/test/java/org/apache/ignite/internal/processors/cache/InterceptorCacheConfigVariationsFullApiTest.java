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

import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Full API cache test.
 */
@SuppressWarnings({"unchecked"})
public class InterceptorCacheConfigVariationsFullApiTest extends IgniteCacheConfigVariationsFullApiTest {
    /** */
    private static volatile boolean validate = true;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = super.cacheConfiguration();

        cc.setInterceptor(new TestInterceptor());

        return cc;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testTtlNoTx() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testTtlNoTxOldEntry() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testTtlTx() throws Exception {
        // No-op.
    }

    /**
     *
     */
    private static class TestInterceptor<K, V> implements CacheInterceptor<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public V onGet(K key, V val) {
            if (validate && val != null)
                assertFalse("Val: " + val, val instanceof BinaryObject);

            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V onBeforePut(Cache.Entry<K, V> e, V newVal) {
            if (validate) {
                validateEntry(e);

                if (newVal != null)
                    assertFalse("NewVal: " + newVal, newVal instanceof BinaryObject);
            }

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<K, V> entry) {
            validateEntry(entry);
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry) {
            validateEntry(entry);

            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<K, V> entry) {
            validateEntry(entry);
        }

        /**
         * @param e Value.
         */
        private void validateEntry(Cache.Entry<K, V> e) {
            assertNotNull(e);
            assertNotNull(e.getKey());

            if (validate) {
                assertFalse("Key: " + e.getKey(), e.getKey() instanceof BinaryObject);

                if (e.getValue() != null)
                    assertFalse("Val: " + e.getValue(), e.getValue() instanceof BinaryObject);
            }
        }
    }
}
