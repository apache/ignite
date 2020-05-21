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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@SuppressWarnings("unchecked")
public class InterceptorWithKeepBinaryCacheFullApiTest extends WithKeepBinaryCacheFullApiTest {
    /** */
    private static volatile boolean validate;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(IgniteConfiguration cfg, String cacheName) {
        CacheConfiguration cc = super.cacheConfiguration(cfg, cacheName);

        cc.setInterceptor(new TestInterceptor());

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        validate = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        validate = false;

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    private static class TestInterceptor<K, V> implements CacheInterceptor<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public V onGet(K key, V val) {
            // TODO IGNITE-2973: should always validate key here, but cannot due to the bug.
            validate(key, val, false, true);

            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V onBeforePut(Cache.Entry<K, V> e, V newVal) {
            if (validate) {
                validate(e.getKey(), e.getValue(), true, true);

                if (newVal != null)
                    assertEquals("NewVal: " + newVal, interceptorBinaryObjExp, newVal instanceof BinaryObject);
            }

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<K, V> entry) {
            validate(entry.getKey(), entry.getValue(), true, false);
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry) {
            validate(entry.getKey(), entry.getValue(), true, true);

            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<K, V> entry) {
            validate(entry.getKey(), entry.getValue(), true, true);
        }

        /**
         * @param key Key.
         * @param val Value.
         * @param validateKey Validate key flag.
         * @param validateVal Validate value flag.
         */
        private void validate(K key, V val, boolean validateKey, boolean validateVal) {
            assertNotNull(key);

            if (validate) {
                if (validateKey)
                    assertTrue("Key: " + key, key instanceof BinaryObject);

                if (val != null) {
                    // TODO IGNITE-2973: should always do this check, but cannot due to the bug.
                    if (validateVal && interceptorBinaryObjExp)
                        assertTrue("Val: " + val, val instanceof BinaryObject);
                }
            }
        }
    }
}
