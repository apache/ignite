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

package org.apache.ignite.internal.processors.security.impl;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;

/** */
public class TestStoreFactory implements Factory<TestStoreFactory.TestCacheStore> {
    /** */
    private final T2<Object, Object> keyVal;

    /** */
    public TestStoreFactory(Object key, Object val) {
        keyVal = new T2<>(key, val);
    }

    /** {@inheritDoc} */
    @Override public TestCacheStore create() {
        return new TestCacheStore();
    }

    /** */
    class TestCacheStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            clo.apply(keyVal.getKey(), keyVal.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }

}
