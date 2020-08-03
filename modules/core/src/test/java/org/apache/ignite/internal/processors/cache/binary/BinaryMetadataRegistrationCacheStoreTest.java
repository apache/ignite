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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiInClosure;

/**
 * Tests for discovery message exchange, that is performed upon binary type
 * registration when using Cache Store API.
 */
public class BinaryMetadataRegistrationCacheStoreTest extends AbstractBinaryMetadataRegistrationTest {
    /** */
    private static final Map<Integer, Object> GLOBAL_STORE = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void put(IgniteCache<Integer, Object> cache, Integer key, Object val) {
        GLOBAL_STORE.clear();
        GLOBAL_STORE.put(key, val);

        cache.loadCache(null);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Object> cacheConfiguration() {
        return super.cacheConfiguration().setCacheStoreFactory(new TestStoreFactory());
    }

    /** */
    private static class CustomCacheStore implements CacheStore<Integer, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Object> clo, Object... args) throws CacheLoaderException {
            GLOBAL_STORE.forEach(clo::apply);
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Object load(Integer key) throws CacheLoaderException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Object> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ?> entry) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ?>> entries) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }
    }

    /** */
    private static class TestStoreFactory implements Factory<CustomCacheStore> {
        /** {@inheritDoc} */
        @Override public CustomCacheStore create() {
            return new CustomCacheStore();
        }
    }
}
