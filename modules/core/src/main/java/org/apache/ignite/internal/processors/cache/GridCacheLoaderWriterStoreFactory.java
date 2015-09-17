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

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.cache.store.CacheStore;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class GridCacheLoaderWriterStoreFactory<K, V> implements Factory<CacheStore<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Factory<CacheLoader<K, V>> ldrFactory;

    /** */
    private final Factory<CacheWriter<K, V>> writerFactory;

    /**
     * @param ldrFactory Loader factory.
     * @param writerFactory Writer factory.
     */
    GridCacheLoaderWriterStoreFactory(@Nullable Factory<CacheLoader<K, V>> ldrFactory,
        @Nullable Factory<CacheWriter<K, V>> writerFactory) {
        this.ldrFactory = ldrFactory;
        this.writerFactory = writerFactory;

        assert ldrFactory != null || writerFactory != null;
    }

    /** {@inheritDoc} */
    @Override public CacheStore<K, V> create() {
        CacheLoader<K, V> ldr = ldrFactory == null ? null : ldrFactory.create();

        CacheWriter<K, V> writer = writerFactory == null ? null : writerFactory.create();

        return new GridCacheLoaderWriterStore<>(ldr, writer);
    }

    /**
     * @return Loader factory.
     */
    Factory<CacheLoader<K, V>> loaderFactory() {
        return ldrFactory;
    }

    /**
     * @return Writer factory.
     */
    Factory<CacheWriter<K, V>> writerFactory() {
        return writerFactory;
    }
}
