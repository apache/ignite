/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache;

import javax.cache.Cache;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Cache interceptor convenience adapter. It provides no-op implementations for all
 * interceptor callbacks.
 */
public class CacheInterceptorAdapter<K, V> implements CacheInterceptor<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public V onGet(K key, V val) {
        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V onBeforePut(Cache.Entry<K, V> entry, V newVal) {
        return newVal;
    }

    /** {@inheritDoc} */
    @Override public void onAfterPut(Cache.Entry<K, V> entry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry) {
        return new IgniteBiTuple<>(false, entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void onAfterRemove(Cache.Entry<K, V> entry) {
        // No-op.
    }
}