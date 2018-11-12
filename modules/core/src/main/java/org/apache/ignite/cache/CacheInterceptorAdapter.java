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
    @Override public @Nullable V onGet(K key, V val) {
        return val;
    }

    /** {@inheritDoc} */
    @Override public @Nullable V onBeforePut(Cache.Entry<K, V> entry, V newVal) {
        return newVal;
    }

    /** {@inheritDoc} */
    @Override public void onAfterPut(Cache.Entry<K, V> entry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry) {
        return new IgniteBiTuple<>(false, entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void onAfterRemove(Cache.Entry<K, V> entry) {
        // No-op.
    }
}