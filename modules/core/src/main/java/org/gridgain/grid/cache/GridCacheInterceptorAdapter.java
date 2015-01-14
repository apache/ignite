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

package org.gridgain.grid.cache;

import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

/**
 * Cache interceptor convenience adapter. It provides no-op implementations for all
 * interceptor callbacks.
 */
public class GridCacheInterceptorAdapter<K, V> implements GridCacheInterceptor<K, V> {
    /** {@inheritDoc} */
    @Nullable @Override public V onGet(K key, V val) {
        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V onBeforePut(K key, @Nullable V oldVal, V newVal) {
        return newVal;
    }

    /** {@inheritDoc} */
    @Override public void onAfterPut(K key, V val) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(K key, @Nullable V val) {
        return new IgniteBiTuple<>(false, val);
    }

    /** {@inheritDoc} */
    @Override public void onAfterRemove(K key, V val) {
        // No-op.
    }
}
