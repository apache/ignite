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

package org.gridgain.client;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.store.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Simple HashMap based cache store emulation.
 */
public class GridHashMapStore extends GridCacheStoreAdapter {
    /** Map for cache store. */
    private final Map<Object, Object> map = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure c, Object... args)
        throws IgniteCheckedException {
        for (Map.Entry e : map.entrySet())
            c.apply(e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Override public Object load(@Nullable IgniteTx tx, Object key)
        throws IgniteCheckedException {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, Object key,
        @Nullable Object val) throws IgniteCheckedException {
        map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable IgniteTx tx, Object key)
        throws IgniteCheckedException {
        map.remove(key);
    }
}
