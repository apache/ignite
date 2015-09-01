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

package org.apache.ignite.internal.client;

import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;

/**
 * Simple HashMap based cache store emulation.
 */
public class HashMapStore extends CacheStoreAdapter {
    /** Map for cache store. */
    private final Map<Object, Object> map = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure c, Object... args) {
        for (Map.Entry e : map.entrySet())
            c.apply(e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Override public Object load(Object key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry e) {
        map.put(e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        map.remove(key);
    }
}