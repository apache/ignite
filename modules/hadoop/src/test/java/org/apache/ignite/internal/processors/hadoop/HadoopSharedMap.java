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

package org.apache.ignite.internal.processors.hadoop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * For tests.
 */
public class HadoopSharedMap {
    /** */
    private static final ConcurrentMap<String, HadoopSharedMap> maps = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<String, Object> map = new ConcurrentHashMap<>();

    /**
     * Private.
     */
    private HadoopSharedMap() {
        // No-op.
    }

    /**
     * Puts object by key.
     *
     * @param key Key.
     * @param val Value.
     */
    @SuppressWarnings("unchecked")
    public <T> T put(String key, T val) {
        Object old = map.putIfAbsent(key, val);

        return old == null ? val : (T)old;
    }

    /**
     * @param cls Class.
     * @return Map of static fields.
     */
    public static HadoopSharedMap map(Class<?> cls) {
        HadoopSharedMap m = maps.get(cls.getName());

        if (m != null)
            return m;

        HadoopSharedMap old = maps.putIfAbsent(cls.getName(), m = new HadoopSharedMap());

        return old == null ? m : old;
    }
}