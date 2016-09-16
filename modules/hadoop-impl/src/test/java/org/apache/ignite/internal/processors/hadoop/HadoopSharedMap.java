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

package org.apache.ignite.internal.processors.hadoop;

import java.util.concurrent.ConcurrentMap;
import org.jsr166.ConcurrentHashMap8;

/**
 * For tests.
 */
public class HadoopSharedMap {
    /** */
    private static final ConcurrentMap<String, HadoopSharedMap> maps = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<String, Object> map = new ConcurrentHashMap8<>();

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