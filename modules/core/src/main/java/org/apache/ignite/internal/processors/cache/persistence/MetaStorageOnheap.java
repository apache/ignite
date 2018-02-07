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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;

/**
 * In memory metadata storage implementation.
 */
public class MetaStorageOnheap implements ReadWriteMetastorage {
    /** Map. */
    private final ConcurrentHashMap<String, Serializable> map = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException {
        map.put(key,val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@NotNull String key) throws IgniteCheckedException {
        map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String key) throws IgniteCheckedException {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public Map<String, ? extends Serializable> readForPredicate(IgnitePredicate<String> keyPred) {
        Map<String, Serializable> ret = new HashMap<>();

        for (ConcurrentHashMap.Entry<String, Serializable> e : map.entrySet()) {
            if (keyPred.apply(e.getKey()))
                ret.put(e.getKey(), e.getValue());
        }

        return ret;
    }
}
