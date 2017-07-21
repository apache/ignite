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

package org.apache.ignite.cache.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheNameResource;
import org.jetbrains.annotations.Nullable;

/**
 * Test store that generates objects on demand.
 */
public class GridGeneratingTestStore implements CacheStore<String, String> {
    /** Number of entries to be generated. */
    private static final int DFLT_GEN_CNT = 100;

    /** */
    @CacheNameResource
    private String cacheName;

    /** {@inheritDoc} */
    @Override public String load(String key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<String, String> clo, @Nullable Object... args) {
        if (args.length > 0) {
            try {
                int cnt = ((Number)args[0]).intValue();
                int postfix = ((Number)args[1]).intValue();

                for (int i = 0; i < cnt; i++)
                    clo.apply("key" + i, "val." + cacheName + "." + postfix);
            }
            catch (Exception e) {
                X.println("Unexpected exception in loadAll: " + e);

                throw new CacheLoaderException(e);
            }
        }
        else {
            for (int i = 0; i < DFLT_GEN_CNT; i++)
                clo.apply("key" + i, "val." + cacheName + "." + i);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> loadAll(Iterable<? extends String> keys) {
        Map<String, String> loaded = new HashMap<>();

        for (String key : keys)
            loaded.put(key, "val" + key);

        return loaded;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends String, ? extends String> entry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends String, ? extends String>> entries) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        // No-op.
    }
}