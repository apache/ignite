/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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