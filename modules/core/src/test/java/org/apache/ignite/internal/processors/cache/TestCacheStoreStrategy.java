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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Interface for cache store backend manipulation and stats routines.
 */
public interface TestCacheStoreStrategy {
    /**
     * @return Number of reads to store.
     */
    public int getReads();

    /**
     * @return Number of writes to store.
     */
    public int getWrites();

    /**
     * @return Number of removals from store.
     */
    public int getRemoves();

    /**
     * @return Total number of items in the store.
     */
    public int getStoreSize();

    /**
     * Clear store contents.
     */
    public void resetStore();

    /**
     * Put entry to cache store.
     *
     * @param key Key.
     * @param val Value.
     */
    public void putToStore(Object key, Object val);

    /**
     * @param data Items to put to store.
     */
    public void putAllToStore(Map<?, ?> data);

    /**
     * @param key Key to look for.
     * @return {@link Object} pointed to by given key or {@code null} if no object is present.
     */
    public Object getFromStore(Object key);

    /**
     * @param key to look for
     */
    public void removeFromStore(Object key);

    /**
     * @param key to look for.
     * @return {@code True} if object pointed to by key is in store, false otherwise.
     */
    public boolean isInStore(Object key);

    /**
     * Called from {@link GridCacheAbstractSelfTest#cacheConfiguration(String)},
     * this method allows implementations to tune cache config.
     *
     * @param cfg {@link CacheConfiguration} to tune.
     */
    public void updateCacheConfiguration(CacheConfiguration<Object, Object> cfg);

    /**
     * @return {@link Factory} for write-through storage emulator.
     */
    public Factory<? extends CacheStore<Object, Object>> getStoreFactory();
}
