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

package org.apache.ignite.cache.eviction;

import java.io.Serializable;
import javax.cache.Cache;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Eviction filter to specify which entries should not be evicted. Not applicable when
 * calling explicit evict via {@link EvictableEntry#evict()}.
 * If {@link #evictAllowed(Cache.Entry)} method returns {@code false} then eviction
 * policy will not be notified and entry will never be evicted.
 * <p>
 * Eviction filter can be configured via {@link CacheConfiguration#getEvictionFilter()}
 * configuration property. Default value is {@code null} which means that all
 * cache entries will be tracked by eviction policy.
 */
public interface EvictionFilter<K, V> extends Serializable {
    /**
     * Checks if entry may be evicted from cache.
     *
     * @param entry Cache entry.
     * @return {@code True} if it is allowed to evict this entry.
     */
    public boolean evictAllowed(Cache.Entry<K, V> entry);
}