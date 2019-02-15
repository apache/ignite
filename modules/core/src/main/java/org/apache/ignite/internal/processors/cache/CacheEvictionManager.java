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

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface CacheEvictionManager extends GridCacheManager {
    /**
     * @param txEntry Transactional entry.
     * @param loc Local transaction flag.
     */
    public void touch(IgniteTxEntry txEntry, boolean loc);

    /**
     * @param e Entry for eviction policy notification.
     */
    public void touch(GridCacheEntryEx e);

    /**
     * @param entry Entry to attempt to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Optional entry filter.
     * @param explicit {@code True} if evict is called explicitly, {@code false} if it's called
     *      from eviction policy.
     * @return {@code True} if entry was marked for eviction.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean evict(@Nullable GridCacheEntryEx entry,
        @Nullable GridCacheVersion obsoleteVer,
        boolean explicit,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException;

    /**
     * @param keys Keys to evict.
     * @param obsoleteVer Obsolete version.
     * @throws IgniteCheckedException In case of error.
     */
    public void batchEvict(Collection<?> keys, @Nullable GridCacheVersion obsoleteVer) throws IgniteCheckedException;
}
