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

package org.apache.ignite.cdc.cfgplugin;

import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;

/**
 *
 */
public class DrIdCacheVersionConflictResolver implements CacheVersionConflictResolver {
    /** {@inheritDoc} */
    @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry,
        boolean atomicVerComparator
    ) {
        GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

        if (oldEntry.isStartVersion() || (oldEntry.dataCenterId() > newEntry.dataCenterId()))
            res.useOld();
        else
            res.useNew();

        return res;
    }
}
