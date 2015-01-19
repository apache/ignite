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
package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Continuous query handler used when "keepPortable" flag is set and security is enabled.
 */
public class GridCacheContinuousQueryHandlerV4<K, V> extends GridCacheContinuousQueryHandlerV2<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * For {@link Externalizable}.
     */
    public GridCacheContinuousQueryHandlerV4() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     * @param topic Topic for ordered messages.
     * @param cb Local callback.
     * @param filter Filter.
     * @param prjPred Projection predicate.
     * @param internal If {@code true} then query is notified about internal entries updates.
     * @param taskHash Task hash.
     */
    public GridCacheContinuousQueryHandlerV4(@Nullable String cacheName, Object topic,
        IgniteBiPredicate<UUID, Collection<GridCacheContinuousQueryEntry<K, V>>> cb,
        @Nullable IgnitePredicate<GridCacheContinuousQueryEntry<K, V>> filter,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> prjPred, boolean internal, int taskHash) {
        super(cacheName, topic, cb, filter, prjPred, internal, taskHash);
    }

    /** {@inheritDoc} */
    @Override protected boolean keepPortable() {
        return true;
    }
}
