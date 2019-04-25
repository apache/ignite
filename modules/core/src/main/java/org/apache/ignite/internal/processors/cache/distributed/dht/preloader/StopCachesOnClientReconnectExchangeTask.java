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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Collection;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class StopCachesOnClientReconnectExchangeTask  extends GridFutureAdapter<Void>
    implements CachePartitionExchangeWorkerTask {
    /** */
    @GridToStringInclude
    private final Collection<GridCacheAdapter> stoppedCaches;

    /**
     * @param stoppedCaches Collection of stopped caches.
     */
    public StopCachesOnClientReconnectExchangeTask(Collection<GridCacheAdapter> stoppedCaches) {
        this.stoppedCaches = stoppedCaches;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return false;
    }

    /**
     * @return Collection of stopped caches.
     */
    public Collection<GridCacheAdapter> stoppedCaches() {
        return stoppedCaches;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StopCachesOnClientReconnectExchangeTask.class, this, super.toString());
    }
}
