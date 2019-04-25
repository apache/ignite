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

import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;

/**
 * Iterator that provides history of updates for a subset of partitions.
 */
public interface IgniteHistoricalIterator extends GridCloseableIterator<CacheDataRow> {
    /**
     * @param partId Partition ID.
     * @return {@code True} if iterator contains data for given partition.
     */
    public boolean contains(int partId);

    /**
     * @param partId Partition ID.
     * @return {@code True} if all data for given partition has already been returned.
     */
    public boolean isDone(int partId);
}
