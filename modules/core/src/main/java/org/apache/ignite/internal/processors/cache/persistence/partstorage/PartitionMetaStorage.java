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

package org.apache.ignite.internal.processors.cache.persistence.partstorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.metric.IoStatisticsHolder;

/**
 * Provides a way to associate any {@link Storable} implementation as partition metadata.
 */
public interface PartitionMetaStorage<T extends Storable> {
    /**
     * Read row data by link as byte array.
     * @param link Link.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] readRow(long link) throws IgniteCheckedException;

    /**
     * @param row Row.
     * @param statHolder Stat holder.
     */
    public void insertDataRow(T row, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * Saves storage metadata.
     */
    public void saveMetadata() throws IgniteCheckedException;
}
