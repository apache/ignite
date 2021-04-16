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

package org.apache.ignite.internal.processors.cache.persistence.partstorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.Storable;

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
    public void saveMetadata(IoStatisticsHolder statHolder) throws IgniteCheckedException;
}
