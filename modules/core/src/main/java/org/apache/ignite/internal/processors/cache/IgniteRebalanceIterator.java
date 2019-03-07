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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;

/**
 * Iterator over supplied data for rebalancing.
 */
public interface IgniteRebalanceIterator extends GridCloseableIterator<CacheDataRow> {
    /**
     * @return {@code True} if this iterator is a historical iterator starting from the requested partition counter.
     */
    public boolean historical(int partId);

    /**
     * @param partId Partition ID.
     * @return {@code True} if all data for given partition was already returned.
     */
    public boolean isPartitionDone(int partId);

    /**
     * @param partId Partition ID.
     * @return {@code True} if partition was marked as missing.
     */
    public boolean isPartitionMissing(int partId);

    /**
     * Marks partition as missing.
     *
     * @param partId Partition ID.
     */
    public void setPartitionMissing(int partId);

    /**
     * Return next element without moving iterator cursor to the next one.
     * @return Next element or {@code Null} if there is no more elements.
     */
    public CacheDataRow peek();
}
