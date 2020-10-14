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

package org.apache.ignite.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;

/**
 * Interface for sorted Ignite indexes.
 */
public interface SortedIndex extends Index {
    /**
     * Find by range (select * from t where val between 1 and 4 and k between 4 and 6).
     * Runs BPlusTree.find.
     */
    public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment) throws IgniteCheckedException;

    /**
     * Find by range (select * from t where val between 1 and 4 and k between 4 and 6).
     * Runs BPlusTree.find.
     */
    public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment, IndexingQueryFilter filter)
        throws IgniteCheckedException;

    /** */
    public GridCursor<IndexSearchRow> findFirstOrLast(boolean firstOrLast, int segment, IndexingQueryFilter filter)
        throws IgniteCheckedException;

    /**
     * @return count of index rows for specified segment.
     */
    public long count(int segment) throws IgniteCheckedException;

    /**
     * @return total count of index rows.
     */
    public long totalCount() throws IgniteCheckedException;

    /**
     * @return count of index rows for specified segment and filter.
     */
    public long count(int segment, IndexingQueryFilter filter) throws IgniteCheckedException;

    /**
     * @return amount of index segmnets.
     */
    public int segmentsCount();
}
