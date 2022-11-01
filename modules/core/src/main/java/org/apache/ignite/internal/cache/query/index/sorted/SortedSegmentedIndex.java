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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for sorted and segmented Ignite indexes.
 */
public interface SortedSegmentedIndex extends Index {
    /**
     * Finds index rows by specified range in specifed tree segment with cache filtering. Range can be bound or unbound.
     *
     * @param lower Nullable lower bound.
     * @param upper Nullable upper bound.
     * @param lowerIncl {@code true} for inclusive lower bound, otherwise {@code false}.
     * @param upperIncl {@code true} for inclusive upper bound, otherwise {@code false}.
     * @param segment Number of tree segment to find.
     * @param qryCtx External index query context.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> find(
        @Nullable IndexRow lower,
        @Nullable IndexRow upper,
        boolean lowerIncl,
        boolean upperIncl,
        int segment,
        IndexQueryContext qryCtx
    ) throws IgniteCheckedException;

    /**
     * Finds index rows by specified range in all tree segments with cache filtering. Range can be bound or unbound.
     *
     * @param lower Nullable lower bound.
     * @param upper Nullable upper bound.
     * @param lowerIncl {@code true} for inclusive lower bound, otherwise {@code false}.
     * @param upperIncl {@code true} for inclusive upper bound, otherwise {@code false}.
     * @param qryCtx External index query context.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> find(
        @Nullable IndexRow lower,
        @Nullable IndexRow upper,
        boolean lowerIncl,
        boolean upperIncl,
        IndexQueryContext qryCtx
    ) throws IgniteCheckedException;

    /**
     * Finds first index row for specified tree segment and cache filter.
     *
     * @param segment Number of tree segment to find.
     * @param qryCtx External index qyery context.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> findFirst(int segment, IndexQueryContext qryCtx)
        throws IgniteCheckedException;

    /**
     * Finds last index row for specified tree segment and cache filter.
     *
     * @param segment Number of tree segment to find.
     * @param qryCtx External index qyery context.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> findLast(int segment, IndexQueryContext qryCtx)
        throws IgniteCheckedException;

    /**
     * Takes only one first or last index record.
     *
     * @param qryCtx External index qyery context.
     * @param first {@code True} to take first index value. {@code False} to take last index value.
     */
    public GridCursor<IndexRow> takeFirstOrLast(IndexQueryContext qryCtx, boolean first) throws IgniteCheckedException;

    /**
     * Counts index rows in specified tree segment.
     *
     * @param segment Number of tree segment to find.
     * @return count of index rows for specified segment.
     */
    public long count(int segment) throws IgniteCheckedException;

    /**
     * Counts index rows in specified tree segment with cache filter.
     *
     * @param segment Number of tree segment to find.
     * @param qryCtx Index query context.
     * @return count of index rows for specified segment.
     */
    public long count(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException;

    /**
     * Counts index rows for all segments.
     *
     * @return total count of index rows.
     */
    public long totalCount() throws IgniteCheckedException;

    /**
     * Returns amount of index tree segments.
     *
     * @return amount of index tree segments.
     */
    public int segmentsCount();
}
