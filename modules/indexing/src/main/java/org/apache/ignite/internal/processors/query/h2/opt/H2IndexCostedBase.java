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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.h2.engine.Constants;
import org.h2.index.BaseIndex;
import org.h2.index.IndexCondition;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * Index base.
 */
public abstract class H2IndexCostedBase extends BaseIndex {
    /** Const function. */
    private final CostFunction constFunc;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param tbl Table.
     */
    protected H2IndexCostedBase(GridH2Table tbl) {
        this(tbl != null ? tbl.rowDescriptor().tableDescriptor().indexing().kernalContext() : null);
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected H2IndexCostedBase(GridKernalContext ctx) {
        log = ctx != null ? ctx.log("H2Index") : null;

        CostFunctionType costFuncType;

        try {
            costFuncType = CostFunctionType.valueOf(
                IgniteSystemProperties.getString(
                    IgniteSystemProperties.IGNITE_INDEX_COST_FUNCTION,
                    CostFunctionType.LAST.name()));
        }
        catch (IllegalArgumentException e) {
            if (log != null) {
                log.warning("Invalid cost function: "
                    + IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_INDEX_COST_FUNCTION)
                    + ", the LAST cost function is used. Available functions: " + Arrays.toString(CostFunctionType.values()));
            }
            else {
                LT.warn(log, "Invalid cost function: "
                    + IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_INDEX_COST_FUNCTION)
                    + ", the LAST cost function is used. Available functions: " + Arrays.toString(CostFunctionType.values()));
            }

            costFuncType = CostFunctionType.LAST;
        }

        switch (costFuncType) {
            case COMPATIBLE_8_5_17:
                constFunc = this::getCostRangeIndex_8_7_12;

                break;

            case COMPATIBLE_8_5_13:
                constFunc = this::getCostRangeIndex_8_7_6;

                break;

            default:
                constFunc = this::getCostRangeIndex_Last;

                break;
        }
    }

    /**
     * Re-implement {@link BaseIndex#getCostRangeIndex} to dispatch cost function on new and old versions.
     */
    protected long costRangeIndex(int[] masks, long rowCount,
        TableFilter[] filters, int filter, SortOrder sortOrder,
        boolean isScanIndex, HashSet<Column> allColumnsSet) {
        return constFunc.getCostRangeIndex(masks, rowCount, filters, filter, sortOrder, isScanIndex, allColumnsSet);
    }

    /**
     * Re-implement {@link BaseIndex#getCostRangeIndex} to support  compatibility with old version.
     */
    private long getCostRangeIndex_Last(int[] masks, long rowCount,
        TableFilter[] filters, int filter, SortOrder sortOrder,
        boolean isScanIndex, HashSet<Column> allColumnsSet) {
        rowCount += Constants.COST_ROW_OFFSET;

        int totalSelectivity = 0;

        long rowsCost = rowCount;

        if (masks != null) {
            int i = 0, len = columns.length;

            while (i < len) {
                Column column = columns[i++];

                int index = column.getColumnId();
                int mask = masks[index];

                if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                    if (i == len && getIndexType().isUnique()) {
                        rowsCost = 3;

                        break;
                    }

                    totalSelectivity = 100 - ((100 - totalSelectivity) *
                        (100 - column.getSelectivity()) / 100);

                    long distinctRows = rowCount * totalSelectivity / 100;

                    if (distinctRows <= 0)
                        distinctRows = 1;

                    rowsCost = Math.min(5 + Math.max(rowsCost / distinctRows, 1), rowsCost - (i > 0 ? 1 : 0));
                }
                else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                    rowsCost = Math.min(5 + rowsCost / 4, rowsCost - (i > 0 ? 1 : 0));

                    break;
                }
                else if ((mask & IndexCondition.START) == IndexCondition.START) {
                    rowsCost = Math.min(5 + rowsCost / 3, rowsCost - (i > 0 ? 1 : 0));

                    break;
                }
                else if ((mask & IndexCondition.END) == IndexCondition.END) {
                    rowsCost = Math.min(rowsCost / 3, rowsCost - (i > 0 ? 1 : 0));

                    break;
                }
                else
                    break;
            }
        }

        // If the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost
        // accordingly.
        long sortingCost = 0;

        if (sortOrder != null)
            sortingCost = 100 + rowCount / 10;

        if (sortOrder != null && !isScanIndex) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();

            TableFilter tableFilter = filters == null ? null : filters[filter];

            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // We can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns.
                    break;
                }

                Column col = sortOrder.getColumn(i, tableFilter);

                if (col == null) {
                    sortOrderMatches = false;

                    break;
                }

                IndexColumn indexCol = indexColumns[i];

                if (!col.equals(indexCol.column)) {
                    sortOrderMatches = false;

                    break;
                }

                int sortType = sortTypes[i];

                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;

                    break;
                }

                coveringCount++;
            }

            if (sortOrderMatches) {
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more.
                sortingCost = 100 - coveringCount;
            }
        }

        // If we have two indexes with the same cost, and one of the indexes can
        // satisfy the query without needing to read from the primary table
        // (scan index), make that one slightly lower cost.
        boolean needsToReadFromScanIndex = true;

        if (!isScanIndex && allColumnsSet != null && !allColumnsSet.isEmpty()) {
            boolean foundAllColumnsWeNeed = true;

            for (Column c : allColumnsSet) {
                if (c.getTable() == getTable()) {
                    boolean found = false;

                    for (Column c2 : getColumns()) {
                        if (c == c2) {
                            found = true;

                            break;
                        }
                    }

                    if (!found) {
                        foundAllColumnsWeNeed = false;

                        break;
                    }
                }
            }

            if (foundAllColumnsWeNeed)
                needsToReadFromScanIndex = false;
        }

        long rc;

        if (isScanIndex)
            rc = rowsCost + sortingCost + 20;
        else if (needsToReadFromScanIndex)
            rc = rowsCost + rowsCost + sortingCost + 20;
        else {
            // The (20-x) calculation makes sure that when we pick a covering
            // index, we pick the covering index that has the smallest number of
            // columns (the more columns we have in index - the higher cost).
            // This is faster because a smaller index will fit into fewer data
            // blocks.
            rc = rowsCost + sortingCost + getColumns().length;
        }

        return rc;
    }

    /**
     * Re-implement {@link BaseIndex#getCostRangeIndex} to support compatibility with versions between 8.7.8 and
     * 8.7.12.
     */
    protected final long getCostRangeIndex_8_7_12(int[] masks, long rowCount,
        TableFilter[] filters, int filter, SortOrder sortOrder,
        boolean isScanIndex, HashSet<Column> allColumnsSet) {
        rowCount += Constants.COST_ROW_OFFSET;

        int totalSelectivity = 0;

        long rowsCost = rowCount;

        if (masks != null) {
            int i = 0, len = columns.length;

            boolean tryAdditional = false;

            while (i < len) {
                Column column = columns[i++];

                int index = column.getColumnId();
                int mask = masks[index];

                if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                    if (i == len && getIndexType().isUnique()) {
                        rowsCost = 3;

                        break;
                    }

                    totalSelectivity = 100 - ((100 - totalSelectivity) *
                        (100 - column.getSelectivity()) / 100);

                    long distinctRows = rowCount * totalSelectivity / 100;

                    if (distinctRows <= 0)
                        distinctRows = 1;

                    rowsCost = 2 + Math.max(rowCount / distinctRows, 1);
                }
                else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                    rowsCost = 2 + rowsCost / 4;
                    tryAdditional = true;

                    break;
                }
                else if ((mask & IndexCondition.START) == IndexCondition.START) {
                    rowsCost = 2 + rowsCost / 3;
                    tryAdditional = true;

                    break;
                }
                else if ((mask & IndexCondition.END) == IndexCondition.END) {
                    rowsCost = rowsCost / 3;
                    tryAdditional = true;

                    break;
                }
                else {
                    if (mask == 0) {
                        // Adjust counter of used columns (i)
                        i--;
                    }

                    break;
                }
            }

            // Some additional columns can still be used
            if (tryAdditional) {
                while (i < len && masks[columns[i].getColumnId()] != 0) {
                    i++;
                    rowsCost--;
                }
            }
            // Increase cost of indexes with additional unused columns
            rowsCost += len - i;
        }

        // If the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost
        // accordingly.
        long sortingCost = 0;

        if (sortOrder != null)
            sortingCost = 100 + rowCount / 10;

        if (sortOrder != null && !isScanIndex) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();

            TableFilter tableFilter = filters == null ? null : filters[filter];

            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // We can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns.
                    break;
                }

                Column col = sortOrder.getColumn(i, tableFilter);

                if (col == null) {
                    sortOrderMatches = false;

                    break;
                }

                IndexColumn indexCol = indexColumns[i];

                if (!col.equals(indexCol.column)) {
                    sortOrderMatches = false;

                    break;
                }

                int sortType = sortTypes[i];

                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;

                    break;
                }

                coveringCount++;
            }

            if (sortOrderMatches) {
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more.
                sortingCost = 100 - coveringCount;
            }
        }

        // If we have two indexes with the same cost, and one of the indexes can
        // satisfy the query without needing to read from the primary table
        // (scan index), make that one slightly lower cost.
        boolean needsToReadFromScanIndex = true;

        if (!isScanIndex && allColumnsSet != null && !allColumnsSet.isEmpty()) {
            boolean foundAllColumnsWeNeed = true;

            for (Column c : allColumnsSet) {
                if (c.getTable() == getTable()) {
                    boolean found = false;

                    for (Column c2 : columns) {
                        if (c == c2) {
                            found = true;

                            break;
                        }
                    }

                    if (!found) {
                        foundAllColumnsWeNeed = false;

                        break;
                    }
                }
            }

            if (foundAllColumnsWeNeed)
                needsToReadFromScanIndex = false;
        }

        long rc;

        if (isScanIndex)
            rc = rowsCost + sortingCost + 20;
        else if (needsToReadFromScanIndex)
            rc = rowsCost + rowsCost + sortingCost + 20;
        else {
            // The (20-x) calculation makes sure that when we pick a covering
            // index, we pick the covering index that has the smallest number of
            // columns (the more columns we have in index - the higher cost).
            // This is faster because a smaller index will fit into fewer data
            // blocks.
            rc = rowsCost + sortingCost + columns.length;
        }

        return rc;
    }

    /**
     * Re-implement {@link BaseIndex#getCostRangeIndex} to suppor  compatibility with versions 8.7.6 and older.
     */
    private final long getCostRangeIndex_8_7_6(int[] masks, long rowCount,
        TableFilter[] filters, int filter, SortOrder sortOrder,
        boolean isScanIndex, HashSet<Column> allColumnsSet) {
        rowCount += Constants.COST_ROW_OFFSET;

        int totalSelectivity = 0;
        long rowsCost = rowCount;

        if (masks != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                Column column = columns[i];

                int index = column.getColumnId();
                int mask = masks[index];

                if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                    if (i == columns.length - 1 && getIndexType().isUnique()) {
                        rowsCost = 3;

                        break;
                    }
                    totalSelectivity = 100 - ((100 - totalSelectivity) *
                        (100 - column.getSelectivity()) / 100);

                    long distinctRows = rowCount * totalSelectivity / 100;

                    if (distinctRows <= 0)
                        distinctRows = 1;

                    rowsCost = 2 + Math.max(rowCount / distinctRows, 1);
                }
                else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                    rowsCost = 2 + rowCount / 4;

                    break;
                }
                else if ((mask & IndexCondition.START) == IndexCondition.START) {
                    rowsCost = 2 + rowCount / 3;

                    break;
                }
                else if ((mask & IndexCondition.END) == IndexCondition.END) {
                    rowsCost = rowCount / 3;

                    break;
                }
                else
                    break;
            }
        }
        // If the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost
        // accordingly.
        long sortingCost = 0;

        if (sortOrder != null)
            sortingCost = 100 + rowCount / 10;

        if (sortOrder != null && !isScanIndex) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();

            TableFilter tableFilter = filters == null ? null : filters[filter];

            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // We can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns.
                    break;
                }

                Column col = sortOrder.getColumn(i, tableFilter);

                if (col == null) {
                    sortOrderMatches = false;

                    break;
                }
                IndexColumn indexCol = indexColumns[i];

                if (!col.equals(indexCol.column)) {
                    sortOrderMatches = false;

                    break;
                }

                int sortType = sortTypes[i];

                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;

                    break;
                }
                coveringCount++;
            }

            if (sortOrderMatches) {
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more.
                sortingCost = 100 - coveringCount;
            }
        }

        // If we have two indexes with the same cost, and one of the indexes can
        // satisfy the query without needing to read from the primary table
        // (scan index), make that one slightly lower cost.
        boolean needsToReadFromScanIndex = true;

        if (!isScanIndex && allColumnsSet != null && !allColumnsSet.isEmpty()) {
            boolean foundAllColumnsWeNeed = true;

            for (Column c : allColumnsSet) {
                if (c.getTable() == getTable()) {
                    boolean found = false;

                    for (Column c2 : columns) {
                        if (c == c2) {
                            found = true;

                            break;
                        }
                    }

                    if (!found) {
                        foundAllColumnsWeNeed = false;

                        break;
                    }
                }
            }

            if (foundAllColumnsWeNeed)
                needsToReadFromScanIndex = false;
        }

        long rc;

        if (isScanIndex)
            rc = rowsCost + sortingCost + 20;
        else if (needsToReadFromScanIndex)
            rc = rowsCost + rowsCost + sortingCost + 20;
        else {
            // The (20-x) calculation makes sure that when we pick a covering
            // index, we pick the covering index that has the smallest number of
            // columns (the more columns we have in index - the higher cost).
            // This is faster because a smaller index will fit into fewer data
            // blocks.
            rc = rowsCost + sortingCost + columns.length;
        }
        return rc;
    }

    /**
     *
     */
    private enum CostFunctionType {
        /** Last. */
        LAST,

        /** Compatible with ver. 8.5.16. */
        COMPATIBLE_8_5_17,

        /** Compatible with ver. 8.5.13. */
        COMPATIBLE_8_5_13
    }

    /**
     * Cost function interface to re-implement {@link BaseIndex#getCostRangeIndex} to support compatibility with old
     * versions.
     */
    private interface CostFunction {
        /**
         * Cost function. See more: {@link BaseIndex#getCostRangeIndex}.
         */
        long getCostRangeIndex(int[] masks, long rowCount,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            boolean isScanIndex, HashSet<Column> allColumnsSet);
    }
}
