/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.table;

import org.h2.result.SortOrder;

/**
 * Information about current sub-query being prepared.
 *
 * @author Sergi Vladykin
 */
public class SubQueryInfo {

    private final int[] masks;
    private final TableFilter[] filters;
    private final int filter;
    private final SortOrder sortOrder;
    private final SubQueryInfo upper;

    /**
     * @param upper upper level sub-query if any
     * @param masks index conditions masks
     * @param filters table filters
     * @param filter current filter
     * @param sortOrder sort order
     */
    public SubQueryInfo(SubQueryInfo upper, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder) {
        this.upper = upper;
        this.masks = masks;
        this.filters = filters;
        this.filter = filter;
        this.sortOrder = sortOrder;
    }

    public SubQueryInfo getUpper() {
        return upper;
    }

    public int[] getMasks() {
        return masks;
    }

    public TableFilter[] getFilters() {
        return filters;
    }

    public int getFilter() {
        return filter;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }
}
