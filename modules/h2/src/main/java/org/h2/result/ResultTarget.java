/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

/**
 * A object where rows are written to.
 */
public interface ResultTarget {

    /**
     * Add the row to the result set.
     *
     * @param values the values
     */
    void addRow(Value[] values);

    /**
     * Get the number of rows.
     *
     * @return the number of rows
     */
    int getRowCount();

    /**
     * A hint that sorting, offset and limit may be ignored by this result
     * because they were applied during the query. This is useful for WITH TIES
     * clause because result may contain tied rows above limit.
     */
    void limitsWereApplied();

}
