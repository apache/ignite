/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.store.Data;
import org.h2.value.Value;

/**
 * Represents a row in a table.
 */
public interface Row extends SearchRow {

    int MEMORY_CALCULATE = -1;
    Row[] EMPTY_ARRAY = {};

    /**
     * Get the number of bytes required for the data.
     *
     * @param dummy the template buffer
     * @return the number of bytes
     */
    int getByteCount(Data dummy);

    /**
     * Check if this is an empty row.
     *
     * @return {@code true} if the row is empty
     */
    boolean isEmpty();

    /**
     * Mark the row as deleted.
     *
     * @param deleted deleted flag
     */
    void setDeleted(boolean deleted);

    /**
     * Check if the row is deleted.
     *
     * @return {@code true} if the row is deleted
     */
    boolean isDeleted();

    /**
     * Get values.
     *
     * @return values
     */
    Value[] getValueList();

    /**
     * Check whether this row and the specified row share the same underlying
     * data with values. This method must return {@code false} when values are
     * not equal and may return either {@code true} or {@code false} when they
     * are equal. This method may be used only for optimizations and should not
     * perform any slow checks, such as equality checks for all pairs of values.
     *
     * @param other
     *            the other row
     * @return {@code true} if rows share the same underlying data,
     *         {@code false} otherwise or when unknown
     */
    boolean hasSharedData(Row other);

}
