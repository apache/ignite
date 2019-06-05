/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

/**
 * The interface for rows stored in a table, and for partial rows stored in the
 * index.
 */
public interface SearchRow {

    /**
     * An empty array of SearchRow objects.
     */
    SearchRow[] EMPTY_ARRAY = {};

    /**
     * Get the column count.
     *
     * @return the column count
     */
    int getColumnCount();

    /**
     * Get the value for the column
     *
     * @param index the column number (starting with 0)
     * @return the value
     */
    Value getValue(int index);

    /**
     * Set the value for given column
     *
     * @param index the column number (starting with 0)
     * @param v the new value
     */
    void setValue(int index, Value v);

    /**
     * Set the position and version to match another row.
     *
     * @param old the other row.
     */
    void setKeyAndVersion(SearchRow old);

    /**
     * Get the version of the row.
     *
     * @return the version
     */
    int getVersion();

    /**
     * Set the unique key of the row.
     *
     * @param key the key
     */
    void setKey(long key);

    /**
     * Get the unique key of the row.
     *
     * @return the key
     */
    long getKey();

    /**
     * Get the estimated memory used for this row, in bytes.
     *
     * @return the memory
     */
    int getMemory();

}
