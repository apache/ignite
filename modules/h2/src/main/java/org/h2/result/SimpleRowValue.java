/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.value.Value;

/**
 * A simple row that contains data for only one column.
 */
public class SimpleRowValue implements SearchRow {

    private long key;
    private int version;
    private int index;
    private final int virtualColumnCount;
    private Value data;

    public SimpleRowValue(int columnCount) {
        this.virtualColumnCount = columnCount;
    }

    @Override
    public void setKeyAndVersion(SearchRow row) {
        key = row.getKey();
        version = row.getVersion();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public int getColumnCount() {
        return virtualColumnCount;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public void setKey(long key) {
        this.key = key;
    }

    @Override
    public Value getValue(int idx) {
        return idx == index ? data : null;
    }

    @Override
    public void setValue(int idx, Value v) {
        index = idx;
        data = v;
    }

    @Override
    public String toString() {
        return "( /* " + key + " */ " + (data == null ?
                "null" : data.getTraceSQL()) + " )";
    }

    @Override
    public int getMemory() {
        return Constants.MEMORY_OBJECT + (data == null ? 0 : data.getMemory());
    }

}
