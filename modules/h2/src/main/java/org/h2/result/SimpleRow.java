/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.value.Value;

/**
 * Represents a simple row without state.
 */
public class SimpleRow implements SearchRow {

    private long key;
    private final Value[] data;
    private int memory;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    @Override
    public int getColumnCount() {
        return data.length;
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
    public void setKey(SearchRow row) {
        key = row.getKey();
    }

    @Override
    public void setValue(int i, Value v) {
        data[i] = v;
    }

    @Override
    public Value getValue(int i) {
        return data[i];
    }

    @Override
    public String toString() {
        return RowImpl.toString(key, false, data);
    }

    @Override
    public int getMemory() {
        if (memory == 0) {
            int len = data.length;
            memory = Constants.MEMORY_OBJECT + len * Constants.MEMORY_POINTER;
            for (Value v : data) {
                if (v != null) {
                    memory += v.getMemory();
                }
            }
        }
        return memory;
    }

}
