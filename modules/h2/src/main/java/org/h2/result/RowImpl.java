/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.store.Data;
import org.h2.value.Value;
import org.h2.value.ValueLong;

/**
 * Default row implementation.
 */
public class RowImpl implements Row {
    private long key;
    private final Value[] data;
    private int memory;
    private boolean deleted;

    public RowImpl(Value[] data, int memory) {
        this.data = data;
        this.memory = memory;
    }

    @Override
    public void setKey(SearchRow row) {
        setKey(row.getKey());
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
    public Value getValue(int i) {
        return i == SearchRow.ROWID_INDEX ? ValueLong.get(key) : data[i];
    }

    /**
     * Get the number of bytes required for the data.
     *
     * @param dummy the template buffer
     * @return the number of bytes
     */
    @Override
    public int getByteCount(Data dummy) {
        int size = 0;
        for (Value v : data) {
            size += dummy.getValueLen(v);
        }
        return size;
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == SearchRow.ROWID_INDEX) {
            this.key = v.getLong();
        } else {
            data[i] = v;
        }
    }

    @Override
    public boolean isEmpty() {
        return data == null;
    }

    @Override
    public int getColumnCount() {
        return data.length;
    }

    @Override
    public int getMemory() {
        if (memory != MEMORY_CALCULATE) {
            return memory;
        }
        int m = Constants.MEMORY_ROW;
        if (data != null) {
            int len = data.length;
            m += Constants.MEMORY_OBJECT + len * Constants.MEMORY_POINTER;
            for (Value v : data) {
                if (v != null) {
                    m += v.getMemory();
                }
            }
        }
        this.memory = m;
        return m;
    }

    @Override
    public String toString() {
        return toString(key, deleted, data);
    }

    /**
     * Convert a row to a string.
     *
     * @param key the key
     * @param isDeleted whether the row is deleted
     * @param data the row data
     * @return the string representation
     */
    static String toString(long key, boolean isDeleted, Value[] data) {
        StringBuilder builder = new StringBuilder("( /* key:").append(key);
        if (isDeleted) {
            builder.append(" deleted");
        }
        builder.append(" */ ");
        if (data != null) {
            for (int i = 0, length = data.length; i < length; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                Value v = data[i];
                builder.append(v == null ? "null" : v.getTraceSQL());
            }
        }
        return builder.append(')').toString();
    }

    @Override
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public Value[] getValueList() {
        return data;
    }

    @Override
    public boolean hasSharedData(Row other) {
        if (other.getClass() == RowImpl.class) {
            RowImpl o = (RowImpl) other;
            return data == o.data;
        }
        return false;
    }
}
