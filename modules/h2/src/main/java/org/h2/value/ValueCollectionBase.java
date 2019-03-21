/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.util.MathUtils;

/**
 * Base class for ARRAY and ROW values.
 */
public abstract class ValueCollectionBase extends Value {

    /**
     * Values.
     */
    final Value[] values;

    private TypeInfo type;

    private int hash;

    ValueCollectionBase(Value[] values) {
        this.values = values;
    }

    public Value[] getList() {
        return values;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = getValueType();
        for (Value v : values) {
            h = h * 31 + v.hashCode();
        }
        hash = h;
        return h;
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            long precision = 0, displaySize = 0;
            for (Value v : values) {
                TypeInfo t = v.getType();
                precision += t.getPrecision();
                displaySize += t.getDisplaySize();
            }
            this.type = type = new TypeInfo(getValueType(), precision, 0, MathUtils.convertLongToInt(displaySize),
                    null);
        }
        return type;
    }

    @Override
    public int compareWithNull(Value v, boolean forEquality, Mode databaseMode, CompareMode compareMode) {
        if (v == ValueNull.INSTANCE) {
            return Integer.MIN_VALUE;
        }
        ValueCollectionBase l = this;
        int leftType = l.getValueType();
        int rightType = v.getValueType();
        if (rightType != ARRAY && rightType != ROW) {
            throw v.getDataConversionError(leftType);
        }
        ValueCollectionBase r = (ValueCollectionBase) v;
        Value[] leftArray = l.values, rightArray = r.values;
        int leftLength = leftArray.length, rightLength = rightArray.length;
        if (leftLength != rightLength) {
            if (leftType == ROW || rightType == ROW) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
            if (forEquality) {
                return 1;
            }
        }
        if (forEquality) {
            boolean hasNull = false;
            for (int i = 0; i < leftLength; i++) {
                Value v1 = leftArray[i];
                Value v2 = rightArray[i];
                int comp = v1.compareWithNull(v2, forEquality, databaseMode, compareMode);
                if (comp != 0) {
                    if (comp != Integer.MIN_VALUE) {
                        return comp;
                    }
                    hasNull = true;
                }
            }
            return hasNull ? Integer.MIN_VALUE : 0;
        }
        int len = Math.min(leftLength, rightLength);
        for (int i = 0; i < len; i++) {
            Value v1 = leftArray[i];
            Value v2 = rightArray[i];
            int comp = v1.compareWithNull(v2, forEquality, databaseMode, compareMode);
            if (comp != 0) {
                return comp;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    @Override
    public boolean containsNull() {
        for (Value v : values) {
            if (v.containsNull()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getMemory() {
        int memory = 72;
        for (Value v : values) {
            memory += v.getMemory() + Constants.MEMORY_POINTER;
        }
        return memory;
    }

}
