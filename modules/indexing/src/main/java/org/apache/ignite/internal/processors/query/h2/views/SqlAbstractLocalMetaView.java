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

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Local meta view base class (which uses only local node data).
 */
public abstract class SqlAbstractLocalMetaView extends SqlAbstractMetaView {
    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexes Indexed columns.
     * @param cols Columns.
     */
    public SqlAbstractLocalMetaView(String tblName, String desc, GridKernalContext ctx, String[] indexes, Column... cols) {
        super(tblName, desc, ctx, cols, indexes);
        assert tblName != null;
        assert ctx != null;
        assert cols != null;
        assert indexes != null;

    }

    /**
     * Converts string to UUID safe (suppressing exceptions).
     *
     * @param val UUID in string format.
     */
    protected static UUID uuidFromString(String val) {
        try {
            return UUID.fromString(val);
        }
        catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @param data Data for each column.
     */
    protected Row createRow(Session ses, long key, Object... data) {
        Value[] values = new Value[data.length];

        for (int i = 0; i < data.length; i++) {
            Object o = data[i];

            Value v = (o == null) ? ValueNull.INSTANCE :
                (o instanceof Value) ? (Value)o : ValueString.get(o.toString());

            values[i] = cols[i].convert(v);
        }

        Row row = ses.getDatabase().createRow(values, 1);

        row.setKey(key);

        return row;
    }

    /**
     * Gets column index by name.
     *
     * @param colName Column name.
     */
    protected int getColumnIndex(String colName) {
        assert colName != null;

        for (int i = 0; i < cols.length; i++)
            if (colName.equalsIgnoreCase(cols[i].getName()))
                return i;

        return -1;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /**
     * Parse condition for column.
     *
     * @param colName Column name.
     * @param first First.
     * @param last Last.
     */
    protected ColumnCondition conditionForColumn(String colName, SearchRow first, SearchRow last) {
        return ColumnCondition.forColumn(getColumnIndex(colName), first, last);
    }

    /**
     * Column condition.
     */
    protected static class ColumnCondition {
        /** Is equality. */
        private final boolean isEquality;

        /** Is range. */
        private final boolean isRange;

        /** Value 1. */
        private final Value val1;

        /** Value 2. */
        private final Value val2;

        /**
         * @param isEquality Is equality.
         * @param isRange Is range.
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        private ColumnCondition(boolean isEquality, boolean isRange, Value val1, Value val2) {
            this.isEquality = isEquality;
            this.isRange = isRange;
            this.val1 = val1;
            this.val2 = val2;
        }

        /**
         * Parse condition for column.
         *
         * @param colIdx Column index.
         * @param start Start row values.
         * @param end End row values.
         */
        public static ColumnCondition forColumn(int colIdx, SearchRow start, SearchRow end) {
            boolean isEquality = false;
            boolean isRange = false;

            Value val1 = null;
            Value val2 = null;

            if (start != null && colIdx >= 0 && colIdx < start.getColumnCount())
                val1 = start.getValue(colIdx);

            if (end != null && colIdx >= 0 && colIdx < end.getColumnCount())
                val2 = end.getValue(colIdx);

            if (val1 != null && val2 != null) {
                if (val1.equals(val2))
                    isEquality = true;
                else
                    isRange = true;
            }
            else if (val1 != null || val2 != null)
                isRange = true;

            return new ColumnCondition(isEquality, isRange, val1, val2);
        }

        /**
         * Checks whether the condition is equality.
         */
        public boolean isEquality() {
            return isEquality;
        }

        /**
         * Checks whether the condition is range.
         */
        public boolean isRange() {
            return isRange;
        }

        /**
         * Gets value, if condition is equality.
         */
        public Value getValue() {
            if (isEquality)
                return val1;

            return null;
        }
    }
}
