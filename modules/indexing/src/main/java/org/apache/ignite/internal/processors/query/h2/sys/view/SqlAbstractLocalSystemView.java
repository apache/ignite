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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.TimeZone;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.util.DateTimeUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Local system view base class (which uses only local node data).
 */
@SuppressWarnings("IfMayBeConditional")
public abstract class SqlAbstractLocalSystemView extends SqlAbstractSystemView {

    public static final int MILLIS_IN_MIN = 60_000;

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexes Indexes.
     * @param cols Columns.
     */
    protected SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, String[] indexes,
        Column... cols) {
        super(tblName, desc, ctx, cols, indexes);

        assert tblName != null;
        assert cols != null;
        assert indexes != null;
    }

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexedCols Indexed columns.
     * @param cols Columns.
     */
    protected SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, String indexedCols,
        Column... cols) {
        this(tblName, desc, ctx, new String[] {indexedCols}, cols);
    }

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param cols Columns.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    protected SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, Column ... cols) {
        this(tblName, desc, ctx, new String[] {} , cols);
    }

    /**
     * @param ses Session.
     * @param data Data for each column.
     */
    protected Row createRow(Session ses, Object... data) {
        Value[] values = new Value[data.length];

        for (int i = 0; i < data.length; i++) {
            Object o = data[i];

            Value v = (o == null) ? ValueNull.INSTANCE :
                (o instanceof Value) ? (Value)o : ValueString.get(o.toString());

            values[i] = cols[i].convert(v);
        }

        return ses.getDatabase().createRow(values, 0);
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
    protected SqlSystemViewColumnCondition conditionForColumn(String colName, SearchRow first, SearchRow last) {
        return SqlSystemViewColumnCondition.forColumn(getColumnIndex(colName), first, last);
    }

    /**
     * Converts value to UUID safe (suppressing exceptions).
     *
     * @param val UUID.
     */
    protected static UUID uuidFromValue(Value val) {
        try {
            return UUID.fromString(val.getString());
        }
        catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Converts millis to ValueTimestamp
     *
     * @param millis Millis.
     */
    protected static Value valueTimestampFromMillis(long millis) {
        if (millis <= 0L || millis == Long.MAX_VALUE)
            return ValueNull.INSTANCE;
        else
            return ValueTimestamp.fromMillis(millis);
    }

    /**
     * Converts millis to H2 ValueTimestamp in default time zone.
     *
     * @param millis Millis.
     */
    protected static Value valueTimestampZoneFromMillis(long millis) {
        long dateVal = DateTimeUtils.dateValueFromDate(millis);
        long nanos = DateTimeUtils.nanosFromDate(millis);
        int tzOff = TimeZone.getDefault().getRawOffset();

        if(tzOff % MILLIS_IN_MIN == 0)
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateVal, nanos, (short)(tzOff / MILLIS_IN_MIN));
        else
            return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(dateVal, nanos);
    }
}
