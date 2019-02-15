/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTimestamp;

/**
 * Local system view base class (which uses only local node data).
 */
public abstract class SqlAbstractLocalSystemView extends SqlAbstractSystemView {
    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexes Indexes.
     * @param cols Columns.
     */
    public SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, String[] indexes,
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
    public SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, String indexedCols, Column... cols) {
        this(tblName, desc, ctx, new String[] {indexedCols}, cols);
    }

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param cols Columns.
     */
    public SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, Column ... cols) {
        this(tblName, desc, ctx, new String[] {}, cols);
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
}
