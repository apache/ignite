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
public abstract class SqlAbstractLocalSystemView extends SqlAbstractSystemView {
    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexes Indexed columns.
     * @param cols Columns.
     */
    public SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, String[] indexes,
        Column... cols) {
        super(tblName, desc, ctx, cols, indexes);

        assert tblName != null;
        assert ctx != null;
        assert cols != null;
        assert indexes != null;
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
}
