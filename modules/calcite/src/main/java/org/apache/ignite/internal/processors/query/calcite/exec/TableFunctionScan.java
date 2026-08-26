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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class TableFunctionScan<Row> implements Iterable<Row> {
    /** */
    private final RelDataType rowType;

    /** */
    private final Supplier<Iterable<?>> dataSupplier;

    /** */
    private final RowFactory<Row> rowFactory;

    /** */
    private final @Nullable BitSet strCols;

    /** */
    public TableFunctionScan(
        RelDataType rowType,
        Supplier<Iterable<?>> dataSupplier,
        RowFactory<Row> rowFactory,
        boolean emptyStringIsNull
    ) {
        this.rowType = rowType;
        this.dataSupplier = dataSupplier;
        this.rowFactory = rowFactory;

        if (emptyStringIsNull) {
            strCols = new BitSet(rowType.getFieldCount());

            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (SqlTypeUtil.isCharacter(rowType.getFieldList().get(i).getType()))
                    strCols.set(i);
            }
        }
        else
            strCols = null;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> iterator() {
        return F.iterator(dataSupplier.get(), this::convertToRow, true);
    }

    /** */
    private Row convertToRow(Object rowContainer) {
        if (rowContainer.getClass() != Object[].class && !Collection.class.isAssignableFrom(rowContainer.getClass()))
            throw new IgniteSQLException("Unable to process table function data: row type is neither Collection or Object[].");

        Object[] rowArr = rowContainer.getClass() == Object[].class
            ? (Object[])rowContainer
            : ((Collection<?>)rowContainer).toArray();

        if (rowArr.length != rowType.getFieldCount()) {
            throw new IgniteSQLException("Unable to process table function data: row length [" + rowArr.length
                + "] doesn't match defined columns number [" + rowType.getFieldCount() + "].");
        }

        return rowFactory.create(nullIfEmpty(rowArr));
    }

    /** Converts empty strings returned for string columns to {@code null}. */
    private Object[] nullIfEmpty(Object[] row) {
        if (strCols == null)
            return row;

        Object[] res = row;

        for (int i = strCols.nextSetBit(0); i >= 0; i = strCols.nextSetBit(i + 1)) {
            Object val = row[i];

            if (val instanceof String && ((String)val).isEmpty()) {
                if (res == row)
                    res = row.clone();

                res[i] = null;
            }
        }

        return res;
    }
}
