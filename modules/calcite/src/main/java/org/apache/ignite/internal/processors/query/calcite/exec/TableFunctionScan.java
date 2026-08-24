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

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class TableFunctionScan<Row> implements Iterable<Row> {
    /** */
    private final RelDataType rowType;

    /** */
    private final Supplier<Iterable<?>> dataSupplier;

    /** */
    private final RowFactory<Row> rowFactory;

    /** Character columns that require Oracle-compatible empty string handling. */
    private final boolean[] characterColumns;

    /** */
    public TableFunctionScan(
        RelDataType rowType,
        Supplier<Iterable<?>> dataSupplier,
        RowFactory<Row> rowFactory
    ) {
        this.rowType = rowType;
        this.dataSupplier = dataSupplier;
        this.rowFactory = rowFactory;

        characterColumns = new boolean[rowType.getFieldCount()];

        for (int i = 0; i < characterColumns.length; i++)
            characterColumns[i] = SqlTypeUtil.isCharacter(rowType.getFieldList().get(i).getType());
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

    /** Converts empty strings returned for character columns to {@code null}. */
    private Object[] nullIfEmpty(Object[] row) {
        Object[] normalizedRow = row;

        for (int i = 0; i < characterColumns.length; i++) {
            if (characterColumns[i] && "".equals(row[i])) {
                if (normalizedRow == row)
                    normalizedRow = row.clone();

                normalizedRow[i] = null;
            }
        }

        return normalizedRow;
    }
}
