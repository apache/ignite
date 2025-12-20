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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.TableRowIterable;
import org.jetbrains.annotations.Nullable;

/**
 * Scan table rows node.
 */
public class ScanTableRowNode<TableRow, Row> extends ScanStorageNode<Row> {
    /** */
    private final TableRowIterable<TableRow, Row> src;

    /** */
    private final int[] filterColMapping;

    /** */
    private final int[] otherColMapping;

    /** */
    private final RowHandler.RowFactory<Row> factory;

    /** */
    private Row curRow;

    /**
     * @param storageName Storage (index or table) name.
     * @param ctx Execution context.
     * @param outputRowType Output row type.
     * @param inputRowType Input row type.
     * @param src Source.
     * @param filter Row filter.
     * @param rowTransformer Row transformer (projection).
     * @param filterColMapping Fields to columns mapping for fields used in filter.
     * @param otherColMapping Fields to columns mapping for other fields.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ScanTableRowNode(
        String storageName,
        ExecutionContext<Row> ctx,
        RelDataType outputRowType,
        RelDataType inputRowType,
        TableRowIterable<TableRow, Row> src,
        Predicate<Row> filter,
        @Nullable Function<Row, Row> rowTransformer,
        int[] filterColMapping,
        int[] otherColMapping
    ) {
        super(storageName, ctx, outputRowType, src, filter, rowTransformer);

        assert filter != null;

        factory = ctx.rowHandler().factory(ctx.getTypeFactory(), inputRowType);

        this.src = src;
        this.filterColMapping = filterColMapping;
        this.otherColMapping = otherColMapping;
    }

    /** {@inheritDoc} */
    @Override protected Iterator<?> sourceIterator() {
        return src.tableRowIterator();
    }

    /** {@inheritDoc} */
    @Override protected Row processNextRow() {
        Row row = curRow == null ? factory.create() : curRow;

        TableRow tableRow = (TableRow)it.next();

        src.enrichRow(tableRow, row, filterColMapping);

        if (filter.test(row)) {
            src.enrichRow(tableRow, row, otherColMapping);

            if (rowTransformer != null)
                row = rowTransformer.apply(row);

            curRow = null;

            return row;
        }

        return null;
    }

    /** */
    @Override public void closeInternal() {
        super.closeInternal();

        curRow = null;
    }
}
