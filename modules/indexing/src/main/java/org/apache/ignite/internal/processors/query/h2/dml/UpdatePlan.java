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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 * Update plan - where to take data to update cache from and how to construct new keys and values, if needed.
 */
public final class UpdatePlan {
    /** Initial statement to drive the rest of the logic. */
    private final UpdateMode mode;

    /**  to be affected by initial DML statement. */
    private final GridH2Table tbl;

    /** Column names to set or update. */
    private final String[] colNames;

    /** Column types to set for insert/merge. */
    private final int[] colTypes;

    /** Method to create key for INSERT or MERGE, ignored for UPDATE and DELETE. */
    private final KeyValueSupplier keySupplier;

    /** Method to create value to put to cache, ignored for DELETE. */
    private final KeyValueSupplier valSupplier;

    /** Key column index. */
    private final int keyColIdx;

    /** Value column index. */
    private final int valColIdx;

    /** SELECT statement built upon initial DML statement. */
    private final String selectQry;

    /** Subquery flag - {@code true} if {@link #selectQry} is an actual subquery that retrieves data from some cache. */
    private final boolean isLocSubqry;

    /** Rows for query-less MERGE or INSERT. */
    private final List<List<FastUpdateArgument>> rows;

    /** Number of rows in rows based MERGE or INSERT. */
    private final int rowsNum;

    /** Arguments for fast UPDATE or DELETE. */
    private final FastUpdate fastUpdate;

    /** Additional info for distributed update. */
    private final DmlDistributedPlanInfo distributed;

    /*
    private UpdatePlan(UpdateMode mode, GridH2Table tbl, String[] colNames, int[] colTypes, KeyValueSupplier keySupplier,
                       KeyValueSupplier valSupplier, int keyColIdx, int valColIdx, String selectQry, boolean isLocSubqry,
                       List<List<FastUpdateArgument>> rows, int rowsNum, FastUpdateArguments fastUpdateArgs) {
     */

    /**
     * Constructor.
     *
     * @param mode Mode.
     * @param tbl Table.
     * @param colNames Column names.
     * @param colTypes Column types.
     * @param keySupplier Key supplier.
     * @param valSupplier Value supplier.
     * @param keyColIdx Key column index.
     * @param valColIdx value column index.
     * @param selectQry Select query.
     * @param isLocSubqry Local subquery flag.
     * @param rows Rows for query-less INSERT or MERGE.
     * @param rowsNum Rows number.
     * @param fastUpdate Fast update (if any).
     * @param distributed Distributed plan (if any)
     */
    public UpdatePlan(
        UpdateMode mode,
        GridH2Table tbl,
        String[] colNames,
        int[] colTypes,
        KeyValueSupplier keySupplier,
        KeyValueSupplier valSupplier,
        int keyColIdx,
        int valColIdx,
        String selectQry,
        boolean isLocSubqry,
        List<List<FastUpdateArgument>> rows,
        int rowsNum,
        @Nullable FastUpdate fastUpdate,
        @Nullable DmlDistributedPlanInfo distributed
    ) {
        this.colNames = colNames;
        this.colTypes = colTypes;
        this.rows = rows;
        this.rowsNum = rowsNum;

        assert mode != null;
        assert tbl != null;

        this.mode = mode;
        this.tbl = tbl;
        this.keySupplier = keySupplier;
        this.valSupplier = valSupplier;
        this.keyColIdx = keyColIdx;
        this.valColIdx = valColIdx;
        this.selectQry = selectQry;
        this.isLocSubqry = isLocSubqry;
        this.fastUpdate = fastUpdate;
        this.distributed = distributed;
    }

    /**
     * Constructor for delete operation or fast update.
     *
     * @param mode Mode.
     * @param tbl Table.
     * @param selectQry Select query.
     * @param fastUpdate Fast update arguments (if any).
     * @param distributed Distributed plan (if any)
     */
    public UpdatePlan(
        UpdateMode mode,
        GridH2Table tbl,
        String selectQry,
        @Nullable FastUpdate fastUpdate,
        @Nullable DmlDistributedPlanInfo distributed
    ) {
        this(
            mode,
            tbl,
            null,
            null,
            null,
            null,
            -1,
            -1,
            selectQry,
            false,
            null,
            0,
            fastUpdate,
            distributed
        );
    }

    /**
     * Convert a row into key-value pair.
     *
     * @param row Row to process.
     * @throws IgniteCheckedException if failed.
     */
    public IgniteBiTuple<?, ?> processRow(List<?> row) throws IgniteCheckedException {
        GridH2RowDescriptor rowDesc = tbl.rowDescriptor();
        GridQueryTypeDescriptor desc = rowDesc.type();

        GridCacheContext cctx = rowDesc.context();

        Object key = keySupplier.apply(row);

        if (QueryUtils.isSqlType(desc.keyClass())) {
            assert keyColIdx != -1;

            key = DmlUtils.convert(key, rowDesc, desc.keyClass(), colTypes[keyColIdx]);
        }

        Object val = valSupplier.apply(row);

        if (QueryUtils.isSqlType(desc.valueClass())) {
            assert valColIdx != -1;

            val = DmlUtils.convert(val, rowDesc, desc.valueClass(), colTypes[valColIdx]);
        }

        if (key == null) {
            if (F.isEmpty(desc.keyFieldName()))
                throw new IgniteSQLException("Key for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_KEY);
            else
                throw new IgniteSQLException("Null value is not allowed for column '" + desc.keyFieldName() + "'",
                    IgniteQueryErrorCode.NULL_KEY);
        }

        if (val == null) {
            if (F.isEmpty(desc.valueFieldName()))
                throw new IgniteSQLException("Value for INSERT, MERGE, or UPDATE must not be null",
                    IgniteQueryErrorCode.NULL_VALUE);
            else
                throw new IgniteSQLException("Null value is not allowed for column '" + desc.valueFieldName() + "'",
                    IgniteQueryErrorCode.NULL_VALUE);
        }

        Map<String, Object> newColVals = new HashMap<>();

        for (int i = 0; i < colNames.length; i++) {
            if (i == keyColIdx || i == valColIdx)
                continue;

            String colName = colNames[i];

            GridQueryProperty prop = desc.property(colName);

            assert prop != null;

            Class<?> expCls = prop.type();

            newColVals.put(colName, DmlUtils.convert(row.get(i), rowDesc, expCls, colTypes[i]));
        }

        // We update columns in the order specified by the table for a reason - table's
        // column order preserves their precedence for correct update of nested properties.
        Column[] cols = tbl.getColumns();

        // First 3 columns are _key, _val and _ver. Skip 'em.
        for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
            if (tbl.rowDescriptor().isKeyValueOrVersionColumn(i))
                continue;

            String colName = cols[i].getName();

            if (!newColVals.containsKey(colName))
                continue;

            Object colVal = newColVals.get(colName);

            desc.setValue(colName, key, val, colVal);
        }

        if (cctx.binaryMarshaller()) {
            if (key instanceof BinaryObjectBuilder)
                key = ((BinaryObjectBuilder) key).build();

            if (val instanceof BinaryObjectBuilder)
                val = ((BinaryObjectBuilder) val).build();
        }

        desc.validateKeyAndValue(key, val);

        return new IgniteBiTuple<>(key, val);
    }

    /**
     * Convert a row into value.
     *
     * @param row Row to process.
     * @throws IgniteCheckedException if failed.
     */
    public T3<Object, Object, Object> processRowForUpdate(List<?> row) throws IgniteCheckedException {
        GridH2RowDescriptor rowDesc = tbl.rowDescriptor();
        GridQueryTypeDescriptor desc = rowDesc.type();

        GridCacheContext cctx = rowDesc.context();

        boolean hasNewVal = (valColIdx != -1);

        boolean hasProps = !hasNewVal || colNames.length > 1;

        Object key = row.get(0);

        Object oldVal = row.get(1);

        if (cctx.binaryMarshaller() && !(oldVal instanceof BinaryObject))
            oldVal = cctx.grid().binary().toBinary(oldVal);

        Object newVal;

        Map<String, Object> newColVals = new HashMap<>();

        for (int i = 0; i < colNames.length; i++) {
            if (hasNewVal && i == valColIdx - 2)
                continue;

            GridQueryProperty prop = tbl.rowDescriptor().type().property(colNames[i]);

            assert prop != null : "Unknown property: " + colNames[i];

            newColVals.put(colNames[i], DmlUtils.convert(row.get(i + 2), rowDesc, prop.type(), colTypes[i]));
        }

        newVal = valSupplier.apply(row);

        if (newVal == null)
            throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

        // Skip key and value - that's why we start off with 3rd column
        for (int i = 0; i < tbl.getColumns().length - DEFAULT_COLUMNS_COUNT; i++) {
            Column c = tbl.getColumn(i + DEFAULT_COLUMNS_COUNT);

            if (rowDesc.isKeyValueOrVersionColumn(c.getColumnId()))
                continue;

            GridQueryProperty prop = desc.property(c.getName());

            if (prop.key())
                continue; // Don't get values of key's columns - we won't use them anyway

            boolean hasNewColVal = newColVals.containsKey(c.getName());

            if (!hasNewColVal)
                continue;

            Object colVal = newColVals.get(c.getName());

            // UPDATE currently does not allow to modify key or its fields, so we must be safe to pass null as key.
            rowDesc.setColumnValue(null, newVal, colVal, i);
        }

        if (cctx.binaryMarshaller() && hasProps) {
            assert newVal instanceof BinaryObjectBuilder;

            newVal = ((BinaryObjectBuilder) newVal).build();
        }

        desc.validateKeyAndValue(key, newVal);

        return new T3<>(key, oldVal, newVal);
    }

    /**
     * @return Update mode.
     */
    public UpdateMode mode() {
        return mode;
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext cacheContext() {
        return tbl.cache();
    }

    /**
     * @return Distributed plan info (for skip-reducer mode).
     */
    @Nullable public DmlDistributedPlanInfo distributedPlan() {
        return distributed;
    }

    /**
     * @return Row count.
     */
    public int rowCount() {
        return rowsNum;
    }

    /**
     * @return Select query.
     */
    public String selectQuery() {
        return selectQry;
    }

    /**
     * @return Local subquery flag.
     */
    @Nullable public boolean isLocalSubquery() {
        return isLocSubqry;
    }

    /**
     * @return Fast update.
     */
    @Nullable public FastUpdate fastUpdate() {
        return fastUpdate;
    }

    /**
     * @return Names of affected columns.
     */
    public String[] columnNames() {
        return colNames;
    }

    /**
     * @return Types of affected columns.
     */
    public int[] columnTypes() {
        return colTypes;
    }

    /**
     * @return Rows for query-less MERGE or INSERT.
     */
    public List<List<FastUpdateArgument>> rows() {
        return rows;
    }

    /**
     * @return Key column index.
     */
    public int keyColumnIndex() {
        return keyColIdx;
    }

    /**
     * @return Value column index.
     */
    public int valueColumnIndex() {
        return valColIdx;
    }

    /**
     * @return Target table.
     */
    public GridH2Table table() {
        return tbl;
    }

    /*
    public static UpdatePlan forMerge(GridH2Table tbl, String[] colNames, int[] colTypes, KeyValueSupplier keySupplier,
                                      KeyValueSupplier valSupplier, int keyColIdx, int valColIdx, String selectQry, boolean isLocSubqry,
                                      List<List<FastUpdateArgument>> rows, int rowsNum) {
        assert !F.isEmpty(colNames);

        return new UpdatePlan(UpdateMode.MERGE, tbl, colNames, colTypes, keySupplier, valSupplier, keyColIdx, valColIdx,
            selectQry, isLocSubqry, rows, rowsNum, null);
    }

    public static UpdatePlan forInsert(GridH2Table tbl, String[] colNames, int[] colTypes, KeyValueSupplier keySupplier,
                                       KeyValueSupplier valSupplier, int keyColIdx, int valColIdx, String selectQry, boolean isLocSubqry,
                                       List<List<FastUpdateArgument>> rows, int rowsNum) {
        assert !F.isEmpty(colNames);

        return new UpdatePlan(UpdateMode.INSERT, tbl, colNames, colTypes, keySupplier, valSupplier, keyColIdx, valColIdx,
            selectQry, isLocSubqry, rows, rowsNum, null);
    }

    public static UpdatePlan forUpdate(GridH2Table tbl, String[] colNames, int[] colTypes, KeyValueSupplier valSupplier,
                                       int valColIdx, String selectQry) {
        assert !F.isEmpty(colNames);

        return new UpdatePlan(UpdateMode.UPDATE, tbl, colNames, colTypes, null, valSupplier, -1, valColIdx, selectQry,
            false, null, 0, null);
    }

    public static UpdatePlan forDelete(GridH2Table tbl, String selectQry) {
        return new UpdatePlan(UpdateMode.DELETE, tbl, null, null, null, null, -1, -1, selectQry, false, null, 0, null);
    }

    public static UpdatePlan forFastUpdate(UpdateMode mode, GridH2Table tbl, FastUpdateArguments fastUpdateArgs) {
        assert mode == UpdateMode.UPDATE || mode == UpdateMode.DELETE;

        return new UpdatePlan(mode, tbl, null, null, null, null, -1, -1, null, false, null, 0, fastUpdateArgs);
    }
     */
}
