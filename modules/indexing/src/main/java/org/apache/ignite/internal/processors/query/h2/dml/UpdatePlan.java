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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.h2.ConnectionManager;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.dml.UpdateMode.BULK_LOAD;

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
    private final List<List<DmlArgument>> rows;

    /** Number of rows in rows based MERGE or INSERT. */
    private final int rowsNum;

    /** Arguments for fast UPDATE or DELETE. */
    private final FastUpdate fastUpdate;

    /** Additional info for distributed update. */
    private final DmlDistributedPlanInfo distributed;

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
        List<List<DmlArgument>> rows,
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
        if (mode != BULK_LOAD && row.size() != colNames.length)
            throw new IgniteSQLException("Not enough values in a row: " + row.size() + " instead of " + colNames.length,
                IgniteQueryErrorCode.ENTRY_PROCESSING);

        GridH2RowDescriptor rowDesc = tbl.rowDescriptor();
        GridQueryTypeDescriptor desc = rowDesc.type();

        GridCacheContext cctx = rowDesc.context();

        Object key = keySupplier.apply(row);

        if (QueryUtils.isSqlType(desc.keyClass())) {
            assert keyColIdx != -1;

            key = DmlUtils.convert(key, rowDesc, desc.keyClass(), colTypes[keyColIdx], colNames[keyColIdx]);
        }

        Object val = valSupplier.apply(row);

        if (QueryUtils.isSqlType(desc.valueClass())) {
            assert valColIdx != -1;

            val = DmlUtils.convert(val, rowDesc, desc.valueClass(), colTypes[valColIdx], colNames[valColIdx]);
        }

        if (key == null) {
            if (F.isEmpty(desc.keyFieldName()))
                throw new IgniteSQLException("Key for INSERT, COPY, or MERGE must not be null",
                    IgniteQueryErrorCode.NULL_KEY);
            else
                throw new IgniteSQLException("Null value is not allowed for column '" + desc.keyFieldName() + "'",
                    IgniteQueryErrorCode.NULL_KEY);
        }

        if (val == null) {
            if (F.isEmpty(desc.valueFieldName()))
                throw new IgniteSQLException("Value for INSERT, COPY, MERGE, or UPDATE must not be null",
                    IgniteQueryErrorCode.NULL_VALUE);
            else
                throw new IgniteSQLException("Null value is not allowed for column '" + desc.valueFieldName() + "'",
                    IgniteQueryErrorCode.NULL_VALUE);
        }

        int actualColCnt = Math.min(colNames.length, row.size());

        Map<String, Object> newColVals = new HashMap<>();

        for (int i = 0; i < actualColCnt; i++) {
            if (i == keyColIdx || i == valColIdx)
                continue;

            String colName = colNames[i];

            GridQueryProperty prop = desc.property(colName);

            assert prop != null;

            Class<?> expCls = prop.type();

            newColVals.put(colName, DmlUtils.convert(row.get(i), rowDesc, expCls, colTypes[i], colNames[i]));
        }

        desc.setDefaults(key, val);

        // We update columns in the order specified by the table for a reason - table's
        // column order preserves their precedence for correct update of nested properties.
        Column[] tblCols = tbl.getColumns();

        // First 2 columns are _key and _val Skip 'em.
        for (int i = QueryUtils.DEFAULT_COLUMNS_COUNT; i < tblCols.length; i++) {
            if (tbl.rowDescriptor().isKeyValueOrVersionColumn(i))
                continue;

            String colName = tblCols[i].getName();

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
     * @return Tuple contains: [key, old value, new value]
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

            newColVals.put(colNames[i], DmlUtils.convert(row.get(i + 2), rowDesc, prop.type(), colTypes[i], colNames[i]));
        }

        newVal = valSupplier.apply(row);

        if (newVal == null)
            throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

        // Skip key and value - that's why we start off with 3rd column
        for (int i = 0; i < tbl.getColumns().length - QueryUtils.DEFAULT_COLUMNS_COUNT; i++) {
            Column c = tbl.getColumn(i + QueryUtils.DEFAULT_COLUMNS_COUNT);

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
     * @return {@code True} if DML can be fast processed.
     */
    public boolean fastResult() {
        return fastUpdate != null;
    }

    /**
     * Process fast DML operation if possible.
     *
     * @param args QUery arguments.
     * @return Update result or {@code null} if fast update is not applicable for plan.
     * @throws IgniteCheckedException If failed.
     */
    public UpdateResult processFast(Object[] args) throws IgniteCheckedException {
        if (fastUpdate != null)
            return fastUpdate.execute(cacheContext().cache(), args);

        return null;
    }

    /**
     * @return {@code True} if predefined rows exist.
     */
    public boolean hasRows() {
        return !F.isEmpty(rows);
    }

    /**
     * Extract rows from plan without performing any query.
     *
     * @param args Original query arguments.
     * @return {@link List} of rows from the plan for a single query.
     * For example, if we have multiple args in a query: <br/>
     * {@code INSERT INTO person VALUES (k1, v1), (k2, v2), (k3, v3);} <br/>
     * we will get a {@link List} of {@link List} with items {@code {[k1, v1], [k2, v2], [k3, v3]}}.
     * @throws IgniteCheckedException if failed.
     */
    public List<List<?>> createRows(Object[] args) throws IgniteCheckedException {
        assert rowsNum > 0 && !F.isEmpty(colNames);

        List<List<?>> res = new ArrayList<>(rowsNum);

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        extractArgsValues(args, res, desc);

        return res;
    }

    /**
     * Extract rows from plan without performing any query.
     *
     * @param argss Batch of arguments.
     * @return {@link List} of rows from the plan for each query.
     * For example, if we have a batch of queries with multiple args: <br/>
     * <code>
     * INSERT INTO person VALUES (k1, v1), (k2, v2), (k3, v3); <br/>
     * INSERT INTO person VALUES (k4, v4), (k5, v5), (k6, v6);<br/>
     * </code>
     * we will get a {@link List} of {@link List} of {@link List} with items: <br/>
     * <code>
     * {[k1, v1], [k2, v2], [k3, v3]},<br/>
     * {[k4, v4], [k5, v5], [k6, v6]}<br/>
     *
     * @throws IgniteCheckedException If failed.
     */
    public List<List<List<?>>> createRows(List<Object[]> argss) throws IgniteCheckedException {
        assert rowsNum > 0 && !F.isEmpty(colNames);
        assert argss != null;

        List<List<List<?>>> resPerQry = new ArrayList<>(argss.size());

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        for (Object[] args : argss) {
            List<List<?>> res = new ArrayList<>();

            resPerQry.add(res);

            extractArgsValues(args, res, desc);
        }

        return resPerQry;
    }

    /**
     * Extracts values from arguments.
     *
     * @param args Arguments.
     * @param res Result list where to put values to.
     * @param desc Row descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void extractArgsValues(Object[] args, List<List<?>> res, GridH2RowDescriptor desc)
        throws IgniteCheckedException {
        assert res != null;

        for (List<DmlArgument> row : rows) {
            List<Object> resRow = new ArrayList<>();

            for (int j = 0; j < colNames.length; j++) {
                Object colVal = row.get(j).get(args);

                if (j == keyColIdx || j == valColIdx) {
                    Class<?> colCls = j == keyColIdx ? desc.type().keyClass() : desc.type().valueClass();

                    colVal = DmlUtils.convert(colVal, desc, colCls, colTypes[j], colNames[j]);
                }

                resRow.add(colVal);
            }

            res.add(resRow);
        }
    }

    /**
     * Create iterator for transaction.
     *
     * @param connMgr Connection manager.
     * @param cur Cursor.
     * @return Iterator.
     */
    public UpdateSourceIterator<?> iteratorForTransaction(ConnectionManager connMgr, QueryCursor<List<?>> cur) {
        switch (mode) {
            case MERGE:
                return new InsertIterator(cur, this, EnlistOperation.UPSERT);
            case INSERT:
                return new InsertIterator(cur, this, EnlistOperation.INSERT);
            case UPDATE:
                return new UpdateIterator(cur, this, EnlistOperation.UPDATE);
            case DELETE:
                return new DeleteIterator( cur, this, EnlistOperation.DELETE);

            default:
                throw new IllegalArgumentException(String.valueOf(mode));
        }
    }

    /**
     * @param updMode Update plan mode.
     * @return Operation.
     */
    public static EnlistOperation enlistOperation(UpdateMode updMode) {
        switch (updMode) {
            case INSERT:
                return EnlistOperation.INSERT;
            case MERGE:
                return EnlistOperation.UPSERT;
            case UPDATE:
                return EnlistOperation.UPDATE;
            case DELETE:
                return EnlistOperation.DELETE;
            default:
                throw new IllegalArgumentException(String.valueOf(updMode));
        }
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
        return tbl.cacheContext();
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
    public boolean isLocalSubquery() {
        return isLocSubqry;
    }

    /**
     * @param args Query parameters.
     * @return Iterator.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteBiTuple getFastRow(Object[] args) throws IgniteCheckedException {
        if (fastUpdate != null)
            return fastUpdate.getRow(args);

        return null;
    }

    /**
     * @param row Row.
     * @return Resulting entry.
     * @throws IgniteCheckedException If failed.
     */
    public Object processRowForTx(List<?> row) throws IgniteCheckedException {
        switch (mode()) {
            case INSERT:
            case MERGE:
                return processRow(row);

            case UPDATE: {
                T3<Object, Object, Object> row0 = processRowForUpdate(row);

                return new IgniteBiTuple<>(row0.get1(), row0.get3());
            }
            case DELETE:
                return row.get(0);

            default:
                throw new UnsupportedOperationException(String.valueOf(mode()));
        }
    }

    /**
     * Abstract iterator.
     */
    private abstract static class AbstractIterator extends GridCloseableIteratorAdapterEx<Object>
        implements UpdateSourceIterator<Object> {
        /** */
        private final QueryCursor<List<?>> cur;

        /** */
        protected final UpdatePlan plan;

        /** */
        private final Iterator<List<?>> it;

        /** */
        private final EnlistOperation op;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param op Operation.
         */
        private AbstractIterator(QueryCursor<List<?>> cur, UpdatePlan plan,
            EnlistOperation op) {
            this.cur = cur;
            this.plan = plan;
            this.op = op;

            it = cur.iterator();
        }

        /** {@inheritDoc} */
        @Override public EnlistOperation operation() {
            return op;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() {
            cur.close();
        }

        /** {@inheritDoc} */
        @Override protected Object onNext() throws IgniteCheckedException {
            return process(it.next());
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return it.hasNext();
        }

        /** */
        protected abstract Object process(List<?> row) throws IgniteCheckedException;
    }

    /** */
    private static final class UpdateIterator extends AbstractIterator {
        /** */
        private static final long serialVersionUID = -4949035950470324961L;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param op Operation.
         */
        private UpdateIterator(QueryCursor<List<?>> cur, UpdatePlan plan,
            EnlistOperation op) {
            super(cur, plan, op);
        }

        /** {@inheritDoc} */
        @Override protected Object process(List<?> row) throws IgniteCheckedException {
            T3<Object, Object, Object> row0 = plan.processRowForUpdate(row);

            return new IgniteBiTuple<>(row0.get1(), row0.get3());
        }
    }

    /** */
    private static final class DeleteIterator extends AbstractIterator {
        /** */
        private static final long serialVersionUID = -4949035950470324961L;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param op Operation.
         */
        private DeleteIterator(QueryCursor<List<?>> cur, UpdatePlan plan,
            EnlistOperation op) {
            super(cur, plan, op);
        }

        /** {@inheritDoc} */
        @Override protected Object process(List<?> row) throws IgniteCheckedException {
            return row.get(0);
        }
    }

    /** */
    private static final class InsertIterator extends AbstractIterator {
        /** */
        private static final long serialVersionUID = -4949035950470324961L;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param op Operation.
         */
        private InsertIterator(QueryCursor<List<?>> cur, UpdatePlan plan,
            EnlistOperation op) {
            super(cur, plan, op);
        }

        /** {@inheritDoc} */
        @Override protected Object process(List<?> row) throws IgniteCheckedException {
            return plan.processRow(row);
        }
    }
}
