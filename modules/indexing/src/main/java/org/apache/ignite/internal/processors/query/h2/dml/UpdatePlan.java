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
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
     * @param args Original query arguments.
     * @return Rows from plan.
     * @throws IgniteCheckedException if failed.
     */
    public List<List<?>> createRows(Object[] args) throws IgniteCheckedException {
        assert rowsNum > 0 && !F.isEmpty(colNames);

        List<List<?>> res = new ArrayList<>(rowsNum);

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        for (List<DmlArgument> row : rows) {
            List<Object> resRow = new ArrayList<>();

            for (int j = 0; j < colNames.length; j++) {
                Object colVal = row.get(j).get(args);

                if (j == keyColIdx || j == valColIdx) {
                    Class<?> colCls = j == keyColIdx ? desc.type().keyClass() : desc.type().valueClass();

                    colVal = DmlUtils.convert(colVal, desc, colCls, colTypes[j]);
                }

                resRow.add(colVal);
            }

            res.add(resRow);
        }

        return res;
    }

    /**
     * Create iterator for transaction.
     *
     * @param cur Cursor.
     * @param topVer Topology version.
     * @return Iterator.
     */
    public GridCloseableIteratorAdapter<?> iteratorForTransaction(QueryCursorImpl<List<?>> cur,
        AffinityTopologyVersion topVer) {
        switch (mode) {
            case INSERT:
                return new InsertIterator(cur, this, topVer);
            case UPDATE:
                return new UpdateIterator(cur, this, topVer);
            case DELETE:
                return new DeleteIterator(cur, this, topVer);

            default:
                throw new UnsupportedOperationException(String.valueOf(mode));
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
     * Abstract iterator.
     */
    private abstract static class AbstractIterator extends GridCloseableIteratorAdapter<Object> {
        /** */
        protected final QueryCursor<List<?>> cur;

        /** */
        protected final UpdatePlan plan;

        /** */
        protected final Iterator<List<?>> it;

        /** */
        protected final GridCacheContext cctx;

        /** */
        protected final AffinityTopologyVersion topVer;

        /** */
        protected final GridCacheAffinityManager affinity;

        /** */
        protected Object curr;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private AbstractIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            this.cur = cur;
            this.plan = plan;
            this.topVer = topVer;

            it = cur.iterator();
            cctx = plan.cacheContext();
            affinity = cctx.affinity();
        }

        /** {@inheritDoc} */
        @Override protected Object onNext() throws IgniteCheckedException {
            advance();

            if(curr == null)
                throw new NoSuchElementException();

            Object res = curr;

            curr = null;

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            advance();

            return curr != null;
        }

        /** */
        protected abstract void advance() throws IgniteCheckedException;

        /** {@inheritDoc} */
        @Override protected void onClose() {
            cur.close();
        }
    }

    /** */
    private final class UpdateIterator extends AbstractIterator {
        /** */
        private final boolean bin;

        /** */
        private final GridH2RowDescriptor desc;

        /** */
        private final boolean hasNewVal;

        /** */
        private final boolean hasProps;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private UpdateIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            super(cur, plan, topVer);

            bin = cctx.binaryMarshaller();
            desc = plan.tbl.rowDescriptor();

            hasNewVal = (plan.valColIdx != -1);
            hasProps = !hasNewVal || plan.colNames.length > 1;
        }

        /** {@inheritDoc} */
        @Override protected void advance() throws IgniteCheckedException {
            if(curr != null)
                return;

            if (it.hasNext()) {
                List<?> row = it.next();

                Map<String, Object> newColVals = new HashMap<>();

                for (int i = 0; i < plan.colNames.length; i++) {
                    if (hasNewVal && i == plan.valColIdx - 2)
                        continue;

                    GridQueryProperty prop = plan.tbl.rowDescriptor().type().property(plan.colNames[i]);

                    assert prop != null : "Unknown property: " + plan.colNames[i];

                    newColVals.put(plan.colNames[i], DmlUtils.convert(row.get(i + 2), desc, prop.type(), plan.colTypes[i]));
                }

                Object newVal = plan.valSupplier.apply(row);

                if (newVal == null)
                    throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

                // Skip key and value - that's why we start off with 3rd column
                for (int i = 0; i < plan.tbl.getColumns().length - DEFAULT_COLUMNS_COUNT; i++) {
                    Column c = plan.tbl.getColumn(i + DEFAULT_COLUMNS_COUNT);

                    if (desc.isKeyValueOrVersionColumn(c.getColumnId()))
                        continue;

                    GridQueryProperty prop = desc.type().property(c.getName());

                    if (prop.key())
                        continue; // Don't get values of key's columns - we won't use them anyway

                    boolean hasNewColVal = newColVals.containsKey(c.getName());

                    if (!hasNewColVal)
                        continue;

                    Object colVal = newColVals.get(c.getName());

                    // UPDATE currently does not allow to modify key or its fields, so we must be safe to pass null as key.
                    desc.setColumnValue(null, newVal, colVal, i);
                }

                if (bin && hasProps) {
                    assert newVal instanceof BinaryObjectBuilder;

                    newVal = ((BinaryObjectBuilder)newVal).build();
                }

                desc.type().validateKeyAndValue(row.get(0), newVal);

                curr = new Object[] {row.get(0), newVal};
            }
        }
    }

    /** */
    private final class DeleteIterator extends AbstractIterator {

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private DeleteIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            super(cur, plan, topVer);
        }

        /** {@inheritDoc} */
        @Override protected void advance() {
            if(curr != null)
                return;

            if (it.hasNext())
                curr = it.next().get(0);
        }
    }

    /** */
    private final class InsertIterator extends AbstractIterator {
        /** */
        private final GridH2RowDescriptor rowDesc;

        /** */
        private final GridQueryTypeDescriptor desc;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private InsertIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            super(cur, plan, topVer);

            rowDesc = plan.tbl.rowDescriptor();
            desc = rowDesc.type();
        }

        /** {@inheritDoc} */
        @Override protected void advance() throws IgniteCheckedException {
            if(curr != null)
                return;

            while (it.hasNext()) {
                List<?> row = it.next();

                Object key = plan.keySupplier.apply(row);

                if (QueryUtils.isSqlType(desc.keyClass())) {
                    assert plan.keyColIdx != -1;

                    key = DmlUtils.convert(key, rowDesc, desc.keyClass(), plan.colTypes[plan.keyColIdx]);
                }

                if (key == null) {
                    if (F.isEmpty(desc.keyFieldName()))
                        throw new IgniteSQLException("Key for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_KEY);
                    else
                        throw new IgniteSQLException("Null value is not allowed for column '" + desc.keyFieldName() + "'",
                            IgniteQueryErrorCode.NULL_KEY);
                }

                Map<String, Object> newColVals = new HashMap<>();

                for (int i = 0; i < plan.colNames.length; i++) {
                    if (i == plan.keyColIdx || i == plan.valColIdx)
                        continue;

                    String colName = plan.colNames[i];

                    GridQueryProperty prop = desc.property(colName);

                    assert prop != null;

                    Class<?> expCls = prop.type();

                    newColVals.put(colName, DmlUtils.convert(row.get(i), rowDesc, expCls, plan.colTypes[i]));
                }

                // We update columns in the order specified by the table for a reason - table's
                // column order preserves their precedence for correct update of nested properties.
                Column[] cols = plan.tbl.getColumns();

                // Init key
                for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
                    if (plan.tbl.rowDescriptor().isKeyValueOrVersionColumn(i))
                        continue;

                    String colName = cols[i].getName();

                    GridQueryProperty prop;

                    if (newColVals.containsKey(colName) && (prop = desc.property(colName)).key())
                        prop.setValue(key, null, newColVals.remove(colName));
                }

                if (cctx.binaryMarshaller()&& key instanceof BinaryObjectBuilder)
                    key = ((BinaryObjectBuilder)key).build();

                if (affinity.primaryByKey(cctx.localNode(), key, topVer)) {
                    Object val = plan.valSupplier.apply(row);

                    if (QueryUtils.isSqlType(desc.valueClass())) {
                        assert plan.valColIdx != -1;

                        val = DmlUtils.convert(val, rowDesc, desc.valueClass(), plan.colTypes[plan.valColIdx]);
                    }

                    if (val == null) {
                        if (F.isEmpty(desc.valueFieldName()))
                            throw new IgniteSQLException("Value for INSERT, MERGE, or UPDATE must not be null",
                                IgniteQueryErrorCode.NULL_VALUE);
                        else
                            throw new IgniteSQLException("Null value is not allowed for column '" + desc.valueFieldName() + "'",
                                IgniteQueryErrorCode.NULL_VALUE);
                    }

                    // Init value
                    for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
                        if (plan.tbl.rowDescriptor().isKeyValueOrVersionColumn(i))
                            continue;

                        String colName = cols[i].getName();
                        Object colVal = newColVals.get(colName);

                        if (colVal != null)
                            desc.setValue(colName, null, val, colVal);
                    }

                    if (cctx.binaryMarshaller()&& val instanceof BinaryObjectBuilder)
                        val = ((BinaryObjectBuilder)val).build();

                    desc.validateKeyAndValue(key, val);

                    curr = new Object[] {key, null, new DmlTxInsertEntryProcessor(val), null};

                    return;
                }
            }
        }
    }
}
