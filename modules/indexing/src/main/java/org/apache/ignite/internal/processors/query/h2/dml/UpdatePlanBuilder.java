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

import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.H2StatementCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlMerge;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUpdate;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.h2.command.Prepared;
import org.h2.table.Column;

/**
 * Logic for building update plans performed by {@link DmlStatementsProcessor}.
 */
public final class UpdatePlanBuilder {
    /** Converter from GridSqlColumn to Column. */
    private static final IgniteClosure<GridSqlColumn, Column> TO_H2_COL =
        (IgniteClosure<GridSqlColumn, Column>)GridSqlColumn::column;

    /** Allow hidden key value columns at the INSERT/UPDATE/MERGE statements (not final for tests). */
    private static boolean ALLOW_KEY_VAL_UPDATES = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_SQL_ALLOW_KEY_VAL_UPDATES, false);

    /**
     * Constructor.
     */
    private UpdatePlanBuilder() {
        // No-op.
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     *
     * @param planKey Plan key.
     * @param stmt Statement.
     * @param mvccEnabled MVCC enabled flag.
     * @param idx Indexing.
     * @return Update plan.
     */
    @SuppressWarnings("ConstantConditions")
    public static UpdatePlan planForStatement(
        QueryDescriptor planKey,
        GridSqlStatement stmt,
        boolean mvccEnabled,
        IgniteH2Indexing idx,
        IgniteLogger log
    ) throws IgniteCheckedException {
        if (stmt instanceof GridSqlMerge || stmt instanceof GridSqlInsert)
            return planForInsert(planKey, stmt, idx, mvccEnabled, log);
        else if (stmt instanceof GridSqlUpdate || stmt instanceof GridSqlDelete)
            return planForUpdate(planKey, stmt, idx, mvccEnabled, log);
        else
            throw new IgniteSQLException("Unsupported operation: " + stmt.getSQL(),
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Prepare update plan for INSERT or MERGE.
     *
     * @param planKey Plan key.
     * @param stmt INSERT or MERGE statement.
     * @param idx Indexing.
     * @param mvccEnabled Mvcc flag.
     * @return Update plan.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ConstantConditions")
    private static UpdatePlan planForInsert(
        QueryDescriptor planKey,
        GridSqlStatement stmt,
        IgniteH2Indexing idx,
        boolean mvccEnabled,
        IgniteLogger log
    ) throws IgniteCheckedException {
        GridSqlQuery sel = null;

        GridSqlElement target;

        GridSqlColumn[] cols;

        boolean isTwoStepSubqry;

        int rowsNum;

        GridSqlTable tbl;

        GridH2RowDescriptor desc;

        List<GridSqlElement[]> elRows = null;

        UpdateMode mode;

        if (stmt instanceof GridSqlInsert) {
            mode = UpdateMode.INSERT;

            GridSqlInsert ins = (GridSqlInsert) stmt;

            target = ins.into();

            tbl = DmlAstUtils.gridTableForElement(target);

            GridH2Table h2Tbl = tbl.dataTable();

            assert h2Tbl != null;

            desc = h2Tbl.rowDescriptor();

            cols = ins.columns();

            if (noQuery(ins.rows()))
                elRows = ins.rows();
            else
                sel = DmlAstUtils.selectForInsertOrMerge(cols, ins.rows(), ins.query());

            isTwoStepSubqry = (ins.query() != null);
            rowsNum = isTwoStepSubqry ? 0 : ins.rows().size();
        }
        else if (stmt instanceof GridSqlMerge) {
            mode = UpdateMode.MERGE;

            GridSqlMerge merge = (GridSqlMerge) stmt;

            target = merge.into();

            tbl = DmlAstUtils.gridTableForElement(target);
            desc = tbl.dataTable().rowDescriptor();

            cols = merge.columns();

            if (noQuery(merge.rows()))
                elRows = merge.rows();
            else
                sel = DmlAstUtils.selectForInsertOrMerge(cols, merge.rows(), merge.query());

            isTwoStepSubqry = (merge.query() != null);
            rowsNum = isTwoStepSubqry ? 0 : merge.rows().size();
        }
        else {
            throw new IgniteSQLException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);
        }

        // Let's set the flag only for subqueries that have their FROM specified.
        isTwoStepSubqry &= (sel != null && (sel instanceof GridSqlUnion ||
            (sel instanceof GridSqlSelect && ((GridSqlSelect) sel).from() != null)));

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        if (desc == null)
            throw new IgniteSQLException("Row descriptor undefined for table '" + tbl.dataTable().getName() + "'",
                IgniteQueryErrorCode.NULL_TABLE_DESCRIPTOR);

        GridCacheContext<?, ?> cctx = desc.context();

        String[] colNames = new String[cols.length];

        int[] colTypes = new int[cols.length];

        for (int i = 0; i < cols.length; i++) {
            GridSqlColumn col = cols[i];

            String colName = col.columnName();

            colNames[i] = colName;

            colTypes[i] = col.resultType().type();

            int colId = col.column().getColumnId();

            if (desc.isKeyColumn(colId)) {
                keyColIdx = i;

                continue;
            }

            if (desc.isValueColumn(colId)) {
                valColIdx = i;

                continue;
            }

            GridQueryProperty prop = desc.type().property(colName);

            assert prop != null : "Property '" + colName + "' not found.";

            if (prop.key())
                hasKeyProps = true;
            else
                hasValProps = true;
        }

        verifyDmlColumns(tbl.dataTable(), F.viewReadOnly(Arrays.asList(cols), TO_H2_COL));

        KeyValueSupplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true, false);
        KeyValueSupplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false, false);

        String selectSql = sel != null ? sel.getSQL() : null;

        DmlDistributedPlanInfo distributed = null;

        if (rowsNum == 0 && !F.isEmpty(selectSql)) {
            distributed = checkPlanCanBeDistributed(
                idx,
                mvccEnabled,
                planKey,
                selectSql,
                tbl.dataTable().cacheName(),
                log
            );
        }

        List<List<DmlArgument>> rows = null;

        if (elRows != null) {
            assert sel == null;

            rows = new ArrayList<>(elRows.size());

            for (GridSqlElement[] elRow : elRows) {
                List<DmlArgument> row = new ArrayList<>(cols.length);

                for (GridSqlElement el : elRow) {
                    DmlArgument arg = DmlArguments.create(el);

                    row.add(arg);
                }

                rows.add(row);
            }
        }

        return new UpdatePlan(
            mode,
            tbl.dataTable(),
            colNames,
            colTypes,
            keySupplier,
            valSupplier,
            keyColIdx,
            valColIdx,
            selectSql,
            !isTwoStepSubqry,
            rows,
            rowsNum,
            null,
            distributed
        );
    }

    /**
     * @param rows Insert rows.
     * @return {@code True} if no query optimisation may be used.
     */
    private static boolean noQuery(List<GridSqlElement[]> rows) {
        if (F.isEmpty(rows))
            return false;

        boolean noQry = true;

        for (int i = 0; i < rows.size(); i++) {
            GridSqlElement[] row = rows.get(i);

            for (int i1 = 0; i1 < row.length; i1++) {
                GridSqlElement el = row[i1];

                if (!(noQry &= (el instanceof GridSqlConst || el instanceof GridSqlParameter)))
                    return noQry;

            }
        }

        return true;
    }

    /**
     * Prepare update plan for UPDATE or DELETE.
     *
     * @param planKey Plan key.
     * @param stmt UPDATE or DELETE statement.
     * @param idx Indexing.
     * @param mvccEnabled MVCC flag.
     * @return Update plan.
     * @throws IgniteCheckedException if failed.
     */
    private static UpdatePlan planForUpdate(
        QueryDescriptor planKey,
        GridSqlStatement stmt,
        IgniteH2Indexing idx,
        boolean mvccEnabled,
        IgniteLogger log
    ) throws IgniteCheckedException {
        GridSqlElement target;

        FastUpdate fastUpdate;

        UpdateMode mode;

        if (stmt instanceof GridSqlUpdate) {
            // Let's verify that user is not trying to mess with key's columns directly
            verifyUpdateColumns(stmt);

            GridSqlUpdate update = (GridSqlUpdate)stmt;
            target = update.target();
            fastUpdate = DmlAstUtils.getFastUpdateArgs(update);
            mode = UpdateMode.UPDATE;
        }
        else if (stmt instanceof GridSqlDelete) {
            GridSqlDelete del = (GridSqlDelete) stmt;
            target = del.from();
            fastUpdate = DmlAstUtils.getFastDeleteArgs(del);
            mode = UpdateMode.DELETE;
        }
        else
            throw new IgniteSQLException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);

        GridSqlTable tbl = DmlAstUtils.gridTableForElement(target);

        GridH2Table h2Tbl = tbl.dataTable();

        assert h2Tbl != null;

        GridH2RowDescriptor desc = h2Tbl.rowDescriptor();

        if (desc == null)
            throw new IgniteSQLException("Row descriptor undefined for table '" + h2Tbl.getName() + "'",
                IgniteQueryErrorCode.NULL_TABLE_DESCRIPTOR);

        if (fastUpdate != null) {
            return new UpdatePlan(
                mode,
                h2Tbl,
                null,
                fastUpdate,
                null
            );
        }
        else {
            GridSqlSelect sel;

            if (stmt instanceof GridSqlUpdate) {
                List<GridSqlColumn> updatedCols = ((GridSqlUpdate) stmt).cols();

                int valColIdx = -1;

                String[] colNames = new String[updatedCols.size()];

                int[] colTypes = new int[updatedCols.size()];

                for (int i = 0; i < updatedCols.size(); i++) {
                    colNames[i] = updatedCols.get(i).columnName();

                    colTypes[i] = updatedCols.get(i).resultType().type();

                    Column col = updatedCols.get(i).column();

                    if (desc.isValueColumn(col.getColumnId()))
                        valColIdx = i;
                }

                boolean hasNewVal = (valColIdx != -1);

                // Statement updates distinct properties if it does not have _val in updated columns list
                // or if its list of updated columns includes only _val, i.e. is single element.
                boolean hasProps = !hasNewVal || updatedCols.size() > 1;

                // Index of new _val in results of SELECT
                if (hasNewVal)
                    valColIdx += 2;

                int newValColIdx = (hasNewVal ? valColIdx : 1);

                KeyValueSupplier valSupplier = createSupplier(desc.context(), desc.type(), newValColIdx, hasProps,
                    false, true);

                sel = DmlAstUtils.selectForUpdate((GridSqlUpdate)stmt);

                String selectSql = sel.getSQL();

                DmlDistributedPlanInfo distributed = null;

                if (!F.isEmpty(selectSql)) {
                    distributed = checkPlanCanBeDistributed(
                        idx,
                        mvccEnabled,
                        planKey,
                        selectSql,
                        tbl.dataTable().cacheName(),
                        log
                    );
                }

                return new UpdatePlan(
                    UpdateMode.UPDATE,
                    h2Tbl,
                    colNames,
                    colTypes,
                    null,
                    valSupplier,
                    -1,
                    valColIdx,
                    selectSql,
                    false,
                    null,
                    0,
                    null,
                    distributed
                );
            }
            else {
                sel = DmlAstUtils.selectForDelete((GridSqlDelete)stmt);

                String selectSql = sel.getSQL();

                DmlDistributedPlanInfo distributed = null;

                if (!F.isEmpty(selectSql)) {
                    distributed = checkPlanCanBeDistributed(
                        idx,
                        mvccEnabled,
                        planKey,
                        selectSql,
                        tbl.dataTable().cacheName(),
                        log
                    );
                }

                return new UpdatePlan(
                    UpdateMode.DELETE,
                    h2Tbl,
                    selectSql,
                    null,
                    distributed
                );
            }
        }
    }

    /**
     * Prepare update plan for COPY command (AKA bulk load).
     *
     * @param cmd Bulk load command
     * @return The update plan for this command.
     * @throws IgniteCheckedException if failed.
     */
    public static UpdatePlan planForBulkLoad(SqlBulkLoadCommand cmd, GridH2Table tbl) throws IgniteCheckedException {
        GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc == null)
            throw new IgniteSQLException("Row descriptor undefined for table '" + tbl.getName() + "'",
                IgniteQueryErrorCode.NULL_TABLE_DESCRIPTOR);

        GridCacheContext<?, ?> cctx = desc.context();

        List<String> cols = cmd.columns();

        if (cols == null)
            throw new IgniteSQLException("Columns are not defined", IgniteQueryErrorCode.NULL_TABLE_DESCRIPTOR);

        String[] colNames = new String[cols.size()];

        Column[] h2Cols = new Column[cols.size()];

        int[] colTypes = new int[cols.size()];

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        for (int i = 0; i < cols.size(); i++) {
            String colName = cols.get(i);

            colNames[i] = colName;

            Column h2Col = tbl.getColumn(colName);

            h2Cols[i] = h2Col;

            colTypes[i] = h2Col.getType();
            int colId = h2Col.getColumnId();

            if (desc.isKeyColumn(colId)) {
                keyColIdx = i;
                continue;
            }

            if (desc.isValueColumn(colId)) {
                valColIdx = i;
                continue;
            }

            GridQueryProperty prop = desc.type().property(colName);

            assert prop != null : "Property '" + colName + "' not found.";

            if (prop.key())
                hasKeyProps = true;
            else
                hasValProps = true;
        }

        verifyDmlColumns(tbl, Arrays.asList(h2Cols));

        KeyValueSupplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps,
            true, false);
        KeyValueSupplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps,
            false, false);

        return new UpdatePlan(
            UpdateMode.BULK_LOAD,
            tbl,
            colNames,
            colTypes,
            keySupplier,
            valSupplier,
            keyColIdx,
            valColIdx,
            null,
            true,
            null,
            0,
            null,
            null
        );
    }

    /**
     * Detect appropriate method of instantiating key or value (take from param, create binary builder,
     * invoke default ctor, or allocate).
     *
     * @param cctx Cache context.
     * @param desc Table descriptor.
     * @param colIdx Column index if key or value is present in columns list, {@code -1} if it's not.
     * @param hasProps Whether column list affects individual properties of key or value.
     * @param key Whether supplier should be created for key or for value.
     * @param forUpdate {@code FOR UPDATE} flag.
     * @return Closure returning key or value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked", "IfMayBeConditional"})
    private static KeyValueSupplier createSupplier(final GridCacheContext<?, ?> cctx, GridQueryTypeDescriptor desc,
        final int colIdx, boolean hasProps, final boolean key, boolean forUpdate) throws IgniteCheckedException {
        final String typeName = key ? desc.keyTypeName() : desc.valueTypeName();

        //Try to find class for the key locally.
        final Class<?> cls = key ? U.firstNotNull(U.classForName(desc.keyTypeName(), null), desc.keyClass())
            : desc.valueClass();

        boolean isSqlType = QueryUtils.isSqlType(cls);

        // If we don't need to construct anything from scratch, just return value from given list.
        if (isSqlType || !hasProps) {
            if (colIdx != -1)
                return new PlainValueSupplier(colIdx);
            else if (isSqlType)
                // Non constructable keys and values (SQL types) must be present in the query explicitly.
                throw new IgniteCheckedException((key ? "Key" : "Value") + " is missing from query");
        }

        if (cctx.binaryMarshaller()) {
            if (colIdx != -1) {
                // If we have key or value explicitly present in query, create new builder upon them...
                return new KeyValueSupplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) {
                        Object obj = arg.get(colIdx);

                        if (obj == null)
                            return null;

                        BinaryObject bin = cctx.grid().binary().toBinary(obj);

                        BinaryObjectBuilder builder = cctx.grid().binary().builder(bin);

                        cctx.prepareAffinityField(builder);

                        return builder;
                    }
                };
            }
            else {
                // ...and if we don't, just create a new builder.
                return new KeyValueSupplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) {
                        BinaryObjectBuilder builder = cctx.grid().binary().builder(typeName);

                        cctx.prepareAffinityField(builder);

                        return builder;
                    }
                };
            }
        }
        else {
            if (colIdx != -1) {
                if (forUpdate && colIdx == 1) {
                    // It's the case when the old value has to be taken as the basis for the new one on UPDATE,
                    // so we have to clone it. And on UPDATE we don't expect any key supplier.
                    assert !key;

                    return new KeyValueSupplier() {
                        /** {@inheritDoc} */
                        @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                            byte[] oldPropBytes = cctx.marshaller().marshal(arg.get(1));

                            // colVal is another object now, we can mutate it
                            return cctx.marshaller().unmarshal(oldPropBytes, U.resolveClassLoader(cctx.gridConfig()));
                        }
                    };
                }
                else // We either are not updating, or the new value is given explicitly, no cloning needed.
                    return new PlainValueSupplier(colIdx);
            }

            Constructor<?> ctor;

            try {
                ctor = cls.getDeclaredConstructor();
                ctor.setAccessible(true);
            }
            catch (NoSuchMethodException | SecurityException ignored) {
                ctor = null;
            }

            if (ctor != null) {
                final Constructor<?> ctor0 = ctor;

                // Use default ctor, if it's present...
                return new KeyValueSupplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        try {
                            return ctor0.newInstance();
                        }
                        catch (Exception e) {
                            if (S.includeSensitive())
                                throw new IgniteCheckedException("Failed to instantiate " +
                                    (key ? "key" : "value") + " [type=" + typeName + ']', e);
                            else
                                throw new IgniteCheckedException("Failed to instantiate " +
                                    (key ? "key" : "value") + '.', e);
                        }
                    }
                };
            }
            else {
                // ...or allocate new instance with unsafe, if it's not
                return new KeyValueSupplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        try {
                            return GridUnsafe.allocateInstance(cls);
                        }
                        catch (InstantiationException e) {
                            if (S.includeSensitive())
                                throw new IgniteCheckedException("Failed to instantiate " +
                                    (key ? "key" : "value") + " [type=" + typeName + ']', e);
                            else
                                throw new IgniteCheckedException("Failed to instantiate " +
                                    (key ? "key" : "value") + '.', e);
                        }
                    }
                };
            }
        }
    }

    /**
     * Check that UPDATE statement affects no key columns.
     *
     * @param statement Statement.
     */
    private static void verifyUpdateColumns(GridSqlStatement statement) {
        if (!(statement instanceof GridSqlUpdate))
            return;

        GridSqlUpdate update = (GridSqlUpdate) statement;

        GridSqlElement updTarget = update.target();

        Set<GridSqlTable> tbls = new HashSet<>();

        DmlAstUtils.collectAllGridTablesInTarget(updTarget, tbls);

        if (tbls.size() != 1)
            throw new IgniteSQLException("Failed to determine target table for UPDATE",
                IgniteQueryErrorCode.TABLE_NOT_FOUND);

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        if (gridTbl != null && updateAffectsKeyColumns(gridTbl, update.set().keySet()))
            throw new IgniteSQLException("SQL UPDATE can't modify key or its fields directly",
                IgniteQueryErrorCode.KEY_UPDATE);

        if (gridTbl != null)
            verifyDmlColumns(gridTbl, F.viewReadOnly(update.cols(), TO_H2_COL));
    }

    /**
     * Check if given set of modified columns intersects with the set of SQL properties of the key.
     *
     * @param gridTbl Table.
     * @param affectedColNames Column names.
     * @return {@code true} if any of given columns corresponds to the key or any of its properties.
     */
    private static boolean updateAffectsKeyColumns(GridH2Table gridTbl, Set<String> affectedColNames) {
        GridH2RowDescriptor desc = gridTbl.rowDescriptor();

        for (String colName : affectedColNames) {
            int colId = gridTbl.getColumn(colName).getColumnId();

            // Check "_key" column and alias key column
            if (desc.isKeyColumn(colId))
                return true;

            // column ids 0..1 are _key, _val
            if (colId >= QueryUtils.DEFAULT_COLUMNS_COUNT) {
                if (desc.isColumnKeyProperty(colId - QueryUtils.DEFAULT_COLUMNS_COUNT))
                    return true;
            }
        }
        return false;
    }

    /**
     * Checks that DML query (insert, merge, update, bulk load aka copy) columns: <br/>
     * 1) doesn't contain both entire key (_key or alias) and columns referring to part of the key; <br/>
     * 2) doesn't contain both entire value (_val or alias) and columns referring to part of the value. <br/>
     *
     * @param tab - updated table.
     * @param affectedCols - table's column names affected by dml query. Their order should be the same as in the
     * dml statement only to have the same columns order in the error message.
     * @throws IgniteSQLException if check failed.
     */
    private static void verifyDmlColumns(GridH2Table tab, Collection<Column> affectedCols) {
        GridH2RowDescriptor desc = tab.rowDescriptor();

        // _key (_val) or it alias exist in the update columns.
        String keyColName = null;
        String valColName = null;

        // Whether fields that are part of the key (value) exist in the updated columns.
        boolean hasKeyProps = false;
        boolean hasValProps = false;

        for (Column col : affectedCols) {
            int colId = col.getColumnId();

            // At first, let's define whether column refers to entire key, entire value or one of key/val fields.
            // Checking that it's not specified both _key(_val) and its alias by the way.
            if (desc.isKeyColumn(colId)) {
                if (keyColName == null)
                    keyColName = col.getName();
                else
                    throw new IgniteSQLException(
                        "Columns " + keyColName + " and " + col + " both refer to entire cache key object.",
                        IgniteQueryErrorCode.PARSING);
            }
            else if (desc.isValueColumn(colId)) {
                if (valColName == null)
                    valColName = col.getName();
                else
                    throw new IgniteSQLException(
                        "Columns " + valColName + " and " + col + " both refer to entire cache value object.",
                        IgniteQueryErrorCode.PARSING);

            }
            else {
                // Column ids 0..2 are _key, _val, _ver
                assert colId >= QueryUtils.DEFAULT_COLUMNS_COUNT :
                    "Unexpected column [name=" + col + ", id=" + colId + "].";

                if (desc.isColumnKeyProperty(colId - QueryUtils.DEFAULT_COLUMNS_COUNT))
                    hasKeyProps = true;
                else
                    hasValProps = true;
            }

            // And check invariants for the fast fail.
            boolean hasEntireKeyCol = keyColName != null;
            boolean hasEntireValcol = valColName != null;

            if (hasEntireKeyCol && hasKeyProps)
                throw new IgniteSQLException("Column " + keyColName + " refers to entire key cache object. " +
                    "It must not be mixed with other columns that refer to parts of key.",
                    IgniteQueryErrorCode.PARSING);

            if (hasEntireValcol && hasValProps)
                throw new IgniteSQLException("Column " + valColName + " refers to entire value cache object. " +
                    "It must not be mixed with other columns that refer to parts of value.",
                    IgniteQueryErrorCode.PARSING);

            if (!ALLOW_KEY_VAL_UPDATES) {
                if (desc.isKeyColumn(colId) && !QueryUtils.isSqlType(desc.type().keyClass())) {
                    throw new IgniteSQLException(
                        "Update of composite key column is not supported",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION
                    );
                }

                if (desc.isValueColumn(colId) && !QueryUtils.isSqlType(desc.type().valueClass())) {
                    throw new IgniteSQLException(
                        "Update of composite value column is not supported",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION
                    );
                }
            }
        }
    }

    /**
     * Checks whether the given update plan can be distributed and returns additional info.
     *
     * @param idx Indexing.
     * @param mvccEnabled Mvcc flag.
     * @param planKey Plan key.
     * @param selectQry Derived select query.
     * @param cacheName Cache name.
     * @return distributed update plan info, or {@code null} if cannot be distributed.
     * @throws IgniteCheckedException if failed.
     */
    private static DmlDistributedPlanInfo checkPlanCanBeDistributed(
        IgniteH2Indexing idx,
        boolean mvccEnabled,
        QueryDescriptor planKey,
        String selectQry,
        String cacheName,
        IgniteLogger log
    )
        throws IgniteCheckedException {
        if ((!mvccEnabled && !planKey.skipReducerOnUpdate()) || planKey.batched())
            return null;

        try (H2PooledConnection conn = idx.connections().connection(planKey.schemaName())) {
            H2Utils.setupConnection(conn,
                QueryContext.parseContext(idx.backupFilter(null, null), planKey.local()),
                planKey.distributedJoins(),
                planKey.enforceJoinOrder());

            // Get a new prepared statement for derived select query.
            try (PreparedStatement stmt = conn.prepareStatement(selectQry, H2StatementCache.queryFlags(planKey))) {
                Prepared prep = GridSqlQueryParser.prepared(stmt);
                GridSqlQuery selectStmt = (GridSqlQuery)new GridSqlQueryParser(false, log).parse(prep);

                GridCacheTwoStepQuery qry = GridSqlQuerySplitter.split(
                    conn,
                    selectStmt,
                    selectQry,
                    planKey.collocated(),
                    planKey.distributedJoins(),
                    planKey.enforceJoinOrder(),
                    false,
                    idx,
                    prep.getParameters().size(),
                    log
                );

                boolean distributed =
                    !qry.isLocalSplit() &&                                                    // No split for local
                    qry.hasCacheIds() &&                                                      // Over real caches
                    qry.skipMergeTable() &&                                                   // No merge table
                    qry.mapQueries().size() == 1 && !qry.mapQueries().get(0).hasSubQueries(); // One w/o subqueries

                if (distributed) {
                    List<Integer> cacheIds = H2Utils.collectCacheIds(idx, CU.cacheId(cacheName), qry.tables());

                    H2Utils.checkQuery(idx, cacheIds, qry.tables());

                    return new DmlDistributedPlanInfo(qry.isReplicatedOnly(), cacheIds, qry.derivedPartitions());
                }
                else
                    return null;
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Simple supplier that just takes specified element of a given row.
     */
    private static final class PlainValueSupplier implements KeyValueSupplier {
        /** Index of column to use. */
        private final int colIdx;

        /**
         * Constructor.
         *
         * @param colIdx Column index.
         */
        private PlainValueSupplier(int colIdx) {
            this.colIdx = colIdx;
        }

        /** {@inheritDoc} */
        @Override public Object apply(List<?> arg) {
            return arg.get(colIdx);
        }
    }
}
