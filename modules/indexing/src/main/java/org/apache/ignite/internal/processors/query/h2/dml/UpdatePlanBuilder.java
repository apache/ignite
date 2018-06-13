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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlMerge;
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
import org.h2.command.Prepared;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 * Logic for building update plans performed by {@link DmlStatementsProcessor}.
 */
public final class UpdatePlanBuilder {
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
     * @param prepared H2's {@link Prepared}.
     * @param loc Local query flag.
     * @param idx Indexing.
     * @param conn Connection.
     * @param fieldsQry Original query.
     * @return Update plan.
     */
    public static UpdatePlan planForStatement(Prepared prepared, boolean loc, IgniteH2Indexing idx,
        @Nullable Connection conn, @Nullable SqlFieldsQuery fieldsQry, @Nullable Integer errKeysPos)
        throws IgniteCheckedException {
        GridSqlStatement stmt = new GridSqlQueryParser(false).parse(prepared);

        if (stmt instanceof GridSqlMerge || stmt instanceof GridSqlInsert)
            return planForInsert(stmt, loc, idx, conn, fieldsQry);
        else if (stmt instanceof GridSqlUpdate || stmt instanceof GridSqlDelete)
            return planForUpdate(stmt, loc, idx, conn, fieldsQry, errKeysPos);
        else
            throw new IgniteSQLException("Unsupported operation: " + prepared.getSQL(),
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Prepare update plan for INSERT or MERGE.
     *
     * @param stmt INSERT or MERGE statement.
     * @param loc Local query flag.
     * @param idx Indexing.
     * @param conn Connection.
     * @param fieldsQuery Original query.
     * @return Update plan.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ConstantConditions")
    private static UpdatePlan planForInsert(GridSqlStatement stmt, boolean loc, IgniteH2Indexing idx,
        @Nullable Connection conn, @Nullable SqlFieldsQuery fieldsQuery) throws IgniteCheckedException {
        GridSqlQuery sel;

        GridSqlElement target;

        GridSqlColumn[] cols;

        boolean isTwoStepSubqry;

        int rowsNum;

        GridSqlTable tbl;

        GridH2RowDescriptor desc;

        List<GridSqlElement[]> elRows = null;

        if (stmt instanceof GridSqlInsert) {
            GridSqlInsert ins = (GridSqlInsert) stmt;
            target = ins.into();

            tbl = DmlAstUtils.gridTableForElement(target);

            GridH2Table h2Tbl = tbl.dataTable();

            if (h2Tbl == null)
                throw new IgniteSQLException("Operation not supported for table '" + tbl.tableName() + "'",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            desc = h2Tbl.rowDescriptor();

            cols = ins.columns();
            sel = DmlAstUtils.selectForInsertOrMerge(cols, ins.rows(), ins.query());

            if (sel == null)
                elRows = ins.rows();

            isTwoStepSubqry = (ins.query() != null);
            rowsNum = isTwoStepSubqry ? 0 : ins.rows().size();
        }
        else if (stmt instanceof GridSqlMerge) {
            GridSqlMerge merge = (GridSqlMerge) stmt;

            target = merge.into();

            tbl = DmlAstUtils.gridTableForElement(target);
            desc = tbl.dataTable().rowDescriptor();

            cols = merge.columns();
            sel = DmlAstUtils.selectForInsertOrMerge(cols, merge.rows(), merge.query());

            if (sel == null)
                elRows = merge.rows();

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

        KeyValueSupplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true, false);
        KeyValueSupplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false, false);

        String selectSql = sel != null ? sel.getSQL() : null;

        DmlDistributedPlanInfo distributed = (rowsNum == 0 && !F.isEmpty(selectSql)) ?
            checkPlanCanBeDistributed(idx, conn, fieldsQuery, loc, selectSql, tbl.dataTable().cacheName()) : null;

        UpdateMode mode = stmt instanceof GridSqlMerge ? UpdateMode.MERGE : UpdateMode.INSERT;

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
     * Prepare update plan for UPDATE or DELETE.
     *
     * @param stmt UPDATE or DELETE statement.
     * @param loc Local query flag.
     * @param idx Indexing.
     * @param conn Connection.
     * @param fieldsQuery Original query.
     * @param errKeysPos index to inject param for re-run keys at. Null if it's not a re-run plan.
     * @return Update plan.
     * @throws IgniteCheckedException if failed.
     */
    private static UpdatePlan planForUpdate(GridSqlStatement stmt, boolean loc, IgniteH2Indexing idx,
        @Nullable Connection conn, @Nullable SqlFieldsQuery fieldsQuery, @Nullable Integer errKeysPos)
        throws IgniteCheckedException {
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

        if (h2Tbl == null)
            throw new IgniteSQLException("Operation not supported for table '" + tbl.tableName() + "'",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

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

                sel = DmlAstUtils.selectForUpdate((GridSqlUpdate) stmt, errKeysPos);

                String selectSql = sel.getSQL();

                DmlDistributedPlanInfo distributed = F.isEmpty(selectSql) ? null :
                    checkPlanCanBeDistributed(idx, conn, fieldsQuery, loc, selectSql, tbl.dataTable().cacheName());

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
                sel = DmlAstUtils.selectForDelete((GridSqlDelete) stmt, errKeysPos);

                String selectSql = sel.getSQL();

                DmlDistributedPlanInfo distributed = F.isEmpty(selectSql) ? null :
                    checkPlanCanBeDistributed(idx, conn, fieldsQuery, loc, selectSql, tbl.dataTable().cacheName());

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
    @SuppressWarnings("ConstantConditions")
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

        int[] colTypes = new int[cols.size()];

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        for (int i = 0; i < cols.size(); i++) {
            String colName = cols.get(i);

            colNames[i] = colName;

            Column h2Col = tbl.getColumn(colName);

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
     * @return Closure returning key or value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
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
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
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
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
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
                            if (S.INCLUDE_SENSITIVE)
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
                            if (S.INCLUDE_SENSITIVE)
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
        if (statement == null || !(statement instanceof GridSqlUpdate))
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

            // column ids 0..2 are _key, _val, _ver
            if (colId >= DEFAULT_COLUMNS_COUNT) {
                if (desc.isColumnKeyProperty(colId - DEFAULT_COLUMNS_COUNT))
                    return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the given update plan can be distributed and returns additional info.
     *
     * @param idx Indexing.
     * @param conn Connection.
     * @param fieldsQry Initial update query.
     * @param loc Local query flag.
     * @param selectQry Derived select query.
     * @param cacheName Cache name.
     * @return distributed update plan info, or {@code null} if cannot be distributed.
     * @throws IgniteCheckedException if failed.
     */
    private static DmlDistributedPlanInfo checkPlanCanBeDistributed(IgniteH2Indexing idx,
        Connection conn, SqlFieldsQuery fieldsQry, boolean loc, String selectQry, String cacheName)
        throws IgniteCheckedException {

        if (loc || !isSkipReducerOnUpdateQuery(fieldsQry) || DmlUtils.isBatched(fieldsQry))
            return null;

        assert conn != null;

        try {
            // Get a new prepared statement for derived select query.
            try (PreparedStatement stmt = conn.prepareStatement(selectQry)) {
                idx.bindParameters(stmt, F.asList(fieldsQry.getArgs()));

                GridCacheTwoStepQuery qry = GridSqlQuerySplitter.split(conn,
                    GridSqlQueryParser.prepared(stmt),
                    fieldsQry.getArgs(),
                    fieldsQry.isCollocated(),
                    fieldsQry.isDistributedJoins(),
                    fieldsQry.isEnforceJoinOrder(), idx);

                boolean distributed = qry.skipMergeTable() &&  qry.mapQueries().size() == 1 &&
                    !qry.mapQueries().get(0).hasSubQueries();

                return distributed ? new DmlDistributedPlanInfo(qry.isReplicatedOnly(),
                    idx.collectCacheIds(CU.cacheId(cacheName), qry)): null;
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Checks whether query flags are compatible with server side update.
     *
     * @param qry Query.
     * @return {@code true} if update can be distributed.
     */
    public static boolean isSkipReducerOnUpdateQuery(SqlFieldsQuery qry) {
        return qry != null && !qry.isLocal() &&
            qry instanceof SqlFieldsQueryEx && ((SqlFieldsQueryEx)qry).isSkipReducerOnUpdate();
    }

    /**
     * Simple supplier that just takes specified element of a given row.
     */
    private final static class PlainValueSupplier implements KeyValueSupplier {
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
        @Override public Object apply(List<?> arg) throws IgniteCheckedException {
            return arg.get(colIdx);
        }
    }
}
