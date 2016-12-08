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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.DmlAstUtils;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlMerge;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUpdate;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.VAL_FIELD_NAME;

/**
 * Logic for building update plans performed by {@link DmlStatementsProcessor}.
 */
public final class UpdatePlanBuilder {
    /** */
    private UpdatePlanBuilder() {
        // No-op.
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     *
     * @param prepared H2's {@link Prepared}.
     * @return Update plan.
     */
    public static UpdatePlan planForStatement(Prepared prepared,
        @Nullable Integer errKeysPos) throws IgniteCheckedException {
        assert !prepared.isQuery();

        GridSqlStatement stmt = new GridSqlQueryParser().parse(prepared);

        if (stmt instanceof GridSqlMerge || stmt instanceof GridSqlInsert)
            return planForInsert(stmt);
        else
            return planForUpdate(stmt, errKeysPos);
    }

    /**
     * Prepare update plan for INSERT or MERGE.
     *
     * @param stmt INSERT or MERGE statement.
     * @return Update plan.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ConstantConditions")
    private static UpdatePlan planForInsert(GridSqlStatement stmt) throws IgniteCheckedException {
        GridSqlQuery sel;

        GridSqlElement target;

        GridSqlColumn[] cols;

        boolean isTwoStepSubqry;

        int rowsNum;

        GridSqlTable tbl;

        GridH2RowDescriptor desc;

        if (stmt instanceof GridSqlInsert) {
            GridSqlInsert ins = (GridSqlInsert) stmt;
            target = ins.into();

            tbl = gridTableForElement(target);
            desc = tbl.dataTable().rowDescriptor();

            cols = ins.columns();
            sel = DmlAstUtils.selectForInsertOrMerge(cols, ins.rows(), ins.query(), desc);
            isTwoStepSubqry = (ins.query() != null);
            rowsNum = isTwoStepSubqry ? 0 : ins.rows().size();
        }
        else if (stmt instanceof GridSqlMerge) {
            GridSqlMerge merge = (GridSqlMerge) stmt;

            target = merge.into();

            tbl = gridTableForElement(target);
            desc = tbl.dataTable().rowDescriptor();

            // This check also protects us from attempts to update key or its fields directly -
            // when no key except cache key can be used, it will serve only for uniqueness checks,
            // not for updates, and hence will allow putting new pairs only.
            // We don't quote _key and _val column names on CREATE TABLE, so they are always uppercase here.
            GridSqlColumn[] keys = merge.keys();
            if (keys.length != 1 || !IgniteH2Indexing.KEY_FIELD_NAME.equals(keys[0].columnName()))
                throw new CacheException("SQL MERGE does not support arbitrary keys");

            cols = merge.columns();
            sel = DmlAstUtils.selectForInsertOrMerge(cols, merge.rows(), merge.query(), desc);
            isTwoStepSubqry = (merge.query() != null);
            rowsNum = isTwoStepSubqry ? 0 : merge.rows().size();
        }
        else throw new IgniteSQLException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);

        // Let's set the flag only for subqueries that have their FROM specified.
        isTwoStepSubqry = (isTwoStepSubqry && (sel instanceof GridSqlUnion ||
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

            if (KEY_FIELD_NAME.equals(colName)) {
                keyColIdx = i;
                continue;
            }

            if (VAL_FIELD_NAME.equals(colName)) {
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

        KeyValueSupplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true);
        KeyValueSupplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false);

        if (stmt instanceof GridSqlMerge)
            return UpdatePlan.forMerge(tbl.dataTable(), colNames, colTypes, keySupplier, valSupplier, keyColIdx,
                valColIdx, sel.getSQL(), !isTwoStepSubqry, rowsNum);
        else
            return UpdatePlan.forInsert(tbl.dataTable(), colNames, colTypes, keySupplier, valSupplier, keyColIdx,
                valColIdx, sel.getSQL(), !isTwoStepSubqry, rowsNum);
    }

    /**
     * Prepare update plan for UPDATE or DELETE.
     *
     * @param stmt UPDATE or DELETE statement.
     * @param errKeysPos index to inject param for re-run keys at. Null if it's not a re-run plan.
     * @return Update plan.
     * @throws IgniteCheckedException if failed.
     */
    private static UpdatePlan planForUpdate(GridSqlStatement stmt, @Nullable Integer errKeysPos) throws IgniteCheckedException {
        GridSqlElement target;

        FastUpdateArguments fastUpdate;

        UpdateMode mode;

        if (stmt instanceof GridSqlUpdate) {
            // Let's verify that user is not trying to mess with key's columns directly
            verifyUpdateColumns(stmt);

            GridSqlUpdate update = (GridSqlUpdate) stmt;
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

        GridSqlTable tbl = gridTableForElement(target);

        GridH2Table gridTbl = tbl.dataTable();

        GridH2RowDescriptor desc = gridTbl.rowDescriptor();

        if (desc == null)
            throw new IgniteSQLException("Row descriptor undefined for table '" + gridTbl.getName() + "'",
                IgniteQueryErrorCode.NULL_TABLE_DESCRIPTOR);

        if (fastUpdate != null)
            return UpdatePlan.forFastUpdate(mode, gridTbl, fastUpdate);
        else {
            GridSqlSelect sel;

            if (stmt instanceof GridSqlUpdate) {
                boolean bin = desc.context().binaryMarshaller();

                List<GridSqlColumn> updatedCols = ((GridSqlUpdate) stmt).cols();

                int valColIdx = -1;

                String[] colNames = new String[updatedCols.size()];

                int[] colTypes = new int[updatedCols.size()];

                for (int i = 0; i < updatedCols.size(); i++) {
                    colNames[i] = updatedCols.get(i).columnName();

                    colTypes[i] = updatedCols.get(i).resultType().type();

                    if (VAL_FIELD_NAME.equals(colNames[i]))
                        valColIdx = i;
                }

                boolean hasNewVal = (valColIdx != -1);

                // Statement updates distinct properties if it does not have _val in updated columns list
                // or if its list of updated columns includes only _val, i.e. is single element.
                boolean hasProps = !hasNewVal || updatedCols.size() > 1;

                // Index of new _val in results of SELECT
                if (hasNewVal)
                    valColIdx += 2;

                int newValColIdx;

                if (!hasProps) // No distinct properties, only whole new value - let's take it
                    newValColIdx = valColIdx;
                else if (bin) // We update distinct columns in binary mode - let's choose correct index for the builder
                    newValColIdx = (hasNewVal ? valColIdx : 1);
                else // Distinct properties, non binary mode - let's instantiate.
                    newValColIdx = -1;

                // We want supplier to take present value only in case of binary mode as it will create
                // whole new object as a result anyway, so we don't need to copy previous property values explicitly.
                // Otherwise we always want it to instantiate new object whose properties we will later
                // set to current values.
                KeyValueSupplier newValSupplier = createSupplier(desc.context(), desc.type(), newValColIdx, hasProps, false);

                sel = DmlAstUtils.selectForUpdate((GridSqlUpdate) stmt, errKeysPos);

                return UpdatePlan.forUpdate(gridTbl, colNames, colTypes, newValSupplier, valColIdx, sel.getSQL());
            }
            else {
                sel = DmlAstUtils.selectForDelete((GridSqlDelete) stmt, errKeysPos);

                return UpdatePlan.forDelete(gridTbl, sel.getSQL());
            }
        }
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
     * @throws IgniteCheckedException
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private static KeyValueSupplier createSupplier(final GridCacheContext<?, ?> cctx, GridQueryTypeDescriptor desc,
                                                   final int colIdx, boolean hasProps, final boolean key) throws IgniteCheckedException {
        final String typeName = key ? desc.keyTypeName() : desc.valueTypeName();

        //Try to find class for the key locally.
        final Class<?> cls = key ? U.firstNotNull(U.classForName(desc.keyTypeName(), null), desc.keyClass())
            : desc.valueClass();

        boolean isSqlType = GridQueryProcessor.isSqlType(cls);

        // If we don't need to construct anything from scratch, just return value from array.
        if (isSqlType || !hasProps || !cctx.binaryMarshaller()) {
            if (colIdx != -1)
                return new KeyValueSupplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        return arg.get(colIdx);
                    }
                };
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
                        BinaryObject bin = cctx.grid().binary().toBinary(arg.get(colIdx));

                        return cctx.grid().binary().builder(bin);
                    }
                };
            }
            else {
                // ...and if we don't, just create a new builder.
                return new KeyValueSupplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        return cctx.grid().binary().builder(typeName);
                    }
                };
            }
        }
        else {
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
                            throw new IgniteCheckedException("Failed to invoke default ctor for " +
                                (key ? "key" : "value"), e);
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
                            throw new IgniteCheckedException("Failed to invoke default ctor for " +
                                (key ? "key" : "value"), e);
                        }
                    }
                };
            }
        }
    }



    /**
     * @param target Expression to extract the table from.
     * @return Back end table for this element.
     */
    private static GridSqlTable gridTableForElement(GridSqlElement target) {
        Set<GridSqlTable> tbls = new HashSet<>();

        DmlAstUtils.collectAllGridTablesInTarget(target, tbls);

        if (tbls.size() != 1)
            throw new IgniteSQLException("Failed to determine target table", IgniteQueryErrorCode.TABLE_NOT_FOUND);

        return tbls.iterator().next();
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
            throw new IgniteSQLException("Failed to determine target table for UPDATE", IgniteQueryErrorCode.TABLE_NOT_FOUND);

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        if (updateAffectsKeyColumns(gridTbl, update.set().keySet()))
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

        Column[] cols = gridTbl.getColumns();

        // Check "_key" column itself - always has index of 0.
        if (affectedColNames.contains(cols[0].getName()))
            return true;

        // Start off from i = 2 to skip indices of 0 an 1 corresponding to key and value respectively.
        for (int i = 2; i < cols.length; i++)
            if (affectedColNames.contains(cols[i].getName()) && desc.isColumnKeyProperty(i - 2))
                return true;

        return false;
    }
}
