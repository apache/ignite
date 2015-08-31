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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * Thread local table wrapper for another table instance.
 */
public class GridThreadLocalTable extends Table {
    /** Delegate table */
    private final ThreadLocal<Table> tbl = new ThreadLocal<>();

    /**
     * @param schema Schema.
     * @param id ID.
     * @param name Table name.
     * @param persistIndexes Persist indexes.
     * @param persistData Persist data.
     */
    public GridThreadLocalTable(Schema schema, int id, String name, boolean persistIndexes, boolean persistData) {
        super(schema, id, name, persistIndexes, persistData);
    }

    /**
     * @param t Table or {@code null} to reset existing.
     */
    public void setInnerTable(Table t) {
        if (t == null)
            tbl.remove();
        else
            tbl.set(t);
    }

    /** {@inheritDoc} */
    @Override public Index getPrimaryKey() {
        return tbl.get().getPrimaryKey();
    }

    /** {@inheritDoc} */
    @Override public Column getRowIdColumn() {
        return tbl.get().getRowIdColumn();
    }

    /** {@inheritDoc} */
    @Override public PlanItem getBestPlanItem(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return tbl.get().getBestPlanItem(session, masks, filter, sortOrder);
    }

    /** {@inheritDoc} */
    @Override public Value getDefaultValue(Session session, Column column) {
        return tbl.get().getDefaultValue(session, column);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        return tbl.get().getTemplateSimpleRow(singleColumn);
    }

    /** {@inheritDoc} */
    @Override public Row getTemplateRow() {
        return tbl.get().getTemplateRow();
    }

    /** {@inheritDoc} */
    @Override public Column getColumn(String columnName) {
        return tbl.get().getColumn(columnName);
    }

    /** {@inheritDoc} */
    @Override public Column getColumn(int index) {
        return tbl.get().getColumn(index);
    }

    /** {@inheritDoc} */
    @Override public Index getIndexForColumn(Column column) {
        return tbl.get().getIndexForColumn(column);
    }

    /** {@inheritDoc} */
    @Override public Column[] getColumns() {
        return tbl.get().getColumns();
    }

    /** {@inheritDoc} */
    @Override protected void setColumns(Column[] columns) {
        throw new IllegalStateException("Cols: " + Arrays.asList(columns));
    }

    /** {@inheritDoc} */
    @Override public void lock(Session session, boolean exclusive, boolean force) {
        tbl.get().lock(session, exclusive, force);
    }

    /** {@inheritDoc} */
    @Override public void close(Session session) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session s) {
        tbl.get().unlock(s);
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols,
        IndexType indexType, boolean create, String indexComment) {
        return tbl.get().addIndex(session, indexName, indexId, cols, indexType, create, indexComment);
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session session, Row row) {
        tbl.get().removeRow(session, row);
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session session) {
        tbl.get().truncate(session);
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session session, Row row) {
        tbl.get().addRow(session, row);
    }

    /** {@inheritDoc} */
    @Override public void checkSupportAlter() {
        tbl.get().checkSupportAlter();
    }

    /** {@inheritDoc} */
    @Override public String getTableType() {
        return EXTERNAL_TABLE_ENGINE;
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        return tbl.get().getUniqueIndex();
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session session) {
        return tbl.get().getScanIndex(session);
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        return tbl.get().getIndexes();
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusively() {
        return tbl.get().isLockedExclusively();
    }

    /** {@inheritDoc} */
    @Override public long getMaxDataModificationId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDeterministic() {
        return tbl.get().isDeterministic();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return tbl.get().canGetRowCount();
    }

    /** {@inheritDoc} */
    @Override public boolean canDrop() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session session) {
        return tbl.get().getRowCount(session);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        Table t = tbl.get();

        return t == null ? 0 : t.getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getCreateSQL() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getDropSQL() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public void addDependencies(HashSet<DbObject> dependencies) {
        // No-op. We should not have any dependencies to add.
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /**
     * Engine.
     */
    public static class Engine implements TableEngine {
        /** */
        private static ThreadLocal<GridThreadLocalTable> createdTbl = new ThreadLocal<>();

        /**
         * @return Created table.
         */
        public static GridThreadLocalTable getCreated() {
            GridThreadLocalTable tbl = createdTbl.get();

            assert tbl != null;

            createdTbl.remove();

            return tbl;
        }

        /** {@inheritDoc} */
        @Override public Table createTable(CreateTableData d) {
            assert createdTbl.get() == null;

            GridThreadLocalTable tbl = new GridThreadLocalTable(d.schema, d.id, d.tableName, d.persistIndexes,
                d.persistData);

            createdTbl.set(tbl);

            return tbl;
        }
    }
}