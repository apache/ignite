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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRowAdapter;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * H2 utility methods.
 */
public class H2Utils {
    /** Spatial index class name. */
    private static final String SPATIAL_IDX_CLS =
        "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex";

    /** Quotation character. */
    private static final char ESC_CH = '\"';

    /**
     * @param c1 First column.
     * @param c2 Second column.
     * @return {@code true} If they are the same.
     */
    public static boolean equals(IndexColumn c1, IndexColumn c2) {
        return c1.column.getColumnId() == c2.column.getColumnId();
    }

    /**
     * @param cols Columns list.
     * @param col Column to find.
     * @return {@code true} If found.
     */
    public static boolean containsColumn(List<IndexColumn> cols, IndexColumn col) {
        for (int i = cols.size() - 1; i >= 0; i--) {
            if (equals(cols.get(i), col))
                return true;
        }

        return false;
    }

    /**
     * Check whether columns list contains key or key alias column.
     *
     * @param desc Row descriptor.
     * @param cols Columns list.
     * @return Result.
     */
    public static boolean containsKeyColumn(GridH2RowDescriptor desc, List<IndexColumn> cols) {
        for (int i = cols.size() - 1; i >= 0; i--) {
            if (desc.isKeyColumn(cols.get(i).column.getColumnId()))
                return true;
        }

        return false;
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param fullTblName Fully qualified table name.
     * @param h2Idx H2 index.
     * @param ifNotExists Quietly skip index creation if it exists.
     * @return Statement string.
     */
    public static String indexCreateSql(String fullTblName, GridH2IndexBase h2Idx, boolean ifNotExists) {
        boolean spatial = F.eq(SPATIAL_IDX_CLS, h2Idx.getClass().getName());

        GridStringBuilder sb = new SB("CREATE ")
            .a(spatial ? "SPATIAL " : "")
            .a("INDEX ")
            .a(ifNotExists ? "IF NOT EXISTS " : "")
            .a(withQuotes(h2Idx.getName()))
            .a(" ON ")
            .a(fullTblName)
            .a(" (");

        boolean first = true;

        for (IndexColumn col : h2Idx.getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a(withQuotes(col.columnName)).a(" ").a(col.sortType == SortOrder.ASCENDING ? "ASC" : "DESC");
        }

        sb.a(')');

        return sb.toString();
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param schemaName <b>Quoted</b> schema name.
     * @param idxName Index name.
     * @param ifExists Quietly skip index drop if it exists.
     * @return Statement string.
     */
    public static String indexDropSql(String schemaName, String idxName, boolean ifExists) {
        return "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + withQuotes(schemaName) + '.' + withQuotes(idxName);
    }

    /**
     * @param desc Row descriptor.
     * @param cols Columns list.
     * @param keyCol Primary key column.
     * @param affCol Affinity key column.
     * @return The same list back.
     */
    public static List<IndexColumn> treeIndexColumns(GridH2RowDescriptor desc, List<IndexColumn> cols,
        IndexColumn keyCol, IndexColumn affCol) {
        assert keyCol != null;

        if (!containsKeyColumn(desc, cols))
            cols.add(keyCol);

        if (affCol != null && !containsColumn(cols, affCol))
            cols.add(affCol);

        return cols;
    }

    /**
     * Create spatial index.
     *
     * @param tbl Table.
     * @param idxName Index name.
     * @param cols Columns.
     */
    public static GridH2IndexBase createSpatialIndex(GridH2Table tbl, String idxName, IndexColumn[] cols) {
        try {
            Class<?> cls = Class.forName(SPATIAL_IDX_CLS);

            Constructor<?> ctor = cls.getConstructor(
                GridH2Table.class,
                String.class,
                Integer.TYPE,
                IndexColumn[].class);

            if (!ctor.isAccessible())
                ctor.setAccessible(true);

            final int segments = tbl.rowDescriptor().context().config().getQueryParallelism();

            return (GridH2IndexBase)ctor.newInstance(tbl, idxName, segments, cols);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: " + SPATIAL_IDX_CLS, e);
        }
    }

    /**
     * Add quotes around the name.
     *
     * @param str String.
     * @return String with quotes.
     */
    public static String withQuotes(String str) {
        return ESC_CH + str + ESC_CH;
    }

    /**
     * @param rsMeta Metadata.
     * @return List of fields metadata.
     * @throws SQLException If failed.
     */
    public static List<GridQueryFieldMetadata> meta(ResultSetMetaData rsMeta) throws SQLException {
        List<GridQueryFieldMetadata> meta = new ArrayList<>(rsMeta.getColumnCount());

        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
            String schemaName = rsMeta.getSchemaName(i);
            String typeName = rsMeta.getTableName(i);
            String name = rsMeta.getColumnLabel(i);
            String type = rsMeta.getColumnClassName(i);

            if (type == null) // Expression always returns NULL.
                type = Void.class.getName();

            meta.add(new H2SqlFieldMetadata(schemaName, typeName, name, type));
        }

        return meta;
    }

    /**
     * @param c Connection.
     * @return Session.
     */
    public static Session session(Connection c) {
        return (Session)((JdbcConnection)c).getSession();
    }

    /**
     * @param conn Connection to use.
     * @param distributedJoins If distributed joins are enabled.
     * @param enforceJoinOrder Enforce join order of tables.
     */
    public static void setupConnection(Connection conn, boolean distributedJoins, boolean enforceJoinOrder) {
        Session s = session(conn);

        s.setForceJoinOrder(enforceJoinOrder);
        s.setJoinBatchEnabled(distributedJoins);
    }

    /**
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param desc Row descriptor.
     * @param type Expected column type to convert to.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    public static Object convert(Object val, GridH2RowDescriptor desc, int type) throws IgniteCheckedException {
        if (val == null)
            return null;

        int objType = DataType.getTypeFromClass(val.getClass());

        if (objType == type)
            return val;

        Value h2Val = desc.wrap(val, objType);

        return h2Val.convertTo(type).getObject();
    }

    /**
     * Compares two H2 rows. Since H2 doesn't define equals()/hashcode(), we use external utility method.
     *
     * Basically, this method compares the number of values using {@link SearchRow#getColumnCount()} and
     * the column values using {@link SearchRow#getValue(int)} or {@link Row#getValueList()} -- depending
     * on the actual class.
     *
     * <p>IMPORTANT NOTE: areRowsEqual(null, null) returns false, since it doesn't make a sense to compare null rows.
     *
     * @param left 1st row to compare
     * @param right 2nd row to compare
     * @return true if rows are not null and have equal contents
     */
    public static boolean areRowsEqual(SearchRow left, SearchRow right) {
        if (left == null || right == null)
            return false;

        if (left.getColumnCount() != right.getColumnCount())
            return false;

        if (left instanceof GridH2SearchRowAdapter)
            return left.equals(right);

        if (right instanceof GridH2SearchRowAdapter)
            return right.equals(left);

        return areRowsEqualInternal(left, right);
    }

    /**
     * Compares two H2 rows. Intended for use from {@link #areRowsEqual(SearchRow, SearchRow)} method and
     * equals()/hashcode() methods of {@link Row} subclasses.
     *
     * @param left 1st row to compare
     * @param right 2nd row to compare
     * @return true if rows are not null and have equal contents
     */
    public static boolean areRowsEqualInternal(SearchRow left, SearchRow right) {
        if (right == null)
            return false;

        if (left.getColumnCount() != right.getColumnCount())
            return false;

        Value[] leftValues = getRowValues(left);
        Value[] rightValues = getRowValues(right);

        return Arrays.equals(leftValues, rightValues);
    }

    /**
     * Returns an array of row column values.
     *
     * <p>Depending on actual class it calls either {@link SearchRow#getValue(int)} for every column from 0
     * to {@link SearchRow#getColumnCount()} or {@link Row#getValueList()}. Returns null if row == null.
     *
     * @param row to extract values from.
     * @return Array of references to row values or null if row == null.
     */
    public static Value[] getRowValues(SearchRow row) {
        if (row == null)
            return null;

        if (row instanceof Row)
            return ((Row)row).getValueList();

        Value[] values = new Value[row.getColumnCount()];
        for (int i = 0; i < row.getColumnCount(); i++)
            values[i] = row.getValue(i);

        return values;
    }

    /**
     * Returns hash code of the row values.
     *
     * @param row Row to calculate hash code.
     * @return Hash code or 0 if row == null.
     */
    public static int rowHashCode(SearchRow row) {
        return (row == null) ? 0 : Objects.hash(getRowValues(row));
    }

    /**
     * Private constructor.
     */
    private H2Utils() {
        // No-op.
    }
}
