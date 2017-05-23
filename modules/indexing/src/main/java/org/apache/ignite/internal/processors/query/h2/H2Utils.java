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

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * H2 utility methods.
 */
public class H2Utils {
    /** Spatial index class name. */
    private static final String SPATIAL_IDX_CLS =
        "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex";

    /** */
    public static final char ESC_CH = '\"';

    /** */
    private static final String ESC_STR = ESC_CH + "" + ESC_CH;

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
    public static String indexCreateSql(String fullTblName, GridH2IndexBase h2Idx, boolean ifNotExists,
        boolean escapeAll) {
        boolean spatial = F.eq(SPATIAL_IDX_CLS, h2Idx.getClass().getName());

        GridStringBuilder sb = new SB("CREATE ")
            .a(spatial ? "SPATIAL " : "")
            .a("INDEX ")
            .a(ifNotExists ? "IF NOT EXISTS " : "")
            .a(escapeName(h2Idx.getName(), escapeAll))
            .a(" ON ")
            .a(fullTblName)
            .a(" (");

        boolean first = true;

        for (IndexColumn col : h2Idx.getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a("\"" + col.columnName + "\"").a(" ").a(col.sortType == SortOrder.ASCENDING ? "ASC" : "DESC");
        }

        sb.a(')');

        return sb.toString();
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param schemaName <b>Quoted</b> schema name.
     * @param idxName Index name.
     * @param ifExists Quietly skip index drop if it exists.
     * @param escapeAll Escape flag.
     * @return Statement string.
     */
    public static String indexDropSql(String schemaName, String idxName, boolean ifExists, boolean escapeAll) {
        return "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + schemaName + '.' + escapeName(idxName, escapeAll);
    }

    /**
     * Escapes name to be valid SQL identifier. Currently just replaces '.' and '$' sign with '_'.
     *
     * @param name Name.
     * @param escapeAll Escape flag.
     * @return Escaped name.
     */
    public static String escapeName(String name, boolean escapeAll) {
        if (name == null) // It is possible only for a cache name.
            return ESC_STR;

        if (escapeAll)
            return ESC_CH + name + ESC_CH;

        SB sb = null;

        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);

            if (!Character.isLetter(ch) && !Character.isDigit(ch) && ch != '_' &&
                !(ch == '"' && (i == 0 || i == name.length() - 1)) && ch != '-') {
                // Class name can also contain '$' or '.' - these should be escaped.
                assert ch == '$' || ch == '.';

                if (sb == null)
                    sb = new SB();

                sb.a(name.substring(sb.length(), i));

                // Replace illegal chars with '_'.
                sb.a('_');
            }
        }

        if (sb == null)
            return name;

        sb.a(name.substring(sb.length(), name.length()));

        return sb.toString();
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

            final int segments = tbl.rowDescriptor().configuration().getQueryParallelism();

            return (GridH2IndexBase)ctor.newInstance(tbl, idxName, segments, cols);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: " + SPATIAL_IDX_CLS, e);
        }
    }

    /**
     * Stores rule for constructing schemaName according to cache configuration.
     *
     * @param ccfg Cache configuration.
     * @return Proper schema name according to ANSI-99 standard.
     */
    public static String schemaNameFromCacheConfiguration(CacheConfiguration<?, ?> ccfg) {
        if (ccfg.getSqlSchema() == null)
            return escapeName(ccfg.getName(), true);

        if (ccfg.getSqlSchema().charAt(0) == ESC_CH)
            return ccfg.getSqlSchema();

        return ccfg.isSqlEscapeAll() ?
            escapeName(ccfg.getSqlSchema(), true) : ccfg.getSqlSchema().toUpperCase();
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
     * Private constructor.
     */
    private H2Utils() {
        // No-op.
    }
}
