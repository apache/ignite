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

package org.apache.ignite.internal.processors.cache.index;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.value.DataType;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;

/**
 * Common stuff for dynamic columns tests.
 */
public abstract class DynamicColumnsAbstractTest extends AbstractIndexingCommonTest {
    /** SQL to create test table. */
    static final String CREATE_SQL = "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar)";

    /** SQL to create test table with additional columns to drop. */
    static final String CREATE_SQL_4_COLS = "CREATE TABLE IF NOT EXISTS Person (id int primary key, " +
        "name varchar, age int, city varchar)";

    /** SQL to drop test table. */
    static final String DROP_SQL = "DROP TABLE Person";

    /**
     * Check that given columns are seen by client.
     * @param node Node to check.
     * @param schemaName Schema name to look for the table in.
     * @param tblName Table name to check.
     * @param cols Columns whose presence must be checked.
     * @return Number of other columns.
     */
    static int checkTableState(IgniteEx node, String schemaName, String tblName, QueryField... cols)
        throws SQLException {
        List<QueryField> flds = new ArrayList<>();

        try (Connection c = connect(node)) {
            try (ResultSet rs = c.getMetaData().getColumns(null, schemaName, tblName, "%")) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");

                    short type = rs.getShort("DATA_TYPE");

                    String typeClsName = DataType.getTypeClassName(DataType.convertSQLTypeToValueType(type));

                    short nullable = rs.getShort("NULLABLE");

                    flds.add(new QueryField(name, typeClsName, nullable == 1));
                }
            }
        }

        Iterator<QueryField> it = flds.iterator();

        for (int i = flds.size() - cols.length; i > 0 && it.hasNext(); i--)
            it.next();

        for (QueryField exp : cols) {
            assertTrue("New column not found in metadata: " + exp.name(), it.hasNext());

            QueryField act = it.next();

            assertEquals(exp.name(), act.name());

            assertEquals(exp.typeName(), act.typeName());

            assertEquals(exp.isNullable(), act.isNullable());
        }

        return flds.size() - cols.length;
    }

    /**
     * Checks presence of specific table column and returns it.
     *
     * @param node Node to check.
     * @param schemaName Schema name to look for the table in.
     * @param tblName Table name to check.
     * @param colName Column name whose presence must be checked.
     * @return field or {@code null} if not found.
     * @throws SQLException if failed.
     */
    static QueryField getColumnMeta(IgniteEx node, String schemaName, String tblName, String colName)
        throws SQLException {
        try (Connection c = connect(node)) {
            try (ResultSet rs = c.getMetaData().getColumns(null, schemaName, tblName, colName)) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");

                    short type = rs.getShort("DATA_TYPE");

                    String typeClsName = DataType.getTypeClassName(DataType.convertSQLTypeToValueType(type));

                    short nullable = rs.getShort("NULLABLE");

                    return new QueryField(name, typeClsName, nullable == 1);
                }
            }
        }

        return null;
    }

    /**
     * @param name New column name.
     * @param typeName Class name for this new column's data type.
     * @return New column with given name and type.
     */
    protected static QueryField c(String name, String typeName) {
        return new QueryField(name, typeName, true);
    }

    /**
     * @param idx Node index.
     * @return Client configuration.
     * @throws Exception if failed.
     */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        QueryEntity e = new QueryEntity(Integer.class.getName(), "Person");

        LinkedHashMap<String, String> flds = new LinkedHashMap<>();

        flds.put("name", String.class.getName());

        e.setFields(flds);

        return commonConfiguration(idx).setClientMode(true).setCacheConfiguration(
            new CacheConfiguration<>("idx").setQueryEntities(Collections.singletonList(e))
        );
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(128L * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        return optimize(cfg);
    }

    /**
     * Create server node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return commonConfiguration(idx);
    }

    /**
     * Execute SQL command and return resulting dataset.
     * @param node Node to run query from.
     * @param sql Statement.
     * @return result.
     */
    protected List<List<?>> run(Ignite node, String sql) {
        return ((IgniteEx)node).context().query()
            .querySqlFields(new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA), true).getAll();
    }

    /**
     * Execute SQL command and return resulting dataset.
     * @param cache Cache to initiate query from.
     * @param sql Statement.
     * @return result.
     */
    protected List<List<?>> run(IgniteCache<?, ?> cache, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA).setArgs(args)
            .setDistributedJoins(true);

        return cache.query(qry).getAll();
    }

    /**
     * Run specified statement expected to throw {@code IgniteSqlException} with expected specified message.
     * @param sql Statement.
     * @param msg Expected message.
     */
    protected void assertThrows(final Ignite node, final String sql, String msg) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                run(node, sql);

                return null;
            }
        }, IgniteSQLException.class, msg);
    }

    /**
     * Run specified statement expected to throw an exception with specified class and message.
     *
     * @param sql Statement.
     * @param cls Expected exception class.
     * @param msg Expected message.
     */
    protected void assertThrowsAnyCause(final Ignite node, final String sql, Class<? extends Throwable> cls,
        String msg) {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                run(node, sql);

                return null;
            }
        }, cls, msg);
    }
}
