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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for dynamic schema changes.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractSchemaSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache. */
    protected static final String CACHE_NAME = "cache";

    /** Table name. */
    protected static final String TBL_NAME = tableName(ValueClass.class);

    /** Escaped table name. */
    protected static final String TBL_NAME_ESCAPED = typeName(ValueClass.class);

    /** Index name 1. */
    protected static final String IDX_NAME_1 = "IDX_1";

    /** Index name 1 escaped. */
    protected static final String IDX_NAME_1_ESCAPED = "idx_1";

    /** Index name 2. */
    protected static final String IDX_NAME_2 = "IDX_2";

    /** Index name 2 escaped. */
    protected static final String IDX_NAME_2_ESCAPED = "idx_2";

    /** Index name 2. */
    protected static final String IDX_NAME_LOCAL = "IDX_LOC";

    /** Key ID field. */
    protected static final String FIELD_KEY = "id";

    /** Key alias */
    protected static final String FIELD_KEY_ALIAS = "key";

    /** Field 1. */
    protected static final String FIELD_NAME_1 = "FIELD1";

    /** Field 1 escaped. */
    protected static final String FIELD_NAME_1_ESCAPED = "field1";

    /** Field 2. */
    protected static final String FIELD_NAME_2 = "FIELD2";

    /** Field 2 escaped. */
    protected static final String FIELD_NAME_2_ESCAPED = "field2";

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setMarshaller(new BinaryMarshaller());

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(128 * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        return optimize(cfg);
    }

    /**
     * Ensure that SQL exception is thrown.
     *
     * @param r Runnable.
     * @param expCode Error code.
     */
    static void assertSqlException(DynamicIndexAbstractBasicSelfTest.RunnableX r, int expCode) {
        try {
            try {
                r.run();
            }
            catch (CacheException e) {
                if (e.getCause() != null)
                    throw (Exception)e.getCause();
                else
                    throw e;
            }
        }
        catch (IgniteSQLException e) {
            assertEquals("Unexpected error code [expected=" + expCode + ", actual=" + e.statusCode() + ']',
                expCode, e.statusCode());

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(IgniteSQLException.class.getSimpleName() +  " is not thrown.");
    }

    /**
     * Get type on the given node for the given cache and table name. Type must exist.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @return Type.
     */
    static QueryTypeDescriptorImpl typeExisting(IgniteEx node, String cacheName, String tblName) {
        QueryTypeDescriptorImpl res = type(node, cacheName, tblName);

        assertNotNull(res);

        return res;
    }

    /**
     * Get type on the given node for the given cache and table name.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @return Type.
     */
    @Nullable protected static QueryTypeDescriptorImpl type(IgniteEx node, String cacheName, String tblName) {
        return types(node, cacheName).get(tblName);
    }

    /**
     * Get available types on the given node for the given cache.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Map from table name to type.
     */
    protected static Map<String, QueryTypeDescriptorImpl> types(IgniteEx node, String cacheName) {
        Map<String, QueryTypeDescriptorImpl> res = new HashMap<>();

        Collection<GridQueryTypeDescriptor> descs = node.context().query().types(cacheName);

        for (GridQueryTypeDescriptor desc : descs) {
            QueryTypeDescriptorImpl desc0 = (QueryTypeDescriptorImpl)desc;

            res.put(desc0.tableName(), desc0);
        }

        return res;
    }

    /**
     * Assert index state on all <b>affinity</b> nodes.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param inlineSize Inline size.
     * @param fields Fields.
     */
    static void assertIndex(String cacheName, String tblName, String idxName,
        int inlineSize, IgniteBiTuple<String, Boolean>... fields) {
        for (Ignite node : Ignition.allGrids())
            assertIndex(node, cacheName, tblName, idxName, inlineSize, fields);
    }

    /**
     * Assert index state on particular node.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param inlineSize Inline size.
     * @param fields Fields.
     */
    static void assertIndex(Ignite node, String cacheName, String tblName,
        String idxName, int inlineSize, IgniteBiTuple<String, Boolean>... fields) {
        awaitCompletion();

        node.cache(cacheName);

        IgniteEx node0 = (IgniteEx)node;

        ArrayList<IgniteBiTuple<String, Boolean>> res = new ArrayList<>();

        try {
            try (Connection c = connect(node0)) {
                try (ResultSet rs = c.getMetaData().getIndexInfo(null, cacheName, tblName, false, false)) {
                    while (rs.next()) {
                        if (F.eq(idxName, rs.getString("INDEX_NAME")))
                            res.add(new T2<>(rs.getString("COLUMN_NAME"), F.eq("A", rs.getString("ASC_OR_DESC"))));
                    }
                }
            }

            assertTrue("Index not found: " + idxName, res.size() > 0);

            assertEquals(Arrays.asList(fields), res);
        }
        catch (SQLException e) {
            throw new AssertionError(e);
        }

        // Also, let's check internal stuff not visible via JDBC - like inline size.
        QueryTypeDescriptorImpl typeDesc = typeExisting(node0, cacheName, tblName);

        assertInternalIndexParams(typeDesc, idxName, inlineSize);
    }

    /**
     * Assert index details not available via JDBC.
     * @param typeDesc Type descriptor.
     * @param idxName Index name.
     * @param inlineSize Inline size.
     */
    private static void assertInternalIndexParams(QueryTypeDescriptorImpl typeDesc, String idxName, int inlineSize) {
        QueryIndexDescriptorImpl idxDesc = typeDesc.index(idxName);

        assertNotNull(idxDesc);

        assertEquals(idxName, idxDesc.name());
        assertEquals(typeDesc, idxDesc.typeDescriptor());
        assertEquals(QueryIndexType.SORTED, idxDesc.type());
        assertEquals(inlineSize, idxDesc.inlineSize());
    }

    /**
     * @param node Node to connect to.
     * @return Thin JDBC connection to specified node.
     */
    public static Connection connect(IgniteEx node) {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class) {
                cliLsnrRec = rec;

                break;
            }
        }

        assertNotNull(cliLsnrRec);

        try {
            return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + cliLsnrRec.port());
        }
        catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * @param conn Connection.
     * @param sql Statement.
     * @throws SQLException if failed.
     */
    public static void execute(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /**
     * Assert index doesn't exist on all nodes.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     */
    static void assertNoIndex(String cacheName, String tblName, String idxName) {
        for (Ignite node : Ignition.allGrids())
            assertNoIndex(node, cacheName, tblName, idxName);
    }

    /**
     * Assert index doesn't exist on particular node.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     */
    static void assertNoIndex(Ignite node, String cacheName, String tblName, String idxName) {
        awaitCompletion();

        node.cache(cacheName);

        try {
            try (Connection c = connect((IgniteEx)node)) {
                try (ResultSet rs = c.getMetaData().getIndexInfo(null, cacheName, tblName, false, false)) {
                    while (rs.next()) {
                        assertFalse("Index exists, although shouldn't: " + tblName + '.' + idxName,
                            F.eq(idxName, rs.getString("INDEX_NAME")));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Await completion (hopefully) of pending operations.
     */
    private static void awaitCompletion() {
        try {
            U.sleep(100);
        }
        catch (IgniteInterruptedCheckedException e) {
            fail();
        }
    }

    /**
     * Get table name for class.
     *
     * @param cls Class.
     * @return Table name.
     */
    protected static String typeName(Class cls) {
        return cls.getSimpleName();
    }

    /**
     * Get table name for class.
     *
     * @param cls Class.
     * @return Table name.
     */
    static String tableName(Class cls) {
        return QueryUtils.normalizeObjectName(typeName(cls), true);
    }

    /**
     * Convenient method for index creation.
     *
     * @param name Name.
     * @param fields Fields.
     * @return Index.
     */
    protected static QueryIndex index(String name, IgniteBiTuple<String, Boolean>... fields) {
        QueryIndex idx = new QueryIndex();

        idx.setName(name);

        LinkedHashMap<String, Boolean> fields0 = new LinkedHashMap<>();

        for (IgniteBiTuple<String, Boolean> field : fields)
            fields0.put(field.getKey(), field.getValue());

        idx.setFields(fields0);

        return idx;
    }

    /**
     * Execute SQL statement on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    protected List<List<?>> execute(Ignite node, String sql) {
        return queryProcessor(node).querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC"), true).getAll();
    }

    /**
     * Get query processor.
     *
     * @param node Node.
     * @return Query processor.
     */
    static GridQueryProcessor queryProcessor(Ignite node) {
        return queryProcessor((IgniteEx)node);
    }

    /**
     * Get query processor.
     *
     * @param node Node.
     * @return Query processor.
     */
    protected static GridQueryProcessor queryProcessor(IgniteEx node) {
        return node.context().query();
    }

    /**
     * Field for index state check (ascending).
     *
     * @param name Name.
     * @return Field.
     */
    protected static IgniteBiTuple<String, Boolean> field(String name) {
        return field(name, true);
    }

    /**
     * Field for index state check.
     *
     * @param name Name.
     * @param asc Ascending flag.
     * @return Field.
     */
    protected static IgniteBiTuple<String, Boolean> field(String name, boolean asc) {
        return F.t(name.toUpperCase(), asc);
    }

    /**
     * @param fieldName Field name.
     * @return Alias.
     */
    protected static String alias(String fieldName) {
        return fieldName + "_alias";
    }

    /**
     * Synchronously create index.
     *
     * @param node Ignite node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idx Index.
     * @param ifNotExists When set to true operation will fail if index already exists.
     * @param parallel Parallelism level.
     * @throws Exception If failed.
     */
    void dynamicIndexCreate(Ignite node, String cacheName, String tblName, QueryIndex idx,
        boolean ifNotExists, int parallel)
        throws Exception {
        GridStringBuilder sql = new SB("CREATE INDEX ")
            .a(ifNotExists ? "IF NOT EXISTS " : "")
            .a(idx.getName())
            .a(" ON ")
            .a(tblName)
            .a(" (");

        boolean first = true;

        for (Map.Entry<String, Boolean> fieldEntry : idx.getFields().entrySet()) {
            if (first)
                first = false;
            else
                sql.a(", ");

            String name = fieldEntry.getKey();
            boolean asc = fieldEntry.getValue();

            sql.a(name).a(" ").a(asc ? "ASC" : "DESC");
        }

        sql.a(')');

        if (idx.getInlineSize() != QueryIndex.DFLT_INLINE_SIZE)
            sql.a(" INLINE_SIZE ").a(idx.getInlineSize());

        if (parallel != 0)
            sql.a(" PARALLEL ").a(parallel);

        executeSql(node, cacheName, sql.toString());
    }

    /**
     * Synchronously drop index.
     *
     * @param node Ignite node.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param ifExists When set to true operation fill fail if index doesn't exists.
     * @throws Exception if failed.
     */
    protected void dynamicIndexDrop(Ignite node, String cacheName, String idxName, boolean ifExists) throws Exception {
        String sql = "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + idxName;

        executeSql(node, cacheName, sql);
    }

    /**
     * Start SQL cache on given node.
     * @param node Node to create cache on.
     * @param ccfg Cache configuration.
     * @return Created cache.
     */
    protected IgniteCache<?, ?> createSqlCache(Ignite node, CacheConfiguration ccfg) throws IgniteCheckedException {
        ((IgniteEx)node).context().cache().dynamicStartSqlCache(ccfg).get();

        IgniteCache<?, ?> res = node.cache(CACHE_NAME);

        assertNotNull(res);

        return res;
    }

    /**
     * Destroy SQL cache on given node.
     * @param node Node to create cache on.
     */
    protected void destroySqlCache(Ignite node) throws IgniteCheckedException {
        ((IgniteEx)node).context().cache().dynamicDestroyCache(CACHE_NAME, true, true, false).get();
    }

    /**
     * Execute SQL.
     *
     * @param node Ignite node.
     * @param cacheName Cache name.
     * @param sql SQL.
     */
    private void executeSql(Ignite node, String cacheName, String sql) {
        log.info("Executing DDL: " + sql);

        node.cache(cacheName).query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * Key class.
     */
    public static class KeyClass {
        /** ID. */
        @QuerySqlField
        private long id;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        public KeyClass(long id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        public long id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyClass keyClass = (KeyClass) o;

            return id == keyClass.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    /**
     * Key class.
     */
    public static class ValueClass {
        /** Field 1. */
        @QuerySqlField
        private String field1;

        /**
         * Constructor.
         *
         * @param field1 Field 1.
         */
        public ValueClass(String field1) {
            this.field1 = field1;
        }

        /**
         * @return Field 1
         */
        public String field1() {
            return field1;
        }
    }

    /**
     * Key class.
     */
    public static class ValueClass2 {
        /** Field 1. */
        @QuerySqlField(name = "field1")
        private String field;

        /**
         * Constructor.
         *
         * @param field Field 1.
         */
        public ValueClass2(String field) {
            this.field = field;
        }

        /**
         * @return Field 1
         */
        public String field() {
            return field;
        }
    }

    /**
     * Runnable which can throw checked exceptions.
     */
    protected interface RunnableX {
        /**
         * Do run.
         *
         * @throws Exception If failed.
         */
        public void run() throws Exception;
    }
}
