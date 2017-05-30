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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for dynamic schema changes.
 */
@SuppressWarnings("unchecked")
public class AbstractSchemaSelfTest extends GridCommonAbstractTest {
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
     * Get type on the given node for the given cache and table name. Type must exist.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @return Type.
     */
    protected static QueryTypeDescriptorImpl typeExisting(IgniteEx node, String cacheName, String tblName) {
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
     * @param fields Fields.
     */
    protected static void assertIndex(String cacheName, String tblName, String idxName,
        IgniteBiTuple<String, Boolean>... fields) {
        assertIndex(cacheName, false, tblName, idxName, fields);
    }

    /**
     * Assert index state on all nodes.
     *
     * @param cacheName Cache name.
     * @param checkNonAffinityNodes Whether existence of {@link GridQueryIndexDescriptor} must be checked on non
     *     affinity nodes as well.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param fields Fields.
     */
    protected static void assertIndex(String cacheName, boolean checkNonAffinityNodes, String tblName, String idxName,
        IgniteBiTuple<String, Boolean>... fields) {
        for (Ignite node : Ignition.allGrids())
            assertIndex(node, checkNonAffinityNodes, cacheName, tblName, idxName, fields);
    }

    /**
     * Assert index state on particular node.
     *
     * @param node Node.
     * @param checkNonAffinityNode Whether existence of {@link GridQueryIndexDescriptor} must be checked regardless of
     * whether this node is affinity node or not.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param fields Fields.
     */
    protected static void assertIndex(Ignite node, boolean checkNonAffinityNode, String cacheName, String tblName,
        String idxName, IgniteBiTuple<String, Boolean>... fields) {
        IgniteEx node0 = (IgniteEx)node;

        assertIndexDescriptor(node0, cacheName, tblName, idxName, fields);

        if (checkNonAffinityNode || affinityNode(node0, cacheName)) {
            QueryTypeDescriptorImpl typeDesc = typeExisting(node0, cacheName, tblName);

            assertIndex(typeDesc, idxName, fields);
        }
    }

    /**
     * Make sure index exists in cache descriptor.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param fields Fields.
     */
    protected static void assertIndexDescriptor(IgniteEx node, String cacheName, String tblName, String idxName,
        IgniteBiTuple<String, Boolean>... fields) {
        awaitCompletion();

        DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

        assert desc != null;

        for (QueryEntity entity : desc.schema().entities()) {
            if (F.eq(tblName, entity.getTableName())) {
                for (QueryIndex idx : entity.getIndexes()) {
                    if (F.eq(QueryUtils.indexName(entity, idx), idxName)) {
                        LinkedHashMap<String, Boolean> idxFields = idx.getFields();

                        assertEquals(idxFields.size(), fields.length);

                        int i = 0;

                        for (String idxField : idxFields.keySet()) {
                            assertEquals(idxField.toLowerCase(), fields[i].get1().toLowerCase());
                            assertEquals(idxFields.get(idxField), fields[i].get2());

                            i++;
                        }

                        return;
                    }
                }
            }
        }

        fail("Index not found [node=" + node.name() + ", cacheName=" + cacheName + ", tlbName=" + tblName +
            ", idxName=" + idxName + ']');
    }

    /**
     * Assert index state.
     *
     * @param typeDesc Type descriptor.
     * @param idxName Index name.
     * @param fields Fields (order is important).
     */
    protected static void assertIndex(QueryTypeDescriptorImpl typeDesc, String idxName,
        IgniteBiTuple<String, Boolean>... fields) {
        QueryIndexDescriptorImpl idxDesc = typeDesc.index(idxName);

        assertNotNull(idxDesc);

        assertEquals(idxName, idxDesc.name());
        assertEquals(typeDesc, idxDesc.typeDescriptor());
        assertEquals(QueryIndexType.SORTED, idxDesc.type());

        List<String> fieldNames = new ArrayList<>(idxDesc.fields());

        assertEquals(fields.length, fieldNames.size());

        for (int i = 0; i < fields.length; i++) {
            String expFieldName = fields[i].get1();
            boolean expFieldAsc = fields[i].get2();

            assertEquals("Index field mismatch [pos=" + i + ", expField=" + expFieldName + ", actualField=" +
                fieldNames.get(i) + ']', expFieldName.toLowerCase(), fieldNames.get(i).toLowerCase());

            boolean fieldAsc = !idxDesc.descending(expFieldName);

            assertEquals("Index field sort mismatch [pos=" + i + ", field=" + expFieldName +
                ", expAsc=" + expFieldAsc + ", actualAsc=" + fieldAsc + ']', expFieldAsc, fieldAsc);
        }
    }

    /**
     * Assert index doesn't exist on all nodes.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     */
    protected static void assertNoIndex(String cacheName, String tblName, String idxName) {
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
    protected static void assertNoIndex(Ignite node, String cacheName, String tblName, String idxName) {
        IgniteEx node0 = (IgniteEx)node;

        assertNoIndexDescriptor(node0, cacheName, idxName);

        if (affinityNode(node0, cacheName)) {
            QueryTypeDescriptorImpl typeDesc = typeExisting(node0, cacheName, tblName);

            assertNoIndex(typeDesc, idxName);
        }
    }

    /**
     * Assert index doesn't exist in particular node's cache descriptor.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param idxName Index name.
     */
    protected static void assertNoIndexDescriptor(IgniteEx node, String cacheName, String idxName) {
        awaitCompletion();

        DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

        if (desc == null)
            return;

        for (QueryEntity entity : desc.schema().entities()) {
            for (QueryIndex idx : entity.getIndexes()) {
                if (F.eq(idxName, QueryUtils.indexName(entity, idx)))
                    fail("Index exists: " + idxName);
            }
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
     * Assert index doesn't exist.
     *
     * @param typeDesc Type descriptor.
     * @param idxName Index name.
     */
    protected static void assertNoIndex(QueryTypeDescriptorImpl typeDesc, String idxName) {
        assertNull(typeDesc.index(idxName));
    }

    /**
     * Check whether this is affinity node for cache.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return {@code True} if affinity node.
     */
    private static boolean affinityNode(IgniteEx node, String cacheName) {
        if (node.configuration().isClientMode())
            return false;

        DynamicCacheDescriptor cacheDesc = node.context().cache().cacheDescriptor(cacheName);

        IgnitePredicate<ClusterNode> filter = cacheDesc.cacheConfiguration().getNodeFilter();

        return filter == null || filter.apply(node.localNode());
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
    protected static String tableName(Class cls) {
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
     * Get query processor.
     *
     * @param node Node.
     * @return Query processor.
     */
    protected static GridQueryProcessor queryProcessor(Ignite node) {
        return ((IgniteEx)node).context().query();
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
        return F.t(name, asc);
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
     * @throws Exception If failed.
     */
    protected void dynamicIndexCreate(Ignite node, String cacheName, String tblName, QueryIndex idx, boolean ifNotExists)
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
