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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings("unchecked")
public class DynamicIndexSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    private static final String CACHE_NAME = "cache";

    /** Table name. */
    private static final String TBL_NAME = tableName(ValueClass.class);

    /** Index name 1. */
    private static final String IDX_NAME = "idx_1";

    /** Index name 2. */
    private static final String IDX_NAME_2 = "idx_2";

    /** Index name 3. */
    private static final String IDX_NAME_3 = "idx_3";

    /** Field 1. */
    private static final String FIELD_NAME = "field1";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        Thread.sleep(2000);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).getOrCreateCache(cacheConfiguration());

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(CACHE_NAME);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test simple index create.
     *
     * @throws Exception If failed.
     */
    public void testCreate() throws Exception {
        final QueryIndex idx = index(IDX_NAME, field(FIELD_NAME));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();

                return null;
            }
        }, IgniteCheckedException.class, null);

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME));
    }

    /**
     * Test simple index drop.
     *
     * @throws Exception If failed.
     */
    public void testDrop() throws Exception {
        QueryIndex idx = index(IDX_NAME, field(FIELD_NAME));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME));

        queryProcessor(grid(0)).dynamicIndexDrop(CACHE_NAME, IDX_NAME, true).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /**
     * Get query processor.
     *
     * @param node Node.
     * @return Query processor.
     */
    private GridQueryProcessor queryProcessor(Ignite node) {
        return ((IgniteEx)node).context().query();
    }

    /**
     * Get table name for class.
     *
     * @param cls Class.
     * @return Table name.
     */
    private static String tableName(Class cls) {
        return cls.getSimpleName();
    }

    /**
     * Convenient method for index creation.
     *
     * @param name Name.
     * @param fields Fields.
     * @return Index.
     */
    private QueryIndex index(String name, IgniteBiTuple<String, Boolean>... fields) {
        QueryIndex idx = new QueryIndex();

        idx.setName(name);

        LinkedHashMap<String, Boolean> fields0 = new LinkedHashMap<>();

        for (IgniteBiTuple<String, Boolean> field : fields)
            fields0.put(field.getKey(), field.getValue());

        idx.setFields(fields0);

        return idx;
    }

    /**
     * Assert index state on all nodes.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param fields Fields.
     */
    private void assertIndex(String cacheName, String tblName, String idxName,
        IgniteBiTuple<String, Boolean>... fields) {
        for (Ignite node : Ignition.allGrids())
            assertIndex((IgniteEx)node, cacheName, tblName, idxName, fields);
    }

    /**
     * Assert index state on particular node.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param fields Fields.
     */
    private void assertIndex(IgniteEx node, String cacheName, String tblName, String idxName,
        IgniteBiTuple<String, Boolean>... fields) {
        QueryTypeDescriptorImpl typeDesc = typeExisting(node, cacheName, tblName);

        assertIndex(typeDesc, idxName, fields);
    }

    /**
     * Assert index state.
     *
     * @param typeDesc Type descriptor.
     * @param idxName Index name.
     * @param fields Fields (order is important).
     */
    private void assertIndex(QueryTypeDescriptorImpl typeDesc, String idxName,
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

            assertEquals("Index field mismatch [pos=" + i + ", expField=" + expFieldName +
                ", actualField=" + fieldNames.get(i) + ']', expFieldName, fieldNames.get(i));

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
    private void assertNoIndex(String cacheName, String tblName, String idxName) {
        for (Ignite node : Ignition.allGrids())
            assertNoIndex((IgniteEx)node, cacheName, tblName, idxName);
    }

    /**
     * Assert index doesn't exist on particular node.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     */
    private void assertNoIndex(IgniteEx node, String cacheName, String tblName, String idxName) {
        QueryTypeDescriptorImpl typeDesc = typeExisting(node, cacheName, tblName);

        assertNoIndex(typeDesc, idxName);
    }

    /**
     * Assert index doesn't exist.
     *
     * @param typeDesc Type descriptor.
     * @param idxName Index name.
     */
    private void assertNoIndex(QueryTypeDescriptorImpl typeDesc, String idxName) {
        assertNull(typeDesc.index(idxName));
    }

    /**
     * Field for index state check (ascending).
     *
     * @param name Name.
     * @return Field.
     */
    private IgniteBiTuple<String, Boolean> field(String name) {
        return field(name, true);
    }

    /**
     * Field for index state check.
     *
     * @param name Name.
     * @param asc Ascending flag.
     * @return Field.
     */
    private IgniteBiTuple<String, Boolean> field(String name, boolean asc) {
        return F.t(name, asc);
    }


    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration<KeyClass, ValueClass>()
            .setName(CACHE_NAME)
            .setIndexedTypes(KeyClass.class, ValueClass.class);
    }

    /**
     * Get type on the given node for the given cache and table name. Type must exist.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @return Type.
     */
    private static QueryTypeDescriptorImpl typeExisting(IgniteEx node, String cacheName, String tblName) {
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
    @Nullable private static QueryTypeDescriptorImpl type(IgniteEx node, String cacheName, String tblName) {
        return types(node, cacheName).get(tblName);
    }

    /**
     * Get available types on the given node for the given cache.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Map from table name to type.
     */
    private static Map<String, QueryTypeDescriptorImpl> types(IgniteEx node, String cacheName) {
        Map<String, QueryTypeDescriptorImpl> res = new HashMap<>();

        Collection<GridQueryTypeDescriptor> descs = node.context().query().types(cacheName);

        for (GridQueryTypeDescriptor desc : descs) {
            QueryTypeDescriptorImpl desc0 = (QueryTypeDescriptorImpl)desc;

            res.put(desc0.tableName(), desc0);
        }

        return res;
    }

    /**
     * Key class.
     */
    private static class KeyClass {
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
    }

    /**
     * Key class.
     */
    private static class ValueClass {
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
}
