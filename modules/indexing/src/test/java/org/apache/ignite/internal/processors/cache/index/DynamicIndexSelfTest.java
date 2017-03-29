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

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings("unchecked")
public class DynamicIndexSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
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
    public void testSimpleCreate() throws Exception {
        IgniteEx node = grid(0);

        node.getOrCreateCache(cacheConfiguration());

        GridCacheProcessor cacheProc = node.context().cache();

        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();

        idxFields.put("str", true);

        QueryIndex idx = new QueryIndex().setName("my_idx").setFields(idxFields);

        cacheProc.dynamicIndexCreate(CACHE_NAME, ValueClass.class.getSimpleName(), idx, false).get();

        QueryTypeDescriptorImpl type = typeExisting(node, CACHE_NAME, ValueClass.class);

        assertIndex(type, "my_idx", field("str"));
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
     * Get type on the given node for the given cache and value class. Type must exist.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param valCls Value class.
     * @return Type.
     */
    private static QueryTypeDescriptorImpl typeExisting(IgniteEx node, String cacheName, Class valCls) {
        QueryTypeDescriptorImpl res = type(node, cacheName, valCls.getSimpleName());

        assertNotNull(res);

        return res;
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
        /** String value. */
        @QuerySqlField
        private String str;

        /**
         * Constructor.
         *
         * @param str String value.
         */
        public ValueClass(String str) {
            this.str = str;
        }

        /**
         * @return String value.
         */
        public String stringValue() {
            return str;
        }
    }
}
