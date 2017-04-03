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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for dynamic schema changes.
 */
@SuppressWarnings("unchecked")
public class AbstractSchemaSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    protected static final String CACHE_NAME = "cache";

    /** Cache with case sensitive field names. */
    protected static final String CACHE_NAME_SENSITIVE = "cacheSensitive";

    /** Table name. */
    protected static final String TBL_NAME = tableName(ValueClass.class);

    /** Table name 2. */
    protected static final String TBL_NAME_2 = tableName(ValueClass2.class);

    /** Index name 1. */
    protected static final String IDX_NAME = "idx_1";

    /** Index name 2. */
    protected static final String IDX_NAME_2 = "idx_2";

    /** Index name 3. */
    protected static final String IDX_NAME_3 = "idx_3";

    /** Field 1. */
    protected static final String FIELD_NAME_1 = "field1";

    /** Field 1. */
    protected static final String FIELD_NAME_2 = "field2";

    /** Field 3. */
    protected static final String FIELD_NAME_3 = "field3";

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
     * Assert index state on all nodes.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param fields Fields.
     */
    protected static void assertIndex(String cacheName, String tblName, String idxName,
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
    protected static void assertIndex(IgniteEx node, String cacheName, String tblName, String idxName,
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
    protected static void assertNoIndex(String cacheName, String tblName, String idxName) {
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
    protected static void assertNoIndex(IgniteEx node, String cacheName, String tblName, String idxName) {
        QueryTypeDescriptorImpl typeDesc = typeExisting(node, cacheName, tblName);

        assertNoIndex(typeDesc, idxName);
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
     * Get table name for class.
     *
     * @param cls Class.
     * @return Table name.
     */
    protected static String tableName(Class cls) {
        return cls.getSimpleName();
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
     * Key class with two ids.
     */
    public static class KeyClass2 {
        /** ID. */
        @QuerySqlField
        private long id;

        /** Another ID. */
        @QuerySqlField
        private long Id;

        /**
         * Constructor.
         *
         * @param id ID.
         * @param Id Another ID.
         */
        public KeyClass2(long id, long Id) {
            this.id = id;

            this.Id = id;
        }

        /**
         * @return ID.
         */
        public long id() {
            return id;
        }

        /**
         * @return Another ID.
         */
        public long Id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyClass2 keyClass2 = (KeyClass2) o;

            if (id != keyClass2.id) return false;
            return Id == keyClass2.Id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (int) (Id ^ (Id >>> 32));
            return result;
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
}
