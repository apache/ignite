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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.util.JdbcUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for all SQL based indexing SPI implementations.
 */
public abstract class GridIndexingSpiAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TextIndex textIdx = new TextIndex(F.asList("txt"));

    /** */
    private static final LinkedHashMap<String, String> fieldsAA = new LinkedHashMap<>();

    /** */
    private static final LinkedHashMap<String, String> fieldsAB = new LinkedHashMap<>();

    /** */
    private static final LinkedHashMap<String, String> fieldsBA = new LinkedHashMap<>();

    /** */
    private IgniteEx ignite0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /*
     * Fields initialization.
     */
    static {
        fieldsAA.put("id", Long.class.getName());
        fieldsAA.put("name", String.class.getName());
        fieldsAA.put("age", Integer.class.getName());

        fieldsAB.putAll(fieldsAA);
        fieldsAB.put("txt", String.class.getName());

        fieldsBA.putAll(fieldsAA);
        fieldsBA.put("sex", Boolean.class.getName());
    }

    /** */
    private static TypeDesc typeAA = new TypeDesc("A", "A", "A", Collections.<String, Class<?>>emptyMap(), null);

    /** */
    private static TypeDesc typeAB = new TypeDesc("A", "A", "B", Collections.<String, Class<?>>emptyMap(), textIdx);

    /** */
    private static TypeDesc typeBA = new TypeDesc("B", "B", "A", Collections.<String, Class<?>>emptyMap(), null);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite0 = startGrid(0);
    }

    /**
     */
    private CacheConfiguration cacheACfg() {
        CacheConfiguration<?,?> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setName("A");

        QueryEntity eA = new QueryEntity(Integer.class.getName(), "A");
        eA.setFields(fieldsAA);

        QueryEntity eB = new QueryEntity(Integer.class.getName(), "B");
        eB.setFields(fieldsAB);

        List<QueryEntity> list = new ArrayList<>(2);

        list.add(eA);
        list.add(eB);

        QueryIndex idx = new QueryIndex("txt");
        idx.setIndexType(QueryIndexType.FULLTEXT);
        eB.setIndexes(Collections.singleton(idx));

        cfg.setQueryEntities(list);

        return cfg;
    }

    /**
     *
     */
    private CacheConfiguration cacheBCfg() {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setName("B");

        QueryEntity eA = new QueryEntity(Integer.class.getName(), "A");
        eA.setFields(fieldsBA);

        cfg.setQueryEntities(Collections.singleton(eA));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param id Id.
     * @param name Name.
     * @param age Age.
     * @return AA.
     */
    private BinaryObjectBuilder aa(String typeName, long id, String name, int age) {
        BinaryObjectBuilder aBuilder = ignite0.binary().builder(typeName)
                .setField("id", id)
                .setField("name", name)
                .setField("age", age);

        return aBuilder;
    }

    /**
     * @param id Id.
     * @param name Name.
     * @param age Age.
     * @param txt Text.
     * @return AB.
     */
    private BinaryObjectBuilder ab(long id, String name, int age, String txt) {
        BinaryObjectBuilder aBuilder = aa("B", id, name, age);

        aBuilder.setField("txt", txt);

        return aBuilder;
    }

    /**
     * @param id Id.
     * @param name Name.
     * @param age Age.
     * @param sex Sex.
     * @return BA.
     */
    private BinaryObjectBuilder ba(long id, String name, int age, boolean sex) {
        BinaryObjectBuilder builder = aa("A", id, name, age);

        builder.setField("sex", sex);

        return builder;
    }

    /**
     * @param row Row
     * @return Value.
     * @throws IgniteSpiException If failed.
     */
    private BinaryObjectImpl value(IgniteBiTuple<Integer, BinaryObjectImpl> row) throws IgniteSpiException {
        return row.get2();
    }

    /**
     * @return Indexing.
     */
    private IgniteH2Indexing getIndexing() {
        return U.field(ignite0.context().query(), "idx");
    }

    /**
     * @return {@code true} if OFF-HEAP mode should be tested.
     */
    protected boolean offheap() {
        return false;
    }

    /**
     * @param key Key.
     * @return Cache object.
     */
    private KeyCacheObject key(int key) {
        return new TestCacheObject(key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSpi() throws Exception {
        IgniteH2Indexing spi = getIndexing();

        IgniteCache<Integer, BinaryObject> cacheA = ignite0.createCache(cacheACfg());

        IgniteCache<Integer, BinaryObject> cacheB = ignite0.createCache(cacheBCfg());

        assertFalse(spi.queryLocalSql(spi.schema(typeAA.cacheName()), typeAA.cacheName(), "select * from A.A", null,
            Collections.emptySet(), typeAA.name(), null, null).hasNext());

        assertFalse(spi.queryLocalSql(spi.schema(typeAB.cacheName()), typeAB.cacheName(), "select * from A.B", null,
            Collections.emptySet(), typeAB.name(), null, null).hasNext());

        assertFalse(spi.queryLocalSql(spi.schema(typeBA.cacheName()), typeBA.cacheName(), "select * from B.A", null,
            Collections.emptySet(), typeBA.name(), null, null).hasNext());

        assertFalse(spi.queryLocalSql(spi.schema(typeBA.cacheName()), typeBA.cacheName(),
            "select * from B.A, A.B, A.A", null, Collections.emptySet(), typeBA.name(), null, null).hasNext());

        try {
            spi.queryLocalSql(spi.schema(typeBA.cacheName()), typeBA.cacheName(),
                "select aa.*, ab.*, ba.* from A.A aa, A.B ab, B.A ba",
                null, Collections.emptySet(), typeBA.name(), null, null).hasNext();

            fail("Enumerations of aliases in select block must be prohibited");
        }
        catch (IgniteCheckedException ignored) {
            // all fine
        }

        assertFalse(spi.queryLocalSql(spi.schema(typeAB.cacheName()), typeAB.cacheName(), "select ab.* from A.B ab",
            null, Collections.emptySet(), typeAB.name(), null, null).hasNext());

        assertFalse(spi.queryLocalSql(spi.schema(typeBA.cacheName()), typeBA.cacheName(),
            "select   ba.*   from B.A  as ba", null, Collections.emptySet(), typeBA.name(), null, null).hasNext());

        cacheA.put(1, aa("A", 1, "Vasya", 10).build());
        cacheA.put(1, ab(1, "Vasya", 20, "Some text about Vasya goes here.").build());
        cacheB.put(1, ba(2, "Petya", 25, true).build());
        cacheB.put(1, ba(2, "Kolya", 25, true).build());
        cacheA.put(2, aa("A", 2, "Valera", 19).build());
        cacheA.put(3, aa("A", 3, "Borya", 18).build());
        cacheA.put(4, ab(4, "Vitalya", 20, "Very Good guy").build());

        // Query data.
        Iterator<IgniteBiTuple<Integer, BinaryObjectImpl>> res = spi.queryLocalSql(spi.schema(typeAA.cacheName()),
            typeAA.cacheName(), "from a order by age", null, Collections.emptySet(), typeAA.name(), null, null);

        assertTrue(res.hasNext());
        assertEquals(aa("A", 3, "Borya", 18).build(), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(aa("A", 2, "Valera", 19).build(), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.queryLocalSql(spi.schema(typeAA.cacheName()), typeAA.cacheName(),
            "select aa.* from a aa order by aa.age", null, Collections.emptySet(), typeAA.name(), null, null);

        assertTrue(res.hasNext());
        assertEquals(aa("A", 3, "Borya", 18).build(), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(aa("A", 2, "Valera", 19).build(), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.queryLocalSql(spi.schema(typeAB.cacheName()), typeAB.cacheName(), "from b order by name", null,
            Collections.emptySet(), typeAB.name(), null, null);

        assertTrue(res.hasNext());
        assertEquals(ab(1, "Vasya", 20, "Some text about Vasya goes here.").build(), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy").build(), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.queryLocalSql(spi.schema(typeAB.cacheName()), typeAB.cacheName(),
            "select bb.* from b as bb order by bb.name", null, Collections.emptySet(), typeAB.name(), null, null);

        assertTrue(res.hasNext());
        assertEquals(ab(1, "Vasya", 20, "Some text about Vasya goes here.").build(), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy").build(), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.queryLocalSql(spi.schema(typeBA.cacheName()), typeBA.cacheName(), "from a", null,
            Collections.emptySet(), typeBA.name(), null, null);

        assertTrue(res.hasNext());
        assertEquals(ba(2, "Kolya", 25, true).build(), value(res.next()));
        assertFalse(res.hasNext());

        // Text queries
        Iterator<IgniteBiTuple<Integer, BinaryObjectImpl>> txtRes = spi.queryLocalText(spi.schema(typeAB.cacheName()),
            typeAB.cacheName(), "good", typeAB.name(), null);

        assertTrue(txtRes.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy").build(), value(txtRes.next()));
        assertFalse(txtRes.hasNext());

        // Fields query
        GridQueryFieldsResult fieldsRes =
            spi.queryLocalSqlFields(spi.schema("A"), "select a.a.name n1, a.a.age a1, b.a.name n2, " +
            "b.a.age a2 from a.a, b.a where a.a.id = b.a.id ", Collections.emptySet(), null, false, false, 0, null);

        String[] aliases = {"N1", "A1", "N2", "A2"};
        Object[] vals = { "Valera", 19, "Kolya", 25};

        IgniteSpiCloseableIterator<List<?>> it = fieldsRes.iterator();

        assertTrue(it.hasNext());

        List<?> fields = it.next();

        assertEquals(4, fields.size());

        int i = 0;

        for (Object f : fields) {
            assertEquals(aliases[i], fieldsRes.metaData().get(i).fieldName());
            assertEquals(vals[i++], f);
        }

        assertFalse(it.hasNext());

        // Remove
        cacheA.remove(2);
        cacheB.remove(1);
    }

    /**
     * Test long queries write explain warnings into log.
     *
     * @throws Exception If failed.
     */
    public void testLongQueries() throws Exception {
        IgniteH2Indexing spi = getIndexing();

        ignite0.createCache(cacheACfg());

        long longQryExecTime = IgniteConfiguration.DFLT_LONG_QRY_WARN_TIMEOUT;

        GridStringLogger log = new GridStringLogger(false, this.log);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(spi, "log");

        try {
            GridTestUtils.setFieldValue(spi, "log", log);

            String sql = "select sum(x) FROM SYSTEM_RANGE(?, ?)";

            long now = U.currentTimeMillis();
            long time = now;

            long range = 1000000L;

            while (now - time <= longQryExecTime * 3 / 2) {
                time = now;
                range *= 3;

                GridQueryFieldsResult res = spi.queryLocalSqlFields(spi.schema("A"), sql, Arrays.<Object>asList(1,
                    range), null, false, false, 0, null);

                assert res.iterator().hasNext();

                now = U.currentTimeMillis();
            }

            String res = log.toString();

            assertTrue(res.contains("/* PUBLIC.RANGE_INDEX */"));
        }
        finally {
            GridTestUtils.setFieldValue(spi, "log", oldLog);
        }
    }

    /**
     * Index descriptor.
     */
    private static class TextIndex implements GridQueryIndexDescriptor {
        /** */
        private final Collection<String> fields;

        /**
         * @param fields Fields.
         */
        private TextIndex(Collection<String> fields) {
            this.fields = Collections.unmodifiableCollection(fields);
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> fields() {
            return fields;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public QueryIndexType type() {
            return QueryIndexType.FULLTEXT;
        }

        /** {@inheritDoc} */
        @Override public int inlineSize() {
            return 0;
        }
    }

    /**
     * Type descriptor.
     */
    private static class TypeDesc implements GridQueryTypeDescriptor {
        /** */
        private final String name;

        /** */
        private final String cacheName;

        /** */
        private final String schemaName;

        /** */
        private final Map<String, Class<?>> valFields;

        /** */
        private final GridQueryIndexDescriptor textIdx;

        /**
         * @param cacheName Cache name.
         * @param schemaName Schema name.
         * @param name Type name.
         * @param valFields Fields.
         * @param textIdx Fulltext index.
         */
        private TypeDesc(String cacheName, String schemaName, String name, Map<String, Class<?>> valFields, GridQueryIndexDescriptor textIdx) {
            this.name = name;
            this.cacheName = cacheName;
            this.schemaName = schemaName;
            this.valFields = Collections.unmodifiableMap(valFields);
            this.textIdx = textIdx;
        }

        /** {@inheritDoc} */
        @Override public String affinityKey() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public String schemaName() {
            return schemaName;
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return null;
        }

        /**
         * @return Cache name.
         */
        String cacheName() {
            return cacheName;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> fields() {
            return valFields;
        }

        /** {@inheritDoc} */
        @Override public GridQueryProperty property(final String name) {
            return new GridQueryProperty() {
                /** */
                @Override public Object value(Object key, Object val) throws IgniteCheckedException {
                    return TypeDesc.this.value(name, key, val);
                }

                /** */
                @Override public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException {
                    throw new UnsupportedOperationException();
                }

                /** */
                @Override public String name() {
                    return name;
                }

                /** */
                @Override public Class<?> type() {
                    return Object.class;
                }

                /** */
                @Override public boolean key() {
                    return false;
                }

                /** */
                @Override public GridQueryProperty parent() {
                    return null;
                }

                /** */
                @Override public boolean notNull() {
                    return false;
                }

                /** */
                @Override public Object defaultValue() {
                    return null;
                }

                /** */
                @Override public int precision() {
                    return -1;
                }

                /** */
                @Override public int scale() {
                    return -1;
                }
            };
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public <T> T value(String field, Object key, Object val) throws IgniteSpiException {
            assert !F.isEmpty(field);

            assert key instanceof Integer;

            Map<String, T> m = (Map<String, T>)val;

            if (m.containsKey(field))
                return m.get(field);

            return null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void setValue(String field, Object key, Object val, Object propVal) throws IgniteCheckedException {
            assert !F.isEmpty(field);

            assert key instanceof Integer;

            Map<String, Object> m = (Map<String, Object>)val;

            m.put(field, propVal);
        }

        /** */
        @Override public Map<String, GridQueryIndexDescriptor> indexes() {
            return Collections.emptyMap();
        }

        /** */
        @Override public GridQueryIndexDescriptor textIndex() {
            return textIdx;
        }

        /** */
        @Override public Class<?> valueClass() {
            return Object.class;
        }

        /** */
        @Override public Class<?> keyClass() {
            return Integer.class;
        }

        /** */
        @Override public String keyTypeName() {
            return null;
        }

        /** */
        @Override public String valueTypeName() {
            return null;
        }

        /** */
        @Override public boolean valueTextIndex() {
            return textIdx == null;
        }

        /** */
        @Override public int typeId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String keyFieldName() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String valueFieldName() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String keyFieldAlias() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String valueFieldAlias() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void validateKeyAndValue(Object key, Object value) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setDefaults(Object key, Object val) throws IgniteCheckedException {
            // No-op.
        }
    }

    /**
     */
    private static class TestCacheObject implements KeyCacheObject {
        /** */
        private Object val;

        /** */
        private int part;

        /**
         * @param val Value.
         */
        private TestCacheObject(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
            return (T)val;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return part;
        }

        /** {@inheritDoc} */
        @Override public void partition(int part) {
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
            return JdbcUtils.serialize(val, null);
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int putValue(long addr) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(final ByteBuffer buf, final int off, final int len)
            throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte cacheObjectType() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isPlatformType() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject copy(int part) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean internal() {
            return false;
        }
    }
}
