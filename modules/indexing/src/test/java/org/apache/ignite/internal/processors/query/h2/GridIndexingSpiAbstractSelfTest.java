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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests for all SQL based indexing SPI implementations.
 */
public abstract class GridIndexingSpiAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TextIndex textIdx = new TextIndex(F.asList("txt"));

    /** */
    private static final Map<String, Class<?>> fieldsAA = new HashMap<>();

    /** */
    private static final Map<String, Class<?>> fieldsAB = new HashMap<>();

    /** */
    private static final Map<String, Class<?>> fieldsBA = new HashMap<>();

    /**
     * Fields initialization.
     */
    static {
        fieldsAA.put("id", Long.class);
        fieldsAA.put("name", String.class);
        fieldsAA.put("age", Integer.class);

        fieldsAB.putAll(fieldsAA);
        fieldsAB.put("txt", String.class);

        fieldsBA.putAll(fieldsAA);
        fieldsBA.put("sex", Boolean.class);
    }

    /** */
    private static TypeDesc typeAA = new TypeDesc("A", "A", fieldsAA, null);

    /** */
    private static TypeDesc typeAB = new TypeDesc("A", "B", fieldsAB, textIdx);

    /** */
    private static TypeDesc typeBA = new TypeDesc("B", "A", fieldsBA, null);

    /** */
    private IgniteH2Indexing idx = new IgniteH2Indexing();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        getTestResources().inject(idx);

        startIndexing(idx);
    }

    /** {@inheritDoc} */
    protected void startIndexing(IgniteH2Indexing spi) throws Exception {
        spi.start(null);

        spi.registerCache(cacheCfg("A"));
        spi.registerCache(cacheCfg("B"));
    }

    private CacheConfiguration cacheCfg(String name) {
        CacheConfiguration<?,?> cfg = new CacheConfiguration<>();

        cfg.setName(name);

        return cfg;
    }

    @Override protected void afterTest() throws Exception {
        idx.stop();

        idx = null;
    }

    /**
     * @param id Id.
     * @param name Name.
     * @param age Age.
     * @return AA.
     */
    private Map<String, Object> aa(long id, String name, int age) {
        Map<String, Object> map = new HashMap<>();

        map.put("id", id);
        map.put("name", name);
        map.put("age", age);

        return map;
    }

    /**
     * @param id Id.
     * @param name Name.
     * @param age Age.
     * @param txt Text.
     * @return AB.
     */
    private Map<String, Object> ab(long id, String name, int age, String txt) {
        Map<String, Object> map = aa(id, name, age);

        map.put("txt", txt);

        return map;
    }

    /**
     * @param id Id.
     * @param name Name.
     * @param age Age.
     * @param sex Sex.
     * @return BA.
     */
    private Map<String, Object> ba(long id, String name, int age, boolean sex) {
        Map<String, Object> map = aa(id, name, age);

        map.put("sex", sex);

        return map;
    }

    /**
     * @param row Row
     * @return Value.
     * @throws IgniteSpiException If failed.
     */
    private Map<String, Object> value(IgniteBiTuple<Integer, Map<String, Object>> row) throws IgniteSpiException {
        return row.get2();
    }

    /**
     * @return Indexing.
     */
    private IgniteH2Indexing getIndexing() {
        return idx;
    }

    protected boolean offheap() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSpi() throws Exception {
        IgniteH2Indexing spi = getIndexing();

        assertEquals(-1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(-1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(-1, spi.size(typeBA.space(), typeBA, null));

        spi.registerType(typeAA.space(), typeAA);

        assertEquals(0, spi.size(typeAA.space(), typeAA, null));
        assertEquals(-1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(-1, spi.size(typeBA.space(), typeBA, null));

        spi.registerType(typeAB.space(), typeAB);

        assertEquals(0, spi.size(typeAA.space(), typeAA, null));
        assertEquals(0, spi.size(typeAB.space(), typeAB, null));
        assertEquals(-1, spi.size(typeBA.space(), typeBA, null));

        spi.registerType(typeBA.space(), typeBA);

        // Initially all is empty.
        assertEquals(0, spi.size(typeAA.space(), typeAA, null));
        assertEquals(0, spi.size(typeAB.space(), typeAB, null));
        assertEquals(0, spi.size(typeBA.space(), typeBA, null));

        assertFalse(spi.query(typeAA.space(), "select * from A.A", Collections.emptySet(), typeAA, null).hasNext());
        assertFalse(spi.query(typeAB.space(), "select * from A.B", Collections.emptySet(), typeAB, null).hasNext());
        assertFalse(spi.query(typeBA.space(), "select * from B.A", Collections.emptySet(), typeBA, null).hasNext());

        // Nothing to remove.
        spi.remove("A", 1, aa(1, "", 10));
        spi.remove("B", 1, ba(1, "", 10, true));

        spi.store(typeAA.space(), typeAA, 1, aa(1, "Vasya", 10), "v1".getBytes(), 0);

        assertEquals(1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(0, spi.size(typeAB.space(), typeAB, null));
        assertEquals(0, spi.size(typeBA.space(), typeBA, null));

        spi.store(typeAB.space(), typeAB, 1, ab(1, "Vasya", 20, "Some text about Vasya goes here."),
            "v2".getBytes(), 0);

        // In one space all keys must be unique.
        assertEquals(0, spi.size(typeAA.space(), typeAA, null));
        assertEquals(1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(0, spi.size(typeBA.space(), typeBA, null));

        spi.store(typeBA.space(), typeBA, 1, ba(2, "Petya", 25, true), "v3".getBytes(), 0);

        // No replacement because of different space.
        assertEquals(0, spi.size(typeAA.space(), typeAA, null));
        assertEquals(1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(1, spi.size(typeBA.space(), typeBA, null));

        spi.store(typeBA.space(), typeBA, 1, ba(2, "Kolya", 25, true), "v4".getBytes(), 0);

        // Replacement in the same table.
        assertEquals(0, spi.size(typeAA.space(), typeAA, null));
        assertEquals(1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(1, spi.size(typeBA.space(), typeBA, null));

        spi.store(typeAA.space(), typeAA, 2, aa(2, "Valera", 19), "v5".getBytes(), 0);

        assertEquals(1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(1, spi.size(typeBA.space(), typeBA, null));

        spi.store(typeAA.space(), typeAA, 3, aa(3, "Borya", 18), "v6".getBytes(), 0);

        assertEquals(2, spi.size(typeAA.space(), typeAA, null));
        assertEquals(1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(1, spi.size(typeBA.space(), typeBA, null));

        spi.store(typeAB.space(), typeAB, 4, ab(4, "Vitalya", 20, "Very Good guy"), "v7".getBytes(), 0);

        assertEquals(2, spi.size(typeAA.space(), typeAA, null));
        assertEquals(2, spi.size(typeAB.space(), typeAB, null));
        assertEquals(1, spi.size(typeBA.space(), typeBA, null));

        // Query data.
        Iterator<IgniteBiTuple<Integer, Map<String, Object>>> res =
            spi.query(typeAA.space(), "from a order by age", Collections.emptySet(), typeAA, null);

        assertTrue(res.hasNext());
        assertEquals(aa(3, "Borya", 18), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(aa(2, "Valera", 19), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.query(typeAB.space(), "from b order by name", Collections.emptySet(), typeAB, null);

        assertTrue(res.hasNext());
        assertEquals(ab(1, "Vasya", 20, "Some text about Vasya goes here."), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy"), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.query(typeBA.space(), "from a", Collections.emptySet(), typeBA, null);

        assertTrue(res.hasNext());
        assertEquals(ba(2, "Kolya", 25, true), value(res.next()));
        assertFalse(res.hasNext());

        // Text queries
        Iterator<IgniteBiTuple<Integer, Map<String, Object>>> txtRes = spi.queryText(typeAB.space(), "good",
            typeAB, null);

        assertTrue(txtRes.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy"), value(txtRes.next()));
        assertFalse(txtRes.hasNext());

        // Fields query
        GridQueryFieldsResult fieldsRes =
            spi.queryFields("A", "select a.a.name n1, a.a.age a1, b.a.name n2, " +
            "b.a.age a2 from a.a, b.a where a.a.id = b.a.id ", Collections.emptySet(), null);

        String[] aliases = {"N1", "A1", "N2", "A2"};
        Object[] vals = { "Valera", 19, "Kolya", 25};

        assertTrue(fieldsRes.iterator().hasNext());

        List<?> fields = fieldsRes.iterator().next();

        assertEquals(4, fields.size());

        int i = 0;

        for (Object f : fields) {
            assertEquals(aliases[i], fieldsRes.metaData().get(i).fieldName());
            assertEquals(vals[i++], f);
        }

        assertFalse(fieldsRes.iterator().hasNext());

        // Remove
        spi.remove(typeAA.space(), 2, aa(2, "Valera", 19));

        assertEquals(1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(2, spi.size(typeAB.space(), typeAB, null));
        assertEquals(1, spi.size(typeBA.space(), typeBA, null));

        spi.remove(typeBA.space(), 1, ba(2, "Kolya", 25, true));

        assertEquals(1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(2, spi.size(typeAB.space(), typeAB, null));
        assertEquals(0, spi.size(typeBA.space(), typeBA, null));

        boolean h2IdxOffheap = offheap();

        // At the time of this writing index rebuilding is not supported for GridH2Indexing with off-heap storage.
        if (!h2IdxOffheap) {
            // Rebuild

            spi.rebuildIndexes(typeAB.space(), typeAB);

            assertEquals(1, spi.size(typeAA.space(), typeAA, null));
            assertEquals(2, spi.size(typeAB.space(), typeAB, null));
            assertEquals(0, spi.size(typeBA.space(), typeBA, null));

            // For invalid space name/type should not fail.
            spi.rebuildIndexes("not_existing_space", typeAA);
            spi.rebuildIndexes(typeAA.space(), new TypeDesc("C", "C", fieldsAA, null));
        }

        // Unregister.
        spi.unregisterType(typeAA.space(), typeAA);

        assertEquals(-1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(2, spi.size(typeAB.space(), typeAB, null));
        assertEquals(0, spi.size(typeBA.space(), typeBA, null));

        spi.unregisterType(typeAB.space(), typeAB);

        assertEquals(-1, spi.size(typeAA.space(), typeAA, null));
        assertEquals(-1, spi.size(typeAB.space(), typeAB, null));
        assertEquals(0, spi.size(typeBA.space(), typeBA, null));

        spi.unregisterType(typeBA.space(), typeBA);

        // Should not store but should not fail as well.
        spi.store(typeAA.space(), typeAA, 10, aa(1, "Fail", 100500), "v220".getBytes(), 0);

        assertEquals(-1, spi.size(typeAA.space(), typeAA, null));
    }

    /**
     * Test long queries write explain warnings into log.
     *
     * @throws Exception If failed.
     */
    public void testLongQueries() throws Exception {
        IgniteH2Indexing spi = getIndexing();

        long longQryExecTime = CacheConfiguration.DFLT_LONG_QRY_WARN_TIMEOUT;

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

                GridQueryFieldsResult res = spi.queryFields("A", sql, Arrays.<Object>asList(1, range), null);

                assert res.iterator().hasNext();

                now = U.currentTimeMillis();
            }

            String res = log.toString();

            F.println(res);

            assert res.contains("/* PUBLIC.RANGE_INDEX */");
        }
        finally {
            GridTestUtils.setFieldValue(spi, "log", oldLog);
        }
    }

    public void _testResultReuse() throws Exception {
        final IgniteH2Indexing spi = getIndexing();

        multithreaded(new Callable<Object>() {
              @Override public Object call() throws Exception {
                  return spi.queryFields(null, "SELECT sum(x) + sum(x) + sum(x) + sum(x) FROM SYSTEM_RANGE(?, ?)",
                      F.<Object>asList(0, 7000000), null);
              }
          }, 5);
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
        @Override public Collection<String> fields() {
            return fields;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridQueryIndexType type() {
            return GridQueryIndexType.FULLTEXT;
        }
    }

    /**
     * Type descriptor.
     */
    private static class TypeDesc implements GridQueryTypeDescriptor {
        /** */
        private final String name;

        /** */
        private final String space;

        /** */
        private final Map<String, Class<?>> valFields;

        /** */
        private final GridQueryIndexDescriptor textIdx;

        /**
         * @param space Space name.
         * @param name Type name.
         * @param valFields Fields.
         * @param textIdx Fulltext index.
         */
        private TypeDesc(String space, String name, Map<String, Class<?>> valFields, GridQueryIndexDescriptor textIdx) {
            this.name = name;
            this.space = space;
            this.valFields = Collections.unmodifiableMap(valFields);
            this.textIdx = textIdx;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /**
         * @return Space name.
         */
        public String space() {
            return space;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> fields() {
            return valFields;
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

        /** */
        @Override public Map<String, GridQueryIndexDescriptor> indexes() {
            return textIdx == null ? Collections.<String, GridQueryIndexDescriptor>emptyMap() :
                Collections.singletonMap("index", textIdx);
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
        @Override public boolean valueTextIndex() {
            return textIdx == null;
        }
    }
}
