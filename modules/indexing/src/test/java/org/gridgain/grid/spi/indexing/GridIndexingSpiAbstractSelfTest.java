/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests for all SQL based indexing SPI implementations.
 */
public abstract class GridIndexingSpiAbstractSelfTest<X extends GridIndexingSpi>
    extends GridSpiAbstractTest<X> {
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

    /** {@inheritDoc} */
    @Override protected void spiConfigure(X spi) throws Exception {
        super.spiConfigure(spi);

        spi.registerMarshaller(new IdxMarshaller(getTestResources().getMarshaller()));

        spi.registerSpace("A");
        spi.registerSpace("B");
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
     * @param val Value.
     * @param <T> Value type.
     * @return Indexing entity.
     * @throws GridException If failed.
     */
    private <T> GridIndexingEntity<T> entity(T val) throws GridException {
        return new GridIndexingEntityAdapter<>(val, getTestResources().getMarshaller().marshal(val));
    }

    /**
     * @param row Row
     * @return Value.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    private Map<String, Object> value(GridIndexingKeyValueRow<Integer, Map<String, Object>> row) throws IgniteSpiException {
        return row.value().value();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSpi() throws Exception {
        X spi = getSpi();

        assertEquals(-1, spi.size(typeAA.space(), typeAA));
        assertEquals(-1, spi.size(typeAB.space(), typeAB));
        assertEquals(-1, spi.size(typeBA.space(), typeBA));

        spi.registerType(typeAA.space(), typeAA);

        assertEquals(0, spi.size(typeAA.space(), typeAA));
        assertEquals(-1, spi.size(typeAB.space(), typeAB));
        assertEquals(-1, spi.size(typeBA.space(), typeBA));

        spi.registerType(typeAB.space(), typeAB);

        assertEquals(0, spi.size(typeAA.space(), typeAA));
        assertEquals(0, spi.size(typeAB.space(), typeAB));
        assertEquals(-1, spi.size(typeBA.space(), typeBA));

        spi.registerType(typeBA.space(), typeBA);

        // Initially all is empty.
        assertEquals(0, spi.size(typeAA.space(), typeAA));
        assertEquals(0, spi.size(typeAB.space(), typeAB));
        assertEquals(0, spi.size(typeBA.space(), typeBA));

        assertFalse(spi.query(typeAA.space(), "select * from A.A", Collections.emptySet(), typeAA, null).hasNext());
        assertFalse(spi.query(typeAB.space(), "select * from A.B", Collections.emptySet(), typeAB, null).hasNext());
        assertFalse(spi.query(typeBA.space(), "select * from B.A", Collections.emptySet(), typeBA, null).hasNext());

        // Nothing to remove.
        assertFalse(spi.remove("A", new GridIndexingEntityAdapter<>(1, null)));
        assertFalse(spi.remove("B", new GridIndexingEntityAdapter<>(1, null)));

        spi.store(typeAA.space(), typeAA, entity(1), entity(aa(1, "Vasya", 10)), "v1".getBytes(), 0);

        assertEquals(1, spi.size(typeAA.space(), typeAA));
        assertEquals(0, spi.size(typeAB.space(), typeAB));
        assertEquals(0, spi.size(typeBA.space(), typeBA));

        spi.store(typeAB.space(), typeAB, entity(1), entity(ab(1, "Vasya", 20, "Some text about Vasya goes here.")),
            "v2".getBytes(), 0);

        // In one space all keys must be unique.
        assertEquals(0, spi.size(typeAA.space(), typeAA));
        assertEquals(1, spi.size(typeAB.space(), typeAB));
        assertEquals(0, spi.size(typeBA.space(), typeBA));

        spi.store(typeBA.space(), typeBA, entity(1), entity(ba(2, "Petya", 25, true)), "v3".getBytes(), 0);

        // No replacement because of different space.
        assertEquals(0, spi.size(typeAA.space(), typeAA));
        assertEquals(1, spi.size(typeAB.space(), typeAB));
        assertEquals(1, spi.size(typeBA.space(), typeBA));

        spi.store(typeBA.space(), typeBA, entity(1), entity(ba(2, "Kolya", 25, true)), "v4".getBytes(), 0);

        // Replacement in the same table.
        assertEquals(0, spi.size(typeAA.space(), typeAA));
        assertEquals(1, spi.size(typeAB.space(), typeAB));
        assertEquals(1, spi.size(typeBA.space(), typeBA));

        spi.store(typeAA.space(), typeAA, entity(2), entity(aa(2, "Valera", 19)), "v5".getBytes(), 0);

        assertEquals(1, spi.size(typeAA.space(), typeAA));
        assertEquals(1, spi.size(typeAB.space(), typeAB));
        assertEquals(1, spi.size(typeBA.space(), typeBA));

        spi.store(typeAA.space(), typeAA, entity(3), entity(aa(3, "Borya", 18)), "v6".getBytes(), 0);

        assertEquals(2, spi.size(typeAA.space(), typeAA));
        assertEquals(1, spi.size(typeAB.space(), typeAB));
        assertEquals(1, spi.size(typeBA.space(), typeBA));

        spi.store(typeAB.space(), typeAB, entity(4), entity(ab(4, "Vitalya", 20, "Very Good guy")), "v7".getBytes(), 0);

        assertEquals(2, spi.size(typeAA.space(), typeAA));
        assertEquals(2, spi.size(typeAB.space(), typeAB));
        assertEquals(1, spi.size(typeBA.space(), typeBA));

        // Query data.
        Iterator<GridIndexingKeyValueRow<Integer, Map<String, Object>>> res =
            spi.query(typeAA.space(), "select * from a order by age", Collections.emptySet(), typeAA, null);

        assertTrue(res.hasNext());
        assertEquals(aa(3, "Borya", 18), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(aa(2, "Valera", 19), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.query(typeAB.space(), "select * from b order by name", Collections.emptySet(), typeAB, null);

        assertTrue(res.hasNext());
        assertEquals(ab(1, "Vasya", 20, "Some text about Vasya goes here."), value(res.next()));
        assertTrue(res.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy"), value(res.next()));
        assertFalse(res.hasNext());

        res = spi.query(typeBA.space(), "select * from a", Collections.emptySet(), typeBA, null);

        assertTrue(res.hasNext());
        assertEquals(ba(2, "Kolya", 25, true), value(res.next()));
        assertFalse(res.hasNext());

        // Text queries
        Iterator<GridIndexingKeyValueRow<Integer, Map<String, Object>>> txtRes = spi.queryText(typeAB.space(), "good",
            typeAB, null);

        assertTrue(txtRes.hasNext());
        assertEquals(ab(4, "Vitalya", 20, "Very Good guy"), value(txtRes.next()));
        assertFalse(txtRes.hasNext());

        // Fields query
        GridIndexingFieldsResult fieldsRes =
            spi.queryFields(null, "select a.a.name n1, a.a.age a1, b.a.name n2, " +
            "b.a.age a2 from a.a, b.a where a.a.id = b.a.id ", Collections.emptySet(), null);

        String[] aliases = {"N1", "A1", "N2", "A2"};
        Object[] vals = { "Valera", 19, "Kolya", 25};

        assertTrue(fieldsRes.iterator().hasNext());

        List<GridIndexingEntity<?>> fields = fieldsRes.iterator().next();

        assertEquals(4, fields.size());

        int i = 0;

        for (GridIndexingEntity<?> f : fields) {
            assertEquals(aliases[i], fieldsRes.metaData().get(i).fieldName());
            assertEquals(vals[i++], f.value());
        }

        assertFalse(fieldsRes.iterator().hasNext());

        // Query on not existing table should not fail.
        assertFalse(spi.queryFields(null, "select * from not_existing_table",
            Collections.emptySet(), null).iterator().hasNext());

        // Remove
        spi.remove(typeAA.space(), entity(2));

        assertEquals(1, spi.size(typeAA.space(), typeAA));
        assertEquals(2, spi.size(typeAB.space(), typeAB));
        assertEquals(1, spi.size(typeBA.space(), typeBA));

        spi.remove(typeBA.space(), entity(1));

        assertEquals(1, spi.size(typeAA.space(), typeAA));
        assertEquals(2, spi.size(typeAB.space(), typeAB));
        assertEquals(0, spi.size(typeBA.space(), typeBA));

        boolean h2IdxOffheap = (spi instanceof GridH2IndexingSpi) &&
                ((GridH2IndexingSpiMBean)spi).getMaxOffHeapMemory() > 0;

        // At the time of this writing index rebuilding is not supported for GridH2IndexingSpi with off-heap storage.
        if (!h2IdxOffheap) {
            // Rebuild

            spi.rebuildIndexes(typeAB.space(), typeAB);

            assertEquals(1, spi.size(typeAA.space(), typeAA));
            assertEquals(2, spi.size(typeAB.space(), typeAB));
            assertEquals(0, spi.size(typeBA.space(), typeBA));

            // For invalid space name/type should not fail.
            spi.rebuildIndexes("not_existing_space", typeAA);
            spi.rebuildIndexes(typeAA.space(), new TypeDesc("C", "C", fieldsAA, null));
        }

        // Unregister.
        spi.unregisterType(typeAA.space(), typeAA);

        assertEquals(-1, spi.size(typeAA.space(), typeAA));
        assertEquals(2, spi.size(typeAB.space(), typeAB));
        assertEquals(0, spi.size(typeBA.space(), typeBA));

        spi.unregisterType(typeAB.space(), typeAB);

        assertEquals(-1, spi.size(typeAA.space(), typeAA));
        assertEquals(-1, spi.size(typeAB.space(), typeAB));
        assertEquals(0, spi.size(typeBA.space(), typeBA));

        spi.unregisterType(typeBA.space(), typeBA);

        // Should not store but should not fail as well.
        spi.store(typeAA.space(), typeAA, entity(10), entity(aa(1, "Fail", 100500)), "v220".getBytes(), 0);

        assertEquals(-1, spi.size(typeAA.space(), typeAA));
    }

    /**
     * Test long queries write explain warnings into log.
     *
     * @throws Exception If failed.
     */
    public void testLongQueries() throws Exception {
        X spi = getSpi();

        if (!(spi instanceof GridH2IndexingSpi))
            return;

        long longQryExecTime = 100;

        GridStringLogger log = new GridStringLogger(false, this.log);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(spi, "log");
        ((GridH2IndexingSpi)spi).setLongQueryExecutionTimeout(longQryExecTime);
        ((GridH2IndexingSpi) spi).setLongQueryExplain(true);

        try {
            GridTestUtils.setFieldValue(spi, "log", log);

            String sql = "select sum(x) FROM SYSTEM_RANGE(?, ?)";

            long now = U.currentTimeMillis();
            long time = now;

            long range = 1000000L;

            while (now - time <= longQryExecTime * 3 / 2) {
                time = now;
                range *= 3;

                GridIndexingFieldsResult res = spi.queryFields(null, sql, Arrays.<Object>asList(1, range), null);

                assert res.iterator().hasNext();

                now = U.currentTimeMillis();
            }

            String res = log.toString();

            F.println(res);

            assert res.contains("/* PUBLIC.RANGE_INDEX */");
        }
        finally {
            GridTestUtils.setFieldValue(spi, "log", oldLog);
            ((GridH2IndexingSpi)spi).setLongQueryExecutionTimeout(3000);
        }
    }

    public void _testResultReuse() throws Exception {
        final X spi = getSpi();

        multithreaded(new Callable<Object>() {
              @Override public Object call() throws Exception {
                  return spi.queryFields(null, "SELECT sum(x) + sum(x) + sum(x) + sum(x) FROM SYSTEM_RANGE(?, ?)",
                      F.<Object>asList(0, 7000000), null);
              }
          }, 5);
    }

    /**
     * Test long queries write explain warnings into log.
     *
     * @throws Exception If failed.
     */
    public void testZeroLongQuery() throws Exception {
        X spi = getSpi();

        if (!(spi instanceof GridH2IndexingSpi))
            return;

        long longQryExecTime = -1;

        GridStringLogger log = new GridStringLogger(false, this.log);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(spi, "log");
        ((GridH2IndexingSpi)spi).setLongQueryExecutionTimeout(longQryExecTime);
        ((GridH2IndexingSpi) spi).setLongQueryExplain(true);

        try {
            GridTestUtils.setFieldValue(spi, "log", log);

            String sql = "SELECT * FROM MyNonExistingTable";

            GridIndexingFieldsResult res = spi.queryFields(null, sql, Collections.emptyList(), null);

            assertFalse(res.iterator().hasNext());

            String logStr = log.toString();

            F.println(logStr);

            assertTrue(logStr.contains("Failed to explain plan because required table does not exist"));
        }
        finally {
            GridTestUtils.setFieldValue(spi, "log", oldLog);
            ((GridH2IndexingSpi)spi).setLongQueryExecutionTimeout(3000);
        }
    }

    /**
     * Index descriptor.
     */
    private static class TextIndex implements GridIndexDescriptor {
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
        @Override public GridIndexType type() {
            return GridIndexType.FULLTEXT;
        }
    }

    /**
     * Type descriptor.
     */
    private static class TypeDesc implements GridIndexingTypeDescriptor {
        /** */
        private final String name;

        /** */
        private final String space;

        /** */
        private final Map<String, Class<?>> valFields;

        /** */
        private final GridIndexDescriptor textIdx;

        /**
         * @param space Space name.
         * @param name Type name.
         * @param valFields Fields.
         * @param textIdx Fulltext index.
         */
        private TypeDesc(String space, String name, Map<String, Class<?>> valFields, GridIndexDescriptor textIdx) {
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
        @Override public Map<String, Class<?>> valueFields() {
            return valFields;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> keyFields() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public <T> T value(Object obj, String field) throws IgniteSpiException {
            assert obj != null;
            assert !F.isEmpty(field);

            return (T)((Map<String, Object>) obj).get(field);
        }

        /** */
        @Override public Map<String, GridIndexDescriptor> indexes() {
            return textIdx == null ? Collections.<String, GridIndexDescriptor>emptyMap() :
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

    /**
     * Indexing marshaller.
     */
    private static class IdxMarshaller implements GridIndexingMarshaller {
        /** */
        private final IgniteMarshaller marshaller;

        /**
         * @param marshaller Marshaller.
         */
        private IdxMarshaller(IgniteMarshaller marshaller) {
            this.marshaller = marshaller;
        }

        /** {@inheritDoc} */
        @Override public <T> GridIndexingEntity<T> unmarshal(byte[] bytes) throws IgniteSpiException {
            try {
                return new GridIndexingEntityAdapter<>(
                    (T)marshaller.unmarshal(bytes, getClass().getClassLoader()), bytes);
            }
            catch (GridException e) {
                throw new IgniteSpiException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public byte[] marshal(GridIndexingEntity<?> entity) throws IgniteSpiException {
            if (entity.bytes() != null)
                return entity.bytes();

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try {
                marshaller.marshal(entity.value(), out);
            }
            catch (GridException e) {
                throw new IgniteSpiException(e);
            }

            return out.toByteArray();
        }
    }

}
