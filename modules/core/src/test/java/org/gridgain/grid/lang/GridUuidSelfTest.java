/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Tests for {@link GridUuid}.
 */
@GridCommonTest(group = "Lang")
public class GridUuidSelfTest extends GridCommonAbstractTest {
    /** Sample size. */
    private static final int NUM = 100000;

    /**
     * JUnit.
     */
    public void testToString() {
        GridUuid id1 = GridUuid.randomUuid();

        String id8_1 = U.id8(id1);

        info("ID1: " + id1);
        info("ID8_1: " + id8_1);

        String s = id1.toString();

        GridUuid id2 = GridUuid.fromString(s);

        String id8_2 = U.id8(id2);

        info("ID2: " + id2);
        info("ID8_2: " + id8_2);

        assertNotNull(id2);
        assertNotNull(id8_1);
        assertNotNull(id8_2);

        assertEquals(8, id8_1.length());
        assertEquals(8, id8_2.length());

        assertEquals(id1, id2);
        assertEquals(id8_1, id8_2);
    }

    /**
     * JUnit.
     */
    public void testGridUuid() {
        GridUuid id1 = GridUuid.randomUuid();
        GridUuid id2 = GridUuid.randomUuid();

        assert id1.compareTo(id2) == -1;
        assert id2.compareTo(id1) == 1;
        assert id1.compareTo(id1) == 0;
        assert id2.compareTo(id2) == 0;

        assert id1.hashCode() != id2.hashCode();
        assert !id1.equals(id2);

        assert id1.iterator().hasNext();
        assert id1.iterator().next().equals(id1);

        assert id2.iterator().hasNext();
        assert id2.iterator().next().equals(id2);
    }

    /**
     * JUnit.
     */
    public void testGridUuidPerformance() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < NUM; i++)
            UUID.randomUUID();

        long dur1 = System.currentTimeMillis() - start;

        start = System.currentTimeMillis();

        for (int i = 0; i < NUM; i++)
            GridUuid.randomUuid();

        long dur2 = System.currentTimeMillis() - start;

        System.out.println("Creation: UUID=" + dur1 + "ms, GridUuid=" + dur2 + "ms, " + (dur1 / dur2) + "x faster.");
    }

    /**
     * Tests serialization performance.
     *
     * @throws Exception If failed.
     */
    public void testSerializationPerformance() throws Exception {
        UuidBean[] uids = new UuidBean[NUM];

        // Populate.
        for (int i = 0; i < uids.length; i++)
            uids[i] = new UuidBean(UUID.randomUUID());

        GridUuidBean[] guids = new GridUuidBean[NUM];

        // Populate.
        for (int i = 0; i < guids.length; i++)
            guids[i] = new GridUuidBean(GridUuid.randomUuid());

        // Warm up.
        testArray(uids, NUM);
        testArray(guids, NUM);

        System.gc();

        long start = System.currentTimeMillis();

        testArray(uids, NUM);

        long dur1 = System.currentTimeMillis() - start;

        System.gc();

        start = System.currentTimeMillis();

        testArray(guids, NUM);

        long dur2 = System.currentTimeMillis() - start;

        long res = dur1 < dur2 ? (dur2 / dur1) : (dur1 / dur2);

        String metric = res == 1 ? "as fast as UUID" : dur1 < dur2 ? res + "x **SLOWER**" : res + "x faster";

        System.out.println("Serialization: UUID=" + dur1 + "ms, GridUuid=" + dur2 + "ms, GridUuid is " +
            metric + ".");
    }

    /**
     * @param objs Objects to test.
     * @param cnt Number of iterations.
     * @throws Exception If failed.
     */
    private void testArray(Object[] objs, int cnt) throws Exception {
        assert objs.length > 0;

        int size = 0;

        for (int i = 0; i < cnt; i++) {
            Object w = objs[i];

            byte[] bytes = write(w);

            size += bytes.length;

            Object r = read(bytes);

            assert r != null;
            assert r.equals(w);
        }

        info("Average byte size of serialized object [size=" + (size / cnt) +
            ", type=" + objs[0].getClass().getSimpleName() + ']');
    }

    /**
     * @param uid UUID.
     * @return Serialized bytes.
     * @throws IOException If failed.
     */
    private byte[] write(Object uid) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(256);

        ObjectOutputStream out = null;

        try {
            out = new ObjectOutputStream(bout);

            out.writeObject(uid);

            return bout.toByteArray();
        }
        finally {
            if (out != null)
                out.close();
        }
    }

    /**
     * @param bytes Serialized bytes.
     * @return Deserialized object.
     * @throws ClassNotFoundException If failed.
     * @throws IOException If failed.
     */
    @SuppressWarnings({"unchecked"})
    private <T> T read(byte[] bytes) throws ClassNotFoundException, IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);

        ObjectInputStream in = null;

        try {
            in = new ObjectInputStream(bin);

            return (T)in.readObject();
        }
        finally {
            if (in != null)
                in.close();
        }
    }

    /**
     *
     */
    private static class GridUuidBean implements Externalizable {
        /** */
        private GridUuid uid;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public GridUuidBean() {
            // No-op.
        }

        /**
         * @param uid Grid UUID.
         */
        private GridUuidBean(GridUuid uid) {
            this.uid = uid;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // Uncomment if you would like to see raw serialization performance.
            //out.writeObject(uid);

            U.writeGridUuid(out, uid);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // Uncomment if you would like to see raw serialization performance.
            //uid = (GridUuid)in.readObject();

            uid = U.readGridUuid(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || uid.equals(((GridUuidBean)o).uid);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return uid.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() { return S.toString(GridUuidBean.class, this); }
    }

    /**
     *
     */
    private static class UuidBean implements Externalizable {
        /** */
        private UUID uid;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public UuidBean() {
            // No-op.
        }

        /**
         * @param uid Grid UUID.
         */
        private UuidBean(UUID uid) {
            this.uid = uid;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, uid);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uid = U.readUuid(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || uid.equals(((UuidBean)o).uid);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return uid.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() { return S.toString(UuidBean.class, this); }
    }
}
