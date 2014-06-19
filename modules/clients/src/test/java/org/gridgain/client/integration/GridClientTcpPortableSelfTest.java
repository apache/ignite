/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.portable.*;
import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Tests TCP protocol.
 */
public class GridClientTcpPortableSelfTest extends GridClientTcpSelfTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getClientConnectionConfiguration() != null;

        cfg.getClientConnectionConfiguration().setPortableTypesMap(typesMap());

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setMarshaller(new GridClientPortableMarshaller(typesMap()));

        return cfg;
    }

    /**
     * @return Portable types map.
     */
    private Map<Integer, Class<? extends GridPortable>> typesMap() {
        Map<Integer, Class<? extends GridPortable>> map = new HashMap<>();

        map.put(TestKey1.TYPE_ID, TestKey1.class);
        map.put(TestKey2.TYPE_ID, TestKey2.class);
        map.put(TestValue1.TYPE_ID, TestValue1.class);
        map.put(TestValue2.TYPE_ID, TestValue2.class);
        map.put(TestPortable.TYPE_ID, TestPortable.class);

        return map;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public void testPutPortable() throws Exception {
        GridClientData dfltData = client.data();

        TestKey1 key1 = new TestKey1(false, (byte)10, (short)11, 'a', 100, Long.MAX_VALUE, 1.1f, 10.1, "str1");

        TestValue1 val1 = new TestValue1(new boolean[]{true, false}, new byte[]{0, -1, 2, Byte.MAX_VALUE},
            new short[]{3, -4, 5, Short.MAX_VALUE}, new char[]{'a', 'b', 'c'}, new int[]{Integer.MAX_VALUE, -1},
            new long[]{Long.MAX_VALUE, -1, 1}, new float[]{1.1f, -1.1f}, new double[]{1.1f, -1.1f});

        dfltData.put(key1, val1);

        assertEquals(val1, dfltData.get(key1));

        key1 = new TestKey1(false, (byte)10, (short)11, 'a', 100, Long.MAX_VALUE, 1.1f, 10.1, "str2");

        assertEquals(null, dfltData.get(key1));

        TestKey2 key2 = new TestKey2(false, (byte)10, (short)11, 'a', 100, Long.MAX_VALUE, 1.1f, 10.1, "str1", null);

        dfltData.put(key2, val1);

        assertEquals(val1, dfltData.get(key2));

        key2 = new TestKey2(false, (byte)10, (short)11, 'a', 100, Long.MAX_VALUE, 1.1f, 10.1, "str1", 1.1);

        assertNull(dfltData.get(key2));

        key2 = new TestKey2(false, (byte)10, (short)11, 'a', 100, Long.MAX_VALUE, 1.1f, 10.1, "str1",
            UUID.randomUUID());

        val1 = new TestValue1(null, null, null, null, null, null, null, null);

        dfltData.put(key2, val1);

        assertEquals(val1, dfltData.get(key2));

        val1 = new TestValue1(new boolean[]{}, new byte[]{}, new short[]{}, new char[]{}, new int[]{}, new long[]{},
            new float[]{}, new double[]{});

        dfltData.put(key2, val1);

        assertEquals(val1, dfltData.get(key2));

        TestPortable p1 = new TestPortable(1, null, "a");
        TestPortable p2 = new TestPortable(2, new HashMap<String, String>(), "a");
        TestPortable p3 = new TestPortable(2, F.asMap("1", "11", "2", "22"), "a");

        Map<Object, Object> map = new HashMap<>();

        map.put(1, p1);
        map.put("2", p2);
        map.put(3L, p3);
        map.put("4", null);

        Collection<Object> col = new ArrayList<>();

        col.add(p1);
        col.add(1);
        col.add(p2);
        col.add(null);
        col.add(p3);

        TestValue2 val2 = new TestValue2(new boolean[]{true, false}, null,
            new short[]{3, -4, 5, Short.MAX_VALUE}, new char[]{'a', 'b', 'c'}, new int[]{Integer.MAX_VALUE, -1},
            new long[]{Long.MAX_VALUE, -1, 1}, new float[]{1.1f, -1.1f}, new double[]{1.1f, -1.1f},
            map, col);

        dfltData.put(key2, val2);

        assertEquals(val2, dfltData.get(key2));

        val2 = new TestValue2(new boolean[]{true, false}, null,
            new short[]{3, -4, 5, Short.MAX_VALUE}, new char[]{'a', 'b', 'c'}, new int[]{Integer.MAX_VALUE, -1},
            new long[]{Long.MAX_VALUE, -1, 1}, new float[]{1.1f, -1.1f}, new double[]{1.1f, -1.1f},
            null, null);

        dfltData.put(key2, val2);

        assertEquals(val2, dfltData.get(key2));
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestKey1 implements GridPortable {
        /** */
        static final int TYPE_ID = 0;

        /** */
        private boolean a;

        /** */
        private byte b;

        /** */
        private short c;

        /** */
        private char d;

        /** */
        private int e;

        /** */
        private long f;

        /** */
        private float g;

        /** */
        private double h;

        /** */
        private String i;

        /**
         *
         */
        public TestKey1() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         * @param c Value3.
         * @param d Value4.
         * @param e Value5.
         * @param f Value6.
         * @param g Value7.
         * @param h Value8.
         * @param i Value9.
         */
        public TestKey1(boolean a,
            byte b,
            short c,
            char d,
            int e,
            long f,
            float g,
            double h,
            String i) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
            this.e = e;
            this.f = f;
            this.g = g;
            this.h = h;
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeBoolean("a", a);
            writer.writeByte("b", b);
            writer.writeShort("c", c);
            writer.writeChar("d", d);
            writer.writeInt("e", e);
            writer.writeLong("f", f);
            writer.writeFloat("g", g);
            writer.writeDouble("h", h);
            writer.writeString("i", i);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readBoolean("a");
            b = reader.readByte("b");
            c = reader.readShort("c");
            d = reader.readChar("d");
            e = reader.readInt("e");
            f = reader.readLong("f");
            g = reader.readFloat("g");
            h = reader.readDouble("h");
            i = reader.readString("i");
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestKey1 testKey1 = (TestKey1) o;

            if (a != testKey1.a) return false;
            if (b != testKey1.b) return false;
            if (c != testKey1.c) return false;
            if (d != testKey1.d) return false;
            if (e != testKey1.e) return false;
            if (f != testKey1.f) return false;
            if (Float.compare(testKey1.g, g) != 0) return false;
            if (Double.compare(testKey1.h, h) != 0) return false;

            return !(i != null ? !i.equals(testKey1.i) : testKey1.i != null);

        }

        /** {@inheritDoc} */
        @SuppressWarnings({"UnaryPlus", "TooBroadScope"})
        @Override public int hashCode() {
            int res;
            long tmp;

            res = (a ? 1 : 0);
            res = 31 * res + (int) b;
            res = 31 * res + (int) c;
            res = 31 * res + (int) d;
            res = 31 * res + e;
            res = 31 * res + (int) (f ^ (f >>> 32));
            res = 31 * res + (g != +0.0f ? Float.floatToIntBits(g) : 0);
            tmp = Double.doubleToLongBits(h);
            res = 31 * res + (int) (tmp ^ (tmp >>> 32));
            res = 31 * res + (i != null ? i.hashCode() : 0);

            return res;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestKey2 extends TestKey1 {
        /** */
        static final int TYPE_ID = 1;

        /** */
        private Object obj;

        /**
         *
         */
        public TestKey2() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         * @param c Value3.
         * @param d Value4.
         * @param e Value5.
         * @param f Value6.
         * @param g Value7.
         * @param h Value8.
         * @param i Value9.
         * @param obj Value10.
         */
        public TestKey2(boolean a,
            byte b,
            short c,
            char d,
            int e,
            long f,
            float g,
            double h,
            String i,
            @Nullable Object obj) {
            super(a, b, c, d, e, f, g, h, i);

            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            super.writePortable(writer);

            writer.writeObject("obj", obj);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            super.readPortable(reader);

            obj = reader.readObject("obj");
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            TestKey2 testKey2 = (TestKey2)o;

            return !(obj != null ? !obj.equals(testKey2.obj) : testKey2.obj != null);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = super.hashCode();

            res = 31 * res + (obj != null ? obj.hashCode() : 0);

            return res;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestValue1 implements GridPortable {
        /** */
        static final int TYPE_ID = 2;

        /** */
        private boolean[] a;

        /** */
        private byte[] b;

        /** */
        private short[] c;

        /** */
        private char[] d;

        /** */
        private int[] e;

        /** */
        private long[] f;

        /** */
        private float[] g;

        /** */
        private double[] h;

        /**
         *
         */
        public TestValue1() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         * @param c Value3.
         * @param d Value4.
         * @param e Value5.
         * @param f Value6.
         * @param g Value7.
         * @param h Value8.
         */
        public TestValue1(
            @Nullable boolean[] a,
            @Nullable byte[] b,
            @Nullable short[] c,
            @Nullable char[] d,
            @Nullable int[] e,
            @Nullable long[] f,
            @Nullable float[] g,
            @Nullable double[] h) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
            this.e = e;
            this.f = f;
            this.g = g;
            this.h = h;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeBooleanArray("a", a);
            writer.writeByteArray("b", b);
            writer.writeShortArray("c", c);
            writer.writeCharArray("d", d);
            writer.writeIntArray("e", e);
            writer.writeLongArray("f", f);
            writer.writeFloatArray("g", g);
            writer.writeDoubleArray("h", h);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readBooleanArray("a");
            b = reader.readByteArray("b");
            c = reader.readShortArray("c");
            d = reader.readCharArray("d");
            e = reader.readIntArray("e");
            f = reader.readLongArray("f");
            g = reader.readFloatArray("g");
            h = reader.readDoubleArray("h");
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"SimplifiableIfStatement", "RedundantIfStatement"})
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestValue1 that = (TestValue1) o;

            if (!Arrays.equals(a, that.a)) return false;
            if (!Arrays.equals(b, that.b)) return false;
            if (!Arrays.equals(c, that.c)) return false;
            if (!Arrays.equals(d, that.d)) return false;
            if (!Arrays.equals(e, that.e)) return false;
            if (!Arrays.equals(f, that.f)) return false;
            if (!Arrays.equals(g, that.g)) return false;
            if (!Arrays.equals(h, that.h)) return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = a != null ? Arrays.hashCode(a) : 0;

            res = 31 * res + (b != null ? Arrays.hashCode(b) : 0);
            res = 31 * res + (c != null ? Arrays.hashCode(c) : 0);
            res = 31 * res + (d != null ? Arrays.hashCode(d) : 0);
            res = 31 * res + (e != null ? Arrays.hashCode(e) : 0);
            res = 31 * res + (f != null ? Arrays.hashCode(f) : 0);
            res = 31 * res + (g != null ? Arrays.hashCode(g) : 0);
            res = 31 * res + (h != null ? Arrays.hashCode(h) : 0);

            return res;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestValue2 extends TestValue1 {
        /** */
        static final int TYPE_ID = 3;

        /** */
        private Map<Object, Object> a;

        /** */
        private Collection<Object> b;

        /**
         *
         */
        public TestValue2() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         * @param c Value3.
         * @param d Value4.
         * @param e Value5.
         * @param f Value6.
         * @param g Value7.
         * @param h Value8.
         * @param aMap Value9.
         * @param bCol Value10.
         */
        public TestValue2(
            @Nullable boolean[] a,
            @Nullable byte[] b,
            @Nullable short[] c,
            @Nullable char[] d,
            @Nullable int[] e,
            @Nullable long[] f,
            @Nullable float[] g,
            @Nullable double[] h,
            @Nullable Map<Object, Object> aMap,
            @Nullable Collection<Object> bCol) {
            super(a, b, c, d, e, f, g, h);

            this.a = aMap;
            this.b = bCol;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            super.writePortable(writer);

            writer.writeMap("aMap", a);
            writer.writeCollection("bCol", b);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            super.readPortable(reader);

            a = reader.readMap("aMap");
            b = reader.readCollection("aCol");
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestPortable implements GridPortable {
        /** */
        static final int TYPE_ID = 4;

        /** */
        private int a;

        /** */
        private Map<String, String> b;

        /** */
        private String c;

        /**
         *
         */
        public TestPortable() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         * @param c Value3.
         */
        public TestPortable(int a, @Nullable Map<String, String> b, String c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeInt("a", a);
            writer.writeMap("b", b);
            writer.writeString("c", c);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readInt("a");
            b = reader.readMap("b");
            c = reader.readString("c");
        }

        /** {@inheritDoc} */
        @SuppressWarnings("RedundantIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestPortable that = (TestPortable) o;

            if (a != that.a) return false;
            if (b != null ? !b.equals(that.b) : that.b != null) return false;
            if (c != null ? !c.equals(that.c) : that.c != null) return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = a;

            res = 31 * res + (b != null ? b.hashCode() : 0);
            res = 31 * res + (c != null ? c.hashCode() : 0);

            return res;
        }
    }
}
