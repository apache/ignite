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

import java.io.*;
import java.util.*;

/**
 * Tests TCP protocol.
 */
public class GridClientTcpPortableSelfTest extends GridClientTcpSelfTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridClientConnectionConfiguration ccfg = new GridClientConnectionConfiguration();

        ccfg.setPortableTypesMap(typesMap());

        cfg.setClientConnectionConfiguration(ccfg);

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
    private Map<Integer, Class<? extends GridPortableObject>> typesMap() {
        Map<Integer, Class<? extends GridPortableObject>> map = new HashMap<>();

        map.put(TestKey1.TYPE_ID, TestKey1.class);
        map.put(TestKey2.TYPE_ID, TestKey2.class);
        map.put(TestValue1.TYPE_ID, TestValue1.class);
        map.put(TestValue2.TYPE_ID, TestValue2.class);

        return map;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPortable() throws Exception {
        GridClientData dfltData = client.data();

        TestKey1 key1 = new TestKey1(true, (byte)10);

        TestValue1 val1 = new TestValue1("val1");

        dfltData.put(key1, val1);

        key1 = new TestKey1(false, (byte)10);

        assertEquals(null, dfltData.get(key1));
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestKey1 implements GridPortableObject {
        /** */
        static final int TYPE_ID = 0;

        /** */
        private boolean a;

        /** */
        private byte b;

        /**
         *
         */
        public TestKey1() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         */
        public TestKey1(boolean a, byte b) {
            this.a = a;
            this.b = b;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeBoolean("a", a);
            writer.writeByte("b", b);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readBoolean("a");
            b = reader.readByte("b");
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey1 testKey1 = (TestKey1) o;

            return a == testKey1.a && b == testKey1.b;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = (a ? 1 : 0);

            result = 31 * result + (int)b;

            return result;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestKey2 extends TestKey1 {
        /** */
        static final int TYPE_ID = 1;

        /**
         *
         */
        public TestKey2() {
            // No-op.
        }

        /**
         * @param a Value1.
         * @param b Value2.
         */
        public TestKey2(boolean a, byte b) {
            super(a, b);
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            super.writePortable(writer);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            super.readPortable(reader);
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestValue1 implements GridPortableObject {
        /** */
        static final int TYPE_ID = 2;

        /** */
        private String a;

        /**
         *
         */
        public TestValue1() {
            // No-op.
        }

        /**
         * @param a Value1.
         */
        public TestValue1(String a) {
            this.a = a;
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeString("a", a);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readString("a");
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue1 that = (TestValue1) o;

            return !(a != null ? !a.equals(that.a) : that.a != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return a != null ? a.hashCode() : 0;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestValue2 extends TestValue1 {
        /** */
        static final int TYPE_ID = 3;

        /**
         *
         */
        public TestValue2() {
            // No-op.
        }

        public TestValue2(String a) {
            super(a);
        }

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE_ID;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            super.writePortable(writer);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            super.readPortable(reader);
        }
    }
}
