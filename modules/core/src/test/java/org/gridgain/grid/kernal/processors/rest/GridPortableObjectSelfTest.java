/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.portable.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Tests portable objects.
 */
public class GridPortableObjectSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int TYPE1 = 1;

    /** */
    private static final int TYPE2 = 2;

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Type1 implements GridPortableObject {
        /** */
        private int a;

        /** */
        private String b;

        /** */
        private Object c;

        /** */
        private Map<Object, Object> d;

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE1;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeInt("a", a);
            writer.writeString("b", b);
            writer.writeObject("c", c);
            writer.writeMap("d", d);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readInt("a");
            b = reader.readString("b");
            c = reader.readObject("c");
            d = reader.readMap("d");
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Type2 implements GridPortableObject {
        /** */
        private byte a;

        /** */
        private long b;

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE2;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeByte("a", a);
            writer.writeLong("b", b);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readByte("a");
            b = reader.readLong("b");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        Type1 t1 = new Type1();

        t1.a = 1;
        t1.b = "str";
        t1.d = new HashMap<>();
        t1.d.put(1, 1);
        t1.d.put("a", "b");

        Type2 t2 = new Type2();

        t2.a = 1;
        t2.b = Long.MAX_VALUE;

        t1.c = t2;
        t1.d.put(10, t2);

        Map<Integer, Class<? extends GridPortableObject>> typesMap = new HashMap<>();

        typesMap.put(TYPE1, t1.getClass());
        typesMap.put(TYPE2, t2.getClass());

        GridClientPortableMarshaller marshaller = new GridClientPortableMarshaller(typesMap);

        byte[] bytes = marshaller.marshal(t1);

        Type1 t1Read = marshaller.unmarshal(bytes);

        assertEquals(t1.a, t1Read.a);
        assertEquals(t1.b, t1Read.b);
        assertNotNull(t1Read.c);
        assertNotNull(t1Read.d);

        assertEquals(3, t1Read.d.size());
        assertEquals(1, t1Read.d.get(1));
        assertEquals("b", t1Read.d.get("a"));

        Type2 t2Read = (Type2)t1Read.c;

        assertEquals(t2.a, t2Read.a);
        assertEquals(t2.b, t2Read.b);

        Type2 t2ReadMap = (Type2)t1Read.d.get(10);

        assertEquals(t2.a, t2ReadMap.a);
        assertEquals(t2.b, t2ReadMap.b);
    }
}
