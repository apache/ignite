/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.portable;

import org.gridgain.client.*;
import org.gridgain.grid.util.typedef.*;
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

    /** */
    private static final int TYPE3 = 3;

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Type1 implements GridPortableObject {
        /** */
        private int a;

        /** */
        private Map<Object, Object> b;

        /** */
        private Object c;

        /** */
        private String d;

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE1;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeInt("a", a);
            writer.writeMap("b", b);
            writer.writeObject("c", c);
            writer.writeString("d", d);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readInt("a");
            b = reader.readMap("b");
            c = reader.readObject("c");
            d = reader.readString("d");
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

        /** */
        private Type3 c;

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE2;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeByte("a", a);
            writer.writeLong("b", b);
            writer.writeObject("c", c);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readByte("a");
            b = reader.readLong("b");
            c = reader.readObject("c");
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Type3 implements GridPortableObject {
        /** */
        private Type2 a;

        /** {@inheritDoc} */
        @Override public int typeId() {
            return TYPE3;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(GridPortableWriter writer) throws IOException {
            writer.writeObject("a", a);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(GridPortableReader reader) throws IOException {
            a = reader.readObject("a");
        }
    }

    /**
     * @return Types map.
     */
    private Map<Integer, Class<? extends GridPortableObject>> typesMap() {
        Map<Integer, Class<? extends GridPortableObject>> typesMap = new HashMap<>();

        typesMap.put(TYPE1, Type1.class);
        typesMap.put(TYPE2, Type2.class);
        typesMap.put(TYPE3, Type3.class);

        return typesMap;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        Type1 t1 = new Type1();

        t1.a = 1;

        t1.b = new HashMap<>();
        t1.b.put(1, 1);
        t1.b.put("a", "b");

        t1.d = "str";

        Type2 t2 = new Type2();

        t2.a = 1;
        t2.b = Long.MAX_VALUE;

        t1.c = t2;
        t1.b.put(10, t2);

        GridClientPortableMarshaller marshaller = new GridClientPortableMarshaller(typesMap());

        byte[] bytes = marshaller.marshal(t1);

        Type1 t1Read = marshaller.unmarshal(bytes);

        assertEquals(t1.a, t1Read.a);
        assertEquals(t1.d, t1Read.d);
        assertNotNull(t1Read.c);
        assertNotNull(t1Read.d);

        assertEquals(3, t1Read.b.size());
        assertEquals(1, t1Read.b.get(1));
        assertEquals("b", t1Read.b.get("a"));

        Type2 t2Read = (Type2)t1Read.c;

        assertEquals(t2.a, t2Read.a);
        assertEquals(t2.b, t2Read.b);

        Type2 t2ReadMap = (Type2)t1Read.b.get(10);

        assertEquals(t2.a, t2ReadMap.a);
        assertEquals(t2.b, t2ReadMap.b);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReferences() throws Exception {
        Type2 t2 = new Type2();
        Type3 t3 = new Type3();

        t2.c = t3;
        t3.a = t2;

        GridClientPortableMarshaller marshaller = new GridClientPortableMarshaller(typesMap());

        byte[] bytes = marshaller.marshal(t2);

        Type2 t2Read = marshaller.unmarshal(bytes);

        assertSame(t2Read, t2Read.c.a);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsCollection() throws Exception {
        Type1 t1 = new Type1();

        t1.c = new Type2();

        GridPortableMetadataCollectingWriter writer = new GridPortableMetadataCollectingWriter();

        Map<Integer, List<String>> fields = writer.writeAndCollect(t1);

        assertEquals(2, fields.size());

        assertEquals(F.asList("a", "b", "c", "d"), fields.get(TYPE1));
        assertEquals(F.asList("a", "b"), fields.get(TYPE2));

        t1 = new Type1();

        t1.c = new Type1();

        fields = writer.writeAndCollect(t1);

        assertEquals(1, fields.size());

        assertEquals(F.asList("a", "b", "c", "d"), fields.get(TYPE1));
    }
}
