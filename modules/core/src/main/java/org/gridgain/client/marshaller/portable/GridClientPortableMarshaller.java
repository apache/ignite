/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.portable;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Marshaller supporting {@link GridPortableObject}.
 */
public class GridClientPortableMarshaller implements GridClientMarshaller {
    /** */
    public static final byte TYPE_NULL = 0;

    /** */
    public static final byte TYPE_BYTE = 1;

    /** */
    public static final byte TYPE_SHORT = 2;

    /** */
    public static final byte TYPE_INT = 3;

    /** */
    public static final byte TYPE_LONG = 4;

    /** */
    public static final byte TYPE_FLOAT = 5;

    /** */
    public static final byte TYPE_DOUBLE = 6;

    /** */
    public static final byte TYPE_BOOLEAN = 7;

    /** */
    public static final byte TYPE_CHAR = 8;

    /** */
    public static final byte TYPE_STRING = 9;

    /** */
    public static final byte TYPE_BYTE_ARRAY = 10;

    /** */
    public static final byte TYPE_SHORT_ARRAY = 11;

    /** */
    public static final byte TYPE_INT_ARRAY = 12;

    /** */
    public static final byte TYPE_LONG_ARRAY = 13;

    /** */
    public static final byte TYPE_FLOAT_ARRAY = 14;

    /** */
    public static final byte TYPE_DOUBLE_ARRAY = 15;

    /** */
    public static final byte TYPE_BOOLEAN_ARRAY = 16;

    /** */
    public static final byte TYPE_CHAR_ARRAY = 17;

    /** */
    public static final byte TYPE_LIST = 18;

    /** */
    public static final byte TYPE_MAP = 19;

    /** */
    public static final byte TYPE_USER_OBJECT = 20;

    /** */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private final Map<Integer, Class<? extends GridPortableObject>> typesMap;

    /** */
    private final ConcurrentMap<Integer, GridPortableClassMetadata> metadataMap = new ConcurrentHashMap8<>();

    /**
     * @param typesMap Map associating portable type identifiers with java classes..
     */
    public GridClientPortableMarshaller(Map<Integer, Class<? extends GridPortableObject>> typesMap) {
        this.typesMap = typesMap;
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object obj) throws IOException {
        if (!(obj instanceof GridPortableObject))
            throw new IllegalArgumentException();

        GridPortableObject portable = (GridPortableObject)obj;

        GridPortableClassMetadata metadata = metadataMap.get(portable.typeId());

        if (metadata == null) {
            Class<? extends GridPortableObject> cls = typesMap.get(portable.typeId());

            if (cls == null) // TODO 8491.
                throw new IllegalArgumentException("No Java class for portable type " + portable.typeId());

            MetadataCollectingWriter writer = new MetadataCollectingWriter();

            portable.writePortable(writer);

            metadata = new GridPortableClassMetadata(portable.typeId(), cls, writer.fields());

            metadataMap.put(portable.typeId(), metadata);
        }

        Writer writer = new Writer();

        writer.writePortable(portable);

        return writer.end();
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        Reader reader = new Reader(bytes);

        return (T)reader.readPortable();
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolId() {
        return U.PORTABLE_OBJECT_PROTO_ID;
    }

    /**
     *
     */
    private static class ByteArrayOutputStream {
        /** */
        private byte[] bytes;

        /** */
        private int cnt;

        /**
         * @param capacity Initial byte array size.
         */
        ByteArrayOutputStream(int capacity) {
            bytes = new byte[capacity];
        }

        /**
         * @param val Value to write.
         */
        void writeByte(byte val) {
            ensureCapacity(1);

            bytes[cnt++] = val;
        }

        /**
         * @param val Value to write.
         */
        void writeBoolean(boolean val) {
            ensureCapacity(1);

            bytes[cnt++] = val ? (byte)1 : (byte)0;
        }

        /**
         * @param val Value to write.
         */
        void writeInt(int val) {
            ensureCapacity(4);

            UNSAFE.putInt(bytes, OFFSET + cnt, val);

            cnt += 4;
        }

        /**
         * @param val Value to write.
         */
        void writeLong(long val) {
            ensureCapacity(8);

            UNSAFE.putLong(bytes, OFFSET + cnt, val);

            cnt += 8;
        }

        /**
         * @param bytes Bytes to write.
         */
        void writeBytes(byte[] bytes) {
            ensureCapacity(bytes.length);

            System.arraycopy(bytes, 0, this.bytes, cnt, bytes.length);

            cnt += bytes.length;
        }

        /** {@inheritDoc} */
        void ensureCapacity(int size) {
            if (cnt + size > bytes.length)
                bytes = Arrays.copyOf(bytes, bytes.length * 2 + size);
        }

        /**
         * @return Bytes.
         */
        byte[] bytes() {
            return cnt == bytes.length ? bytes : Arrays.copyOf(bytes, cnt);
        }
    }

    /**
     *
     */
    private static class ByteArrayInputStream {
        /** */
        private byte[] bytes;

        /** */
        private int pos;

        /**
         * @param bytes Bytes.
         */
        ByteArrayInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        /**
         * @return Byte value.
         */
        byte readByte() {
            return bytes[pos++];
        }

        /**
         * @return Boolean value.
         */
        boolean readBoolean() {
            int val = bytes[pos++];

            return val != 0;
        }

        /**
         * @return Integer value.
         */
        int readInt() {
            int res = UNSAFE.getInt(bytes, OFFSET + pos);

            pos += 4;

            return res;
        }

        /**
         * @return Long value.
         */
        long readLong() {
            long res = UNSAFE.getLong(bytes, OFFSET + pos);

            pos += 8;

            return res;
        }

        /**
         * @param cnt Number of bytes to skip.
         */
        void skip(int cnt) {
            pos += cnt;
        }

        /**
         * @return Bytes.
         */
        byte[] bytes() {
            return bytes;
        }

        /**
         * @return Position.
         */
        int position() {
            return pos;
        }
    }

    /**
     *
     */
    private static class MetadataCollectingWriter implements GridPortableWriter {
        /** */
        private List<String> fields = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void writeByte(String fieldName, byte val) {
            onWrite(fieldName);
        }

        /** {@inheritDoc} */
        @Override public void writeInt(String fieldName, int val) {
            onWrite(fieldName);
        }

        /** {@inheritDoc} */
        @Override public void writeLong(String fieldName, long val) {
            onWrite(fieldName);
        }

        /** {@inheritDoc} */
        @Override public void writeString(String fieldName, String val) {
            onWrite(fieldName);
        }

        /** {@inheritDoc} */
        @Override public <T> void writeObject(String fieldName, T obj) {
            onWrite(fieldName);
        }

        /** {@inheritDoc} */
        @Override public <K, V> void writeMap(String fieldName, Map<K, V> map) {
            onWrite(fieldName);
        }

        /**
         * @param fieldName Field name.
         */
        private void onWrite(String fieldName) {
            fields.add(fieldName);
        }

        /**
         * @return Field count.
         */
        List<String> fields() {
            return fields;
        }
    }

    /**
     *
     */
    private static class Writer implements GridPortableWriter {
        /** */
        private final ByteArrayOutputStream out;

        /**
         *
         */
        Writer() {
            out = new ByteArrayOutputStream(1024);
        }

        /**
         * @param portable Portable object.
         * @throws IOException In case of error.
         */
        private void writePortable(GridPortableObject portable) throws IOException {
            out.writeInt(portable.typeId());

            portable.writePortable(this);
        }

        /**
         * Ends object write.
         * @return Marshaled object bytes.
         */
        byte[] end() {
            return out.bytes();
        }

        /** {@inheritDoc} */
        @Override public void writeByte(String fieldName, byte val) {
            startField(fieldName);

            out.writeByte(val);
        }

        /** {@inheritDoc} */
        @Override public void writeInt(String fieldName, int val) {
            startField(fieldName);

            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void writeLong(String fieldName, long val) {
            startField(fieldName);

            out.writeLong(val);
        }

        /** {@inheritDoc} */
        @Override public void writeString(String fieldName, String val) {
            startField(fieldName);

            writeStringValue(val);
        }

        /**
         * @param val String.
         */
        private void writeStringValue(@Nullable String val) {
            if (val != null) {
                byte[] bytes = val.getBytes(UTF_8);

                out.writeInt(bytes.length);
                out.writeBytes(bytes);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public <K, V> void writeMap(String fieldName, Map<K, V> map) throws IOException {
            startField(fieldName);

            if (map != null) {
                out.writeInt(map.size());

                for (Map.Entry<K, V> e : map.entrySet()) {
                    writeObjectValue(e.getKey());
                    writeObjectValue(e.getValue());
                }
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public <T> void writeObject(String fieldName, T obj) throws IOException {
            startField(fieldName);

            writeObjectValue(obj);
        }

        /**
         * @param obj Object to write.
         * @throws IOException In case of error.
         */
        private <T> void writeObjectValue(T obj) throws IOException {
            if (obj instanceof Byte) {
                out.writeByte(TYPE_BYTE);
                out.writeByte((Byte)obj);
            }
            else if (obj instanceof Integer) {
                out.writeByte(TYPE_INT);
                out.writeInt((Integer)obj);
            }
            else if (obj instanceof Long) {
                out.writeByte(TYPE_LONG);
                out.writeLong((Long)obj);
            }
            else if (obj instanceof String) {
                out.writeByte(TYPE_STRING);

                writeStringValue((String)obj);
            }
            else if (obj instanceof GridPortableObject) {
                out.writeByte(TYPE_USER_OBJECT);

                writePortable((GridPortableObject)obj);
            }
            else if (obj == null)
                out.writeInt(TYPE_NULL);
            else
                throw new IOException("Unsupported object: " + obj);
        }

        /**
         * @param fieldName Field name.
         */
        private void startField(String fieldName) {
            // No-op.
        }
    }

    /**
     *
     */
    private class Reader implements GridPortableReader {
        /** */
        private final ByteArrayInputStream in;

        /**
         * @param bytes Bytes.
         */
        Reader(byte[] bytes) {
            in = new ByteArrayInputStream(bytes);
        }

        /**
         * @return Portable object.
         * @throws IOException In case of error.
         */
        private GridPortableObject readPortable() throws IOException {
            int typeId = in.readInt();

            Class<? extends GridPortableObject> cls = typesMap.get(typeId);

            if (cls == null)
                throw new IOException("Unknown portable typeId: " + typeId);

            GridPortableObject portable;

            try {
                portable = cls.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e) {
                throw new IOException("Failed to instantiate portable object: " + cls, e);
            }

            portable.readPortable(this);

            return portable;
        }

        /** {@inheritDoc} */
        @Override public byte readByte(String fieldName) {
            return in.readByte();
        }

        /** {@inheritDoc} */
        @Override public int readInt(String fieldName) {
            return in.readInt();
        }

        /** {@inheritDoc} */
        @Override public long readLong(String fieldName) {
            return in.readLong();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ConstantConditions")
        @Override public String readString(String fieldName) {
            return readStringValue();
        }

        /**
         * @return String.
         */
        @Nullable private String readStringValue() {
            int len = in.readInt();

            if (len == -1)
                return null;

            String res = new String(in.bytes(), in.position(), len, UTF_8);

            in.skip(len);

            return res;
        }

        /** {@inheritDoc} */
        @Override public <T> T readObject(String fieldName) throws IOException {
            return readObjectValue();
        }

        /**
         * @return Object.
         * @throws IOException In case of error.
         */
        @Nullable private <T> T readObjectValue() throws IOException {
            byte type = in.readByte();

            Object res;

            switch (type) {
                case TYPE_NULL:
                    res = null;

                    break;

                case TYPE_BYTE:
                    res = in.readByte();

                    break;

                case TYPE_INT:
                    res = in.readInt();

                    break;

                case TYPE_LONG:
                    res = in.readLong();

                    break;

                case TYPE_STRING:
                    res = readStringValue();

                    break;

                case TYPE_USER_OBJECT:
                    res = readPortable();

                    break;

                default:
                    throw new IOException("Invalid type: " + type);
            }

            return (T)res;
        }

        /** {@inheritDoc} */
        @Override public <K, V> Map<K, V> readMap(String fieldName) throws IOException {
            int size = in.readInt();

            if (size == -1)
                return null;

            Map<K, V> map = new HashMap<>(size, 1.0f);

            for (int i = 0; i < size; i++) {
                K key = readObjectValue();
                V val = readObjectValue();

                map.put(key, val);
            }

            return map;
        }
    }
}
