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
import org.gridgain.grid.kernal.processors.rest.client.message.*;
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
    public static final byte TYPE_UUID = 20;

    /** */
    public static final byte TYPE_USER_OBJECT = 21;

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
    public GridClientPortableMarshaller(@Nullable Map<Integer, Class<? extends GridPortableObject>> typesMap) {
        this.typesMap = new HashMap<>();

        if (typesMap != null)
            this.typesMap.putAll(typesMap);

        this.typesMap.put(GridClientAuthenticationRequest.PORTABLE_TYPE_ID, GridClientAuthenticationRequest.class);
        this.typesMap.put(GridClientCacheRequest.PORTABLE_TYPE_ID, GridClientCacheRequest.class);
        this.typesMap.put(GridClientLogRequest.PORTABLE_TYPE_ID, GridClientLogRequest.class);
        this.typesMap.put(GridClientNodeBean.PORTABLE_TYPE_ID, GridClientNodeBean.class);
        this.typesMap.put(GridClientNodeMetricsBean.PORTABLE_TYPE_ID, GridClientNodeMetricsBean.class);
        this.typesMap.put(GridClientResponse.PORTABLE_TYPE_ID, GridClientResponse.class);
        this.typesMap.put(GridClientTaskRequest.PORTABLE_TYPE_ID, GridClientTaskRequest.class);
        this.typesMap.put(GridClientTaskResultBean.PORTABLE_TYPE_ID, GridClientTaskResultBean.class);
        this.typesMap.put(GridClientTopologyRequest.PORTABLE_TYPE_ID, GridClientTopologyRequest.class);
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
                throw new IOException("No Java class for portable type " +
                    "[obj=" + obj + ", typeId=" + portable.typeId() + ']');

            GridPortableMetadataCollectingWriter writer = new GridPortableMetadataCollectingWriter();

            Map<Integer, List<String>> fieldsMap = writer.writeAndCollect(portable);

            List<String> fields = fieldsMap.get(portable.typeId());

            assert fields != null : "Failed to get fields for " + portable;

            metadata = new GridPortableClassMetadata(portable.typeId(), cls, fields);

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
         * @param val Value to write.
         */
        void writeFloat(float val) {
            ensureCapacity(4);

            UNSAFE.putFloat(bytes, OFFSET + cnt, val);

            cnt += 4;
        }

        /**
         * @param val Value to write.
         */
        void writeDouble(double val) {
            ensureCapacity(8);

            UNSAFE.putDouble(bytes, OFFSET + cnt, val);

            cnt += 8;
        }

        /**
         * @param val Value to write.
         */
        void writeShort(short val) {
            ensureCapacity(2);

            UNSAFE.putShort(bytes, OFFSET + cnt, val);

            cnt += 2;
        }

        /**
         * @param val Value to write.
         */
        void writeChar(char val) {
            ensureCapacity(2);

            UNSAFE.putChar(bytes, OFFSET + cnt, val);

            cnt += 2;
        }

        /**
         * @param bytes Bytes to write.
         */
        void writeBytes(byte[] bytes) {
            ensureCapacity(bytes.length);

            System.arraycopy(bytes, 0, this.bytes, cnt, bytes.length);

            cnt += bytes.length;
        }

        /**
         * @param size Number of bytes to write.
         */
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
         * @param cnt Number of bytes to read.
         * @throws IOException If there are no to read.
         */
        void checkAvailable(int cnt) throws IOException {
            if (pos + cnt > bytes.length)
                throw new IOException("Can not read requested amount of bytes, end of stream is reached " +
                    "[total=" + bytes.length + ", pos=" + pos + ", readCnt=" + cnt + ']');
        }

        /**
         * @return Byte value.
         * @throws IOException In case or error.
         */
        byte readByte() throws IOException {
            checkAvailable(1);

            return bytes[pos++];
        }

        /**
         * @return Boolean value.
         * @throws IOException In case or error.
         */
        boolean readBoolean() throws IOException {
            checkAvailable(1);

            int val = bytes[pos++];

            return val != 0;
        }

        /**
         * @return Integer value.
         * @throws IOException In case or error.
         */
        int readInt() throws IOException {
            checkAvailable(4);

            int res = UNSAFE.getInt(bytes, OFFSET + pos);

            pos += 4;

            return res;
        }

        /**
         * @return Long value.
         * @throws IOException In case or error.
         */
        long readLong() throws IOException {
            checkAvailable(8);

            long res = UNSAFE.getLong(bytes, OFFSET + pos);

            pos += 8;

            return res;
        }

        /**
         * @return Float value.
         * @throws IOException In case or error.
         */
        float readFloat() throws IOException {
            checkAvailable(4);

            float res = UNSAFE.getFloat(bytes, OFFSET + pos);

            pos += 4;

            return res;
        }

        /**
         * @return Double value.
         * @throws IOException In case or error.
         */
        double readDouble() throws IOException {
            checkAvailable(8);

            double res = UNSAFE.getDouble(bytes, OFFSET + pos);

            pos += 8;

            return res;
        }

        /**
         * @return Double value.
         * @throws IOException In case or error.
         */
        short readShort() throws IOException {
            checkAvailable(1);

            short res = UNSAFE.getShort(bytes, OFFSET + pos);

            pos += 2;

            return res;
        }

        /**
         * @return Double value.
         * @throws IOException In case or error.
         */
        char readChar() throws IOException {
            checkAvailable(2);

            char res = UNSAFE.getChar(bytes, OFFSET + pos);

            pos += 2;

            return res;
        }

        /**
         * @param len Number of bytes to read.
         * @return Bytes.
         * @throws IOException In case of error.
         */
        byte[] readBytes(int len) throws IOException {
            checkAvailable(len);

            byte[] bytes = Arrays.copyOfRange(this.bytes, pos, pos + len);

            skip(len);

            return bytes;
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
            onWrite(fieldName);

            out.writeByte(val);
        }

        /** {@inheritDoc} */
        @Override public void writeInt(String fieldName, int val) {
            onWrite(fieldName);

            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void writeLong(String fieldName, long val) {
            onWrite(fieldName);

            out.writeLong(val);
        }

        /** {@inheritDoc} */
        @Override public void writeString(String fieldName, String val) {
            onWrite(fieldName);

            writeString(val);
        }

        /** {@inheritDoc} */
        @Override public void writeFloat(String fieldName, float val) throws IOException {
            onWrite(fieldName);

            out.writeFloat(val);
        }

        /** {@inheritDoc} */
        @Override public void writeDouble(String fieldName, double val) throws IOException {
            onWrite(fieldName);

            out.writeDouble(val);
        }

        /** {@inheritDoc} */
        @Override public void writeShort(String fieldName, short val) throws IOException {
            onWrite(fieldName);


        }

        /** {@inheritDoc} */
        @Override public void writeChar(String fieldName, char val) throws IOException {
            onWrite(fieldName);


        }

        /**
         * @param val String.
         */
        private void writeString(@Nullable String val) {
            if (val != null) {
                byte[] bytes = val.getBytes(UTF_8);

                out.writeInt(bytes.length);
                out.writeBytes(bytes);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public void writeBoolean(String fieldName, boolean val) throws IOException {
            onWrite(fieldName);

            out.writeBoolean(val);
        }

        /** {@inheritDoc} */
        @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws IOException {
            onWrite(fieldName);

            writeByteArray(val);
        }

        /**
         * @param val Value to write.
         */
        private void writeByteArray(@Nullable byte[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.writeBytes(val);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws IOException {
            onWrite(fieldName);

            writeCollection(col);
        }

        /**
         * @param col Collection.
         * @throws IOException In case of error.
         */
        private void writeCollection(@Nullable Collection<?> col) throws IOException {
            if (col != null) {
                out.writeInt(col.size());

                for (Object obj : col)
                    writeObject(obj);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public void writeUuid(String fieldName, @Nullable UUID uuid) throws IOException {
            onWrite(fieldName);

            writeUuid(uuid);
        }

        /**
         * @param uuid UUID.
         */
        private void writeUuid(@Nullable UUID uuid) {
            out.writeBoolean(uuid != null);

            if (uuid != null) {
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());
            }
        }

        /** {@inheritDoc} */
        @Override public <K, V> void writeMap(String fieldName, Map<K, V> map) throws IOException {
            onWrite(fieldName);

            writeMap(map);
        }

        /**
         * @param map Map.
         * @throws IOException In case of error.
         */
        private <K, V> void writeMap(Map<K, V> map) throws IOException {
            if (map != null) {
                out.writeInt(map.size());

                for (Map.Entry<K, V> e : map.entrySet()) {
                    writeObject(e.getKey());
                    writeObject(e.getValue());
                }
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public <T> void writeObject(String fieldName, T obj) throws IOException {
            onWrite(fieldName);

            writeObject(obj);
        }

        /**
         * @param obj Object to write.
         * @throws IOException In case of error.
         */
        private <T> void writeObject(T obj) throws IOException {
            if (obj instanceof Byte) {
                out.writeByte(TYPE_BYTE);
                out.writeByte((Byte)obj);
            }
            else if (obj instanceof Boolean) {
                out.writeByte(TYPE_BOOLEAN);
                out.writeBoolean((Boolean)obj);
            }
            else if (obj instanceof Integer) {
                out.writeByte(TYPE_INT);
                out.writeInt((Integer)obj);
            }
            else if (obj instanceof Long) {
                out.writeByte(TYPE_LONG);
                out.writeLong((Long)obj);
            }
            else if (obj instanceof Float) {
                out.writeByte(TYPE_FLOAT);
                out.writeFloat((Float)obj);
            }
            else if (obj instanceof Double) {
                out.writeByte(TYPE_DOUBLE);
                out.writeDouble((Double)obj);
            }
            else if (obj instanceof String) {
                out.writeByte(TYPE_STRING);

                writeString((String)obj);
            }
            else if (obj instanceof Short) {
                out.writeByte(TYPE_SHORT);

                out.writeShort((Short)obj);
            }
            else if (obj instanceof Character) {
                out.writeByte(TYPE_CHAR);

                out.writeChar((Character)obj);
            }
            else if (obj instanceof UUID) {
                out.writeByte(TYPE_UUID);

                writeUuid((UUID)obj);
            }
            else if (obj instanceof GridPortableObject) {
                out.writeByte(TYPE_USER_OBJECT);

                writePortable((GridPortableObject)obj);
            }
            else if (obj instanceof Collection) {
                out.writeByte(TYPE_LIST);

                writeCollection((Collection)obj);
            }
            else if (obj instanceof Map) {
                out.writeByte(TYPE_MAP);

                writeMap((Map)obj);
            }
            else if (obj instanceof byte[]) {
                out.writeByte(TYPE_BYTE_ARRAY);

                writeByteArray((byte[])obj);
            }
            else if (obj instanceof boolean[]) {
                out.writeByte(TYPE_BOOLEAN_ARRAY);

                writeBooleanArray((boolean[]) obj);
            }
            else if (obj instanceof short[]) {
                out.writeByte(TYPE_SHORT_ARRAY);

                writeShortArray((short[]) obj);
            }
            else if (obj instanceof char[]) {
                out.writeByte(TYPE_CHAR_ARRAY);

                writeCharArray((char[]) obj);
            }
            else if (obj instanceof int[]) {
                out.writeByte(TYPE_INT_ARRAY);

                writeIntArray((int[]) obj);
            }
            else if (obj instanceof long[]) {
                out.writeByte(TYPE_LONG_ARRAY);

                writeLongArray((long[]) obj);
            }
            else if (obj instanceof float[]) {
                out.writeByte(TYPE_FLOAT_ARRAY);

                writeFloatArray((float[]) obj);
            }
            else if (obj instanceof double[]) {
                out.writeByte(TYPE_DOUBLE_ARRAY);

                writeDoubleArray((double[]) obj);
            }
            else if (obj == null)
                out.writeByte(TYPE_NULL);
            else
                throw new IOException("Unsupported object: " + obj);
        }

        /** {@inheritDoc} */
        @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws IOException {
            onWrite(fieldName);

            writeBooleanArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeBooleanArray(@Nullable boolean[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length);

                for (int i = 0; i < val.length; i++)
                    out.writeBoolean(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws IOException {
            onWrite(fieldName);

            writeShortArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeShortArray(@Nullable short[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length * 2);

                for (int i = 0; i < val.length; i++)
                    out.writeShort(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws IOException {
            onWrite(fieldName);

            writeCharArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeCharArray(@Nullable char[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length * 2);

                for (int i = 0; i < val.length; i++)
                    out.writeChar(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void writeIntArray(String fieldName, int[] val) throws IOException {
            onWrite(fieldName);

            writeIntArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeIntArray(int[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length * 4);

                for (int i = 0; i < val.length; i++)
                    out.writeInt(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void writeLongArray(String fieldName, long[] val) throws IOException {
            onWrite(fieldName);

            writeLongArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeLongArray(long[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length * 8);

                for (int i = 0; i < val.length; i++)
                    out.writeLong(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void writeFloatArray(String fieldName, float[] val) throws IOException {
            onWrite(fieldName);

            writeFloatArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeFloatArray(float[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length * 4);

                for (int i = 0; i < val.length; i++)
                    out.writeFloat(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override public void writeDoubleArray(String fieldName, double[] val) throws IOException {
            onWrite(fieldName);

            writeDoubleArray(val);
        }

        /**
         * @param val Value to write.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void writeDoubleArray(double[] val) {
            if (val != null) {
                out.writeInt(val.length);

                out.ensureCapacity(val.length * 8);

                for (int i = 0; i < val.length; i++)
                    out.writeDouble(val[i]);
            }
            else
                out.writeInt(-1);
        }

        /**
         * @param fieldName Field name.
         */
        @SuppressWarnings("UnusedParameters")
        private void onWrite(String fieldName) {
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
        @Override public byte readByte(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readByte();
        }

        /** {@inheritDoc} */
        @Override public int readInt(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readInt();
        }

        /** {@inheritDoc} */
        @Override public long readLong(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readLong();
        }

        /** {@inheritDoc} */
        @Nullable @Override public String readString(String fieldName) throws IOException {
            onRead(fieldName);

            return readString();
        }

        /** {@inheritDoc} */
        @Override public float readFloat(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readFloat();
        }

        /** {@inheritDoc} */
        @Override public double readDouble(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readDouble();
        }

        /**
         * @return String.
         * @throws IOException In case of error.
         */
        @Nullable private String readString() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            in.checkAvailable(len);

            String res = new String(in.bytes(), in.position(), len, UTF_8);

            in.skip(len);

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean readBoolean(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readBoolean();
        }

        /** {@inheritDoc} */
        @Override public short readShort(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readShort();
        }

        /** {@inheritDoc} */
        @Override public char readChar(String fieldName) throws IOException {
            onRead(fieldName);

            return in.readChar();
        }

        /** {@inheritDoc} */
        @Nullable @Override public byte[] readByteArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readByteArray();
        }

        /**
         * @return Byte array.
         * @throws IOException If failed.
         */
        @Nullable private byte[] readByteArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            return in.readBytes(len);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws IOException {
            onRead(fieldName);

            return readCollection();
        }

        /**
         * @return Collection.
         * @throws IOException In case of error.
         */
        @Nullable private <T> Collection<T> readCollection() throws IOException {
            int size = in.readInt();

            if (size == -1)
                return null;

            Collection<T> col = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                col.add((T) readObject());

            return col;
        }

        /** {@inheritDoc} */
        @Nullable @Override public UUID readUuid(String fieldName) throws IOException {
            onRead(fieldName);

            return readUuid();
        }

        /**
         * @return UUID.
         * @throws IOException In case of error.
         */
        @Nullable private UUID readUuid() throws IOException {
            if (!in.readBoolean())
                return null;

            return new UUID(in.readLong(), in.readLong());
        }

        /** {@inheritDoc} */
        @Override public <T> T readObject(String fieldName) throws IOException {
            onRead(fieldName);

            return readObject();
        }

        /** {@inheritDoc} */
        @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws IOException {
            onRead(fieldName);

            return readMap();
        }

        /**
         * @return Map.
         * @throws IOException In case of error.
         */
        @Nullable private <K, V> Map<K, V> readMap() throws IOException {
            int size = in.readInt();

            if (size == -1)
                return null;

            Map<K, V> map = new HashMap<>(size, 1.0f);

            for (int i = 0; i < size; i++) {
                K key = readObject();
                V val = readObject();

                map.put(key, val);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readBooleanArray();
        }

        /**
         * @return Short array.
         * @throws IOException In case of error.
         */
        @Nullable private boolean[] readBooleanArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            boolean[] res = new boolean[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readBoolean();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public short[] readShortArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readShortArray();
        }

        /**
         * @return Short array.
         * @throws IOException In case of error.
         */
        @Nullable private short[] readShortArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            short[] res = new short[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readShort();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public int[] readIntArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readIntArray();
        }

        /**
         * @return Integer array.
         * @throws IOException In case of error.
         */
        @Nullable private int[] readIntArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            int[] res = new int[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readInt();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public char[] readCharArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readCharArray();
        }

        /**
         * @return Char array.
         * @throws IOException In case of error.
         */
        @Nullable private char[] readCharArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            char[] res = new char[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readChar();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public long[] readLongArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readLongArray();
        }

        /**
         * @return Long array.
         * @throws IOException In case of error.
         */
        @Nullable private long[] readLongArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            long[] res = new long[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readLong();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public float[] readFloatArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readFloatArray();
        }

        /**
         * @return Float array.
         * @throws IOException In case of error.
         */
        @Nullable private float[] readFloatArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            float[] res = new float[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readFloat();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public double[] readDoubleArray(String fieldName) throws IOException {
            onRead(fieldName);

            return readDoubleArray();
        }

        /**
         * @return Double array.
         * @throws IOException In case of error.
         */
        @Nullable private double[] readDoubleArray() throws IOException {
            int len = in.readInt();

            if (len == -1)
                return null;

            double[] res = new double[len];

            for (int i = 0; i < len; i++)
                res[i] = in.readDouble();

            return res;
        }

        /**
         * @return Object.
         * @throws IOException In case of error.
         */
        @Nullable private <T> T readObject() throws IOException {
            byte type = in.readByte();

            Object res;

            switch (type) {
                case TYPE_NULL:
                    res = null;

                    break;

                case TYPE_BYTE:
                    res = in.readByte();

                    break;

                case TYPE_BOOLEAN:
                    res = in.readBoolean();

                    break;

                case TYPE_INT:
                    res = in.readInt();

                    break;

                case TYPE_LONG:
                    res = in.readLong();

                    break;

                case TYPE_FLOAT:
                    res = in.readFloat();

                    break;

                case TYPE_DOUBLE:
                    res = in.readDouble();

                    break;

                case TYPE_SHORT:
                    res = in.readShort();

                    break;

                case TYPE_CHAR:
                    res = in.readChar();

                    break;

                case TYPE_STRING:
                    res = readString();

                    break;

                case TYPE_UUID:
                    res = readUuid();

                    break;

                case TYPE_LIST:
                    res = readCollection();

                    break;

                case TYPE_MAP:
                    res = readMap();

                    break;

                case TYPE_BYTE_ARRAY:
                    res = readByteArray();

                    break;

                case TYPE_BOOLEAN_ARRAY:
                    res = readBooleanArray();

                    break;

                case TYPE_SHORT_ARRAY:
                    res = readShortArray();

                    break;

                case TYPE_CHAR_ARRAY:
                    res = readCharArray();

                    break;

                case TYPE_INT_ARRAY:
                    res = readIntArray();

                    break;

                case TYPE_LONG_ARRAY:
                    res = readLongArray();

                    break;

                case TYPE_FLOAT_ARRAY:
                    res = readFloatArray();

                    break;

                case TYPE_DOUBLE_ARRAY:
                    res = readDoubleArray();

                    break;

                case TYPE_USER_OBJECT:
                    res = readPortable();

                    break;

                default:
                    throw new IOException("Invalid type: " + type);
            }

            return (T)res;
        }

        /**
         * @param fieldName Field name.
         */
        @SuppressWarnings("UnusedParameters")
        private void onRead(String fieldName) {
            // No-op.
        }
    }
}
