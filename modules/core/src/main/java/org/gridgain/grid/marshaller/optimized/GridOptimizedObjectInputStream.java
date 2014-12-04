/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.optimized;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import sun.misc.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.marshaller.optimized.GridOptimizedMarshallerUtils.*;

/**
 * Optimized object input stream.
 */
class GridOptimizedObjectInputStream extends ObjectInputStream {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Dummy object for HashSet. */
    private static final Object DUMMY = new Object();

    /** */
    private final HandleTable handles = new HandleTable(10);

    /** */
    private ClassLoader clsLdr;

    /** */
    private GridDataInput in;

    /** */
    private Object curObj;

    /** */
    private List<T2<GridOptimizedFieldType, Long>> curFields;

    /** */
    private List<IgniteBiTuple<Integer, GridOptimizedFieldType>> curFieldInfoList;

    /** */
    private Map<String, IgniteBiTuple<Integer, GridOptimizedFieldType>> curFieldInfoMap;

    /** */
    private Class<?> curCls;

    /**
     * @param in Input.
     * @throws IOException In case of error.
     */
    GridOptimizedObjectInputStream(GridDataInput in) throws IOException {
        this.in = in;
    }

    /**
     * @throws IOException In case of error.
     */
    GridOptimizedObjectInputStream() throws IOException {
        // No-op.
    }

    /**
     * @param clsLdr Class loader.
     */
    void classLoader(ClassLoader clsLdr) {
        this.clsLdr = clsLdr;
    }

    /**
     * @return Class loader.
     */
    ClassLoader classLoader() {
        return clsLdr;
    }

    /**
     * @return Input.
     */
    public GridDataInput in() {
        return in;
    }

    /**
     * @param in Input.
     */
    public void in(GridDataInput in) {
        this.in = in;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        reset();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void reset() throws IOException {
        in.reset();
        handles.clear();

        curObj = null;
        curFields = null;
        curFieldInfoList = null;
        curFieldInfoMap = null;
    }

    /** {@inheritDoc} */
    @Override public Object readObjectOverride() throws ClassNotFoundException, IOException {
        curObj = null;
        curFields = null;
        curFieldInfoList = null;
        curFieldInfoMap = null;

        byte ref = in.readByte();

        switch (ref) {
            case NULL:
                return null;

            case HANDLE:
                return handles.lookup(readInt());

            case OBJECT:
                GridOptimizedClassDescriptor desc = GridOptimizedClassResolver.readClass(this, clsLdr);

                curCls = desc.describedClass();

                return desc.read(this);

            default:
                SB msg = new SB("Unexpected error occurred during unmarshalling");

                if (curCls != null)
                    msg.a(" of an instance of the class: ").a(curCls.getName());

                msg.a(". Check that all nodes are running the same version of GridGain and that all nodes have " +
                    "GridOptimizedMarshaller configured with identical optimized classes lists, if any " +
                    "(see setClassNames and setClassNamesPath methods). If your serialized classes implement " +
                    "java.io.Externalizable interface, verify that serialization logic is correct.");

                throw new IOException(msg.toString());
        }
    }

    /**
     * Reads array from this stream.
     *
     * @param compType Array component type.
     * @return Array.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    <T> T[] readArray(Class<T> compType) throws ClassNotFoundException, IOException {
        int len = in.readInt();

        T[] arr = (T[])Array.newInstance(compType, len);

        handles.assign(arr);

        for (int i = 0; i < len; i++)
            arr[i] = (T)readObject();

        return arr;
    }

    /**
     * Reads {@link UUID} from this stream.
     *
     * @return UUID.
     * @throws IOException In case of error.
     */
    UUID readUuid() throws IOException {
        UUID uuid = new UUID(readLong(), readLong());

        handles.assign(uuid);

        return uuid;
    }

    /**
     * Reads {@link Properties} from this stream.
     *
     * @return Properties.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    Properties readProperties() throws ClassNotFoundException, IOException {
        Properties dflts = readBoolean() ? null : (Properties)readObject();

        Properties props = new Properties(dflts);

        int size = in.readInt();

        for (int i = 0; i < size; i++)
            props.setProperty(readUTF(), readUTF());

        handles.assign(props);

        return props;
    }

    /**
     * Reads and sets all non-static and non-transient field values from this stream.
     *
     * @param obj Object.
     * @param fieldOffs Field offsets.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void readFields(Object obj, List<T2<GridOptimizedFieldType, Long>> fieldOffs) throws ClassNotFoundException,
        IOException {
        for (int i = 0; i < fieldOffs.size(); i++) {
            T2<GridOptimizedFieldType, Long> t = fieldOffs.get(i);

            switch ((t.get1())) {
                case BYTE:
                    setByte(obj, t.get2(), readByte());

                    break;

                case SHORT:
                    setShort(obj, t.get2(), readShort());

                    break;

                case INT:
                    setInt(obj, t.get2(), readInt());

                    break;

                case LONG:
                    setLong(obj, t.get2(), readLong());

                    break;

                case FLOAT:
                    setFloat(obj, t.get2(), readFloat());

                    break;

                case DOUBLE:
                    setDouble(obj, t.get2(), readDouble());

                    break;

                case CHAR:
                    setChar(obj, t.get2(), readChar());

                    break;

                case BOOLEAN:
                    setBoolean(obj, t.get2(), readBoolean());

                    break;

                case OTHER:
                    setObject(obj, t.get2(), readObject());
            }
        }
    }

    /**
     * Reads {@link Externalizable} object.
     *
     * @param constructor Constructor.
     * @param readResolveMtd {@code readResolve} method.
     * @return Object.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    Object readExternalizable(Constructor<?> constructor, Method readResolveMtd)
        throws ClassNotFoundException, IOException {
        Object obj;

        try {
            obj = constructor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        }

        int handle = handles.assign(obj);

        Externalizable extObj = ((Externalizable)obj);

        extObj.readExternal(this);

        if (readResolveMtd != null) {
            try {
                obj = readResolveMtd.invoke(obj);

                handles.set(handle, obj);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        return obj;
    }

    /**
     * Reads serializable object.
     *
     * @param cls Class.
     * @param mtds {@code readObject} methods.
     * @param readResolveMtd {@code readResolve} method.
     * @param fields class fields details.
     * @return Object.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    Object readSerializable(Class<?> cls, List<Method> mtds, Method readResolveMtd,
        GridOptimizedClassDescriptor.Fields fields) throws ClassNotFoundException, IOException {
        Object obj;

        try {
            obj = UNSAFE.allocateInstance(cls);
        }
        catch (InstantiationException e) {
            throw new IOException(e);
        }

        int handle = handles.assign(obj);

        for (int i = 0; i < mtds.size(); i++) {
            Method mtd = mtds.get(i);

            if (mtd != null) {
                curObj = obj;
                curFields = fields.fieldOffs(i);
                curFieldInfoList = fields.fieldInfoList(i);
                curFieldInfoMap = fields.fieldInfoMap(i);

                try {
                    mtd.invoke(obj, this);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IOException(e);
                }
            }
            else
                readFields(obj, fields.fieldOffs(i));
        }

        if (readResolveMtd != null) {
            try {
                obj = readResolveMtd.invoke(obj);

                handles.set(handle, obj);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        return obj;
    }

    /**
     * Reads {@link ArrayList}.
     *
     * @return List.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    ArrayList<?> readArrayList() throws ClassNotFoundException, IOException {
        int size = readInt();

        ArrayList<Object> list = new ArrayList<>(size);

        handles.assign(list);

        for (int i = 0; i < size; i++)
            list.add(readObject());

        return list;
    }

    /**
     * Reads {@link HashMap}.
     *
     * @param set Whether reading underlying map from {@link HashSet}.
     * @return Map.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    HashMap<?, ?> readHashMap(boolean set) throws ClassNotFoundException, IOException {
        int size = readInt();
        float loadFactor = readFloat();

        HashMap<Object, Object> map = new HashMap<>(size, loadFactor);

        if (!set)
            handles.assign(map);

        for (int i = 0; i < size; i++) {
            Object key = readObject();
            Object val = !set ? readObject() : DUMMY;

            map.put(key, val);
        }

        return map;
    }

    /**
     * Reads {@link HashSet}.
     *
     * @param mapFieldOff Map field offset.
     * @return Set.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    HashSet<?> readHashSet(long mapFieldOff) throws ClassNotFoundException, IOException {
        try {
            HashSet<Object> set = (HashSet<Object>)UNSAFE.allocateInstance(HashSet.class);

            handles.assign(set);

            setObject(set, mapFieldOff, readHashMap(true));

            return set;
        }
        catch (InstantiationException e) {
            throw new IOException(e);
        }
    }

    /**
     * Reads {@link LinkedList}.
     *
     * @return List.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    LinkedList<?> readLinkedList() throws ClassNotFoundException, IOException {
        int size = readInt();

        LinkedList<Object> list = new LinkedList<>();

        handles.assign(list);

        for (int i = 0; i < size; i++)
            list.add(readObject());

        return list;
    }

    /**
     * Reads {@link LinkedHashMap}.
     *
     * @param set Whether reading underlying map from {@link LinkedHashSet}.
     * @return Map.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    LinkedHashMap<?, ?> readLinkedHashMap(boolean set) throws ClassNotFoundException, IOException {
        int size = readInt();
        float loadFactor = readFloat();
        boolean accessOrder = readBoolean();

        LinkedHashMap<Object, Object> map = new LinkedHashMap<>(size, loadFactor, accessOrder);

        if (!set)
            handles.assign(map);

        for (int i = 0; i < size; i++) {
            Object key = readObject();
            Object val = !set ? readObject() : DUMMY;

            map.put(key, val);
        }

        return map;
    }

    /**
     * Reads {@link LinkedHashSet}.
     *
     * @param mapFieldOff Map field offset.
     * @return Set.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    LinkedHashSet<?> readLinkedHashSet(long mapFieldOff) throws ClassNotFoundException, IOException {
        try {
            LinkedHashSet<Object> set = (LinkedHashSet<Object>)UNSAFE.allocateInstance(LinkedHashSet.class);

            handles.assign(set);

            setObject(set, mapFieldOff, readLinkedHashMap(true));

            return set;
        }
        catch (InstantiationException e) {
            throw new IOException(e);
        }
    }

    /**
     * Reads {@link Date}.
     *
     * @return Date.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    Date readDate() throws ClassNotFoundException, IOException {
        Date date = new Date(readLong());

        handles.assign(date);

        return date;
    }

    /**
     * Reads array of {@code byte}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    byte[] readByteArray() throws IOException {
        byte[] arr = in.readByteArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code short}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    short[] readShortArray() throws IOException {
        short[] arr = in.readShortArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code int}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    int[] readIntArray() throws IOException {
        int[] arr = in.readIntArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code long}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    long[] readLongArray() throws IOException {
        long[] arr = in.readLongArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code float}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    float[] readFloatArray() throws IOException {
        float[] arr = in.readFloatArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code double}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    double[] readDoubleArray() throws IOException {
        double[] arr = in.readDoubleArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code char}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    char[] readCharArray() throws IOException {
        char[] arr = in.readCharArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads array of {@code boolean}s.
     *
     * @return Array.
     * @throws IOException In case of error.
     */
    boolean[] readBooleanArray() throws IOException {
        boolean[] arr = in.readBooleanArray();

        handles.assign(arr);

        return arr;
    }

    /**
     * Reads {@link String}.
     *
     * @return String.
     * @throws IOException In case of error.
     */
    public String readString() throws IOException {
        String str = in.readUTF();

        handles.assign(str);

        return str;
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b) throws IOException {
        in.readFully(b);
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b, int off, int len) throws IOException {
        in.readFully(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        return in.skipBytes(n);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws IOException {
        return in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        return in.readByte();
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return in.readUnsignedByte();
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws IOException {
        return in.readShort();
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return in.readUnsignedShort();
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws IOException {
        return in.readChar();
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws IOException {
        return in.readInt();
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws IOException {
        return in.readLong();
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws IOException {
        return in.readFloat();
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws IOException {
        return in.readDouble();
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        return in.read();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b) throws IOException {
        return in.read(b);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public String readLine() throws IOException {
        return in.readLine();
    }

    /** {@inheritDoc} */
    @Override public String readUTF() throws IOException {
        return in.readUTF();
    }

    /** {@inheritDoc} */
    @Override public Object readUnshared() throws IOException, ClassNotFoundException {
        return readObject();
    }

    /** {@inheritDoc} */
    @Override public void defaultReadObject() throws IOException, ClassNotFoundException {
        if (curObj == null)
            throw new NotActiveException("Not in readObject() call.");

        readFields(curObj, curFields);
    }

    /** {@inheritDoc} */
    @Override public ObjectInputStream.GetField readFields() throws IOException, ClassNotFoundException {
        if (curObj == null)
            throw new NotActiveException("Not in readObject() call.");

        return new GetFieldImpl(this);
    }

    /** {@inheritDoc} */
    @Override public void registerValidation(ObjectInputValidation obj, int pri) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int available() throws IOException {
        return -1;
    }

    /**
     * Returns objects that were added to handles table.
     * Used ONLY for test purposes.
     *
     * @return Handled objects.
     */
    Object[] handledObjects() {
        return handles.entries;
    }

    /**
     * Lightweight identity hash table which maps objects to integer handles,
     * assigned in ascending order.
     */
    private static class HandleTable {
        /** Array mapping handle -> object/exception (depending on status). */
        private Object[] entries;

        /** Number of handles in table. */
        private int size;

        /**
         * Creates handle table with the given initial capacity.
         *
         * @param initCap Initial capacity.
         */
        HandleTable(int initCap) {
            entries = new Object[initCap];
        }

        /**
         * Assigns next available handle to given object, and returns assigned
         * handle.
         *
         * @param obj Object.
         * @return Handle.
         */
        int assign(Object obj) {
            if (size >= entries.length)
                grow();

            entries[size] = obj;

            return size++;
        }

        /**
         * Assigns new object to existing handle. Old object is forgotten.
         *
         * @param handle Handle.
         * @param obj Object.
         */
        void set(int handle, Object obj) {
            entries[handle] = obj;
        }

        /**
         * Looks up and returns object associated with the given handle.
         *
         * @param handle Handle.
         * @return Object.
         */
        Object lookup(int handle) {
            return entries[handle];
        }

        /**
         * Resets table to its initial state.
         */
        void clear() {
            Arrays.fill(entries, 0, size, null);

            size = 0;
        }

        /**
         * Expands capacity of internal arrays.
         */
        private void grow() {
            int newCap = (entries.length << 1) + 1;

            Object[] newEntries = new Object[newCap];

            System.arraycopy(entries, 0, newEntries, 0, size);

            entries = newEntries;
        }
    }

    /**
     * {@link GetField} implementation.
     */
    private static class GetFieldImpl extends GetField {
        /** Field info map. */
        private final Map<String, IgniteBiTuple<Integer, GridOptimizedFieldType>> fieldInfoMap;

        /** Values. */
        private final Object[] objs;

        /**
         * @param in Stream.
         * @throws IOException In case of error.
         * @throws ClassNotFoundException If class not found.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private GetFieldImpl(GridOptimizedObjectInputStream in) throws IOException, ClassNotFoundException {
            fieldInfoMap = in.curFieldInfoMap;

            List<IgniteBiTuple<Integer, GridOptimizedFieldType>> infos = in.curFieldInfoList;

            objs = new Object[infos.size()];

            for (int i = 0; i < infos.size(); i++) {
                IgniteBiTuple<Integer, GridOptimizedFieldType> t = infos.get(i);

                Object obj = null;

                switch (t.get2()) {
                    case BYTE:
                        obj = in.readByte();

                        break;

                    case SHORT:
                        obj = in.readShort();

                        break;

                    case INT:
                        obj = in.readInt();

                        break;

                    case LONG:
                        obj = in.readLong();

                        break;

                    case FLOAT:
                        obj = in.readFloat();

                        break;

                    case DOUBLE:
                        obj = in.readDouble();

                        break;

                    case CHAR:
                        obj = in.readChar();

                        break;

                    case BOOLEAN:
                        obj = in.readBoolean();

                        break;

                    case OTHER:
                        obj = in.readObject();
                }

                objs[t.get1()] = obj;
            }
        }

        /** {@inheritDoc} */
        @Override public ObjectStreamClass getObjectStreamClass() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean defaulted(String name) throws IOException {
            return objs[fieldInfoMap.get(name).get1()] == null;
        }

        /** {@inheritDoc} */
        @Override public boolean get(String name, boolean dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public byte get(String name, byte dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public char get(String name, char dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public short get(String name, short dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public int get(String name, int dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public long get(String name, long dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public float get(String name, float dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public double get(String name, double dflt) throws IOException {
            return value(name, dflt);
        }

        /** {@inheritDoc} */
        @Override public Object get(String name, Object dflt) throws IOException {
            return value(name, dflt);
        }

        /**
         * @param name Field name.
         * @param dflt Default value.
         * @return Value.
         */
        private <T> T value(String name, T dflt) {
            return objs[fieldInfoMap.get(name).get1()] != null ? (T)objs[fieldInfoMap.get(name).get1()] : dflt;
        }
    }
}
