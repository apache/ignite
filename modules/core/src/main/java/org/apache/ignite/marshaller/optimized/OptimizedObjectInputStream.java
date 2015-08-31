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

package org.apache.ignite.marshaller.optimized;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotActiveException;
import java.io.ObjectInputStream;
import java.io.ObjectInputValidation;
import java.io.ObjectStreamClass;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.io.GridDataInput;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContext;
import sun.misc.Unsafe;

import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.ARRAY_LIST;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BOOLEAN;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BOOLEAN_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BYTE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BYTE_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.CHAR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.CHAR_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.CLS;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.DATE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.DOUBLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.DOUBLE_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.ENUM;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.EXTERNALIZABLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.FLOAT;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.FLOAT_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HANDLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HASH_MAP;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HASH_SET;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HASH_SET_MAP_OFF;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.INT;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.INT_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.JDK;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.JDK_MARSH;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LINKED_HASH_MAP;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LINKED_HASH_SET;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LINKED_LIST;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LONG;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LONG_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.NULL;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.OBJ_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.PROPS;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.SERIALIZABLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.SHORT;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.SHORT_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.STR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.UUID;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.classDescriptor;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setBoolean;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setByte;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setChar;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setDouble;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setFloat;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setInt;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setLong;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setObject;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.setShort;

/**
 * Optimized object input stream.
 */
class OptimizedObjectInputStream extends ObjectInputStream {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Dummy object for HashSet. */
    private static final Object DUMMY = new Object();

    /** */
    private final HandleTable handles = new HandleTable(10);

    /** */
    private MarshallerContext ctx;

    /** */
    private OptimizedMarshallerIdMapper mapper;

    /** */
    private ClassLoader clsLdr;

    /** */
    private GridDataInput in;

    /** */
    private Object curObj;

    /** */
    private OptimizedClassDescriptor.ClassFields curFields;

    /** */
    private Class<?> curCls;

    /** */
    private ConcurrentMap<Class, OptimizedClassDescriptor> clsMap;

    /**
     * @param in Input.
     * @throws IOException In case of error.
     */
    OptimizedObjectInputStream(GridDataInput in) throws IOException {
        this.in = in;
    }

    /**
     * @param clsMap Class descriptors by class map.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @param clsLdr Class loader.
     */
    void context(
        ConcurrentMap<Class, OptimizedClassDescriptor> clsMap,
        MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper,
        ClassLoader clsLdr)
    {
        this.clsMap = clsMap;
        this.ctx = ctx;
        this.mapper = mapper;
        this.clsLdr = clsLdr;
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

        ctx = null;
        clsLdr = null;
        clsMap = null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void reset() throws IOException {
        in.reset();
        handles.clear();

        curObj = null;
        curFields = null;
    }

    /** {@inheritDoc} */
    @Override public Object readObjectOverride() throws ClassNotFoundException, IOException {
        curObj = null;
        curFields = null;

        byte ref = in.readByte();

        switch (ref) {
            case NULL:
                return null;

            case HANDLE:
                return handles.lookup(readInt());

            case JDK:
                try {
                    return JDK_MARSH.unmarshal(this, clsLdr);
                }
                catch (IgniteCheckedException e) {
                    IOException ioEx = e.getCause(IOException.class);

                    if (ioEx != null)
                        throw ioEx;
                    else
                        throw new IOException("Failed to deserialize object with JDK marshaller.", e);
                }

            case BYTE:
                return readByte();

            case SHORT:
                return readShort();

            case INT:
                return readInt();

            case LONG:
                return readLong();

            case FLOAT:
                return readFloat();

            case DOUBLE:
                return readDouble();

            case CHAR:
                return readChar();

            case BOOLEAN:
                return readBoolean();

            case BYTE_ARR:
                return readByteArray();

            case SHORT_ARR:
                return readShortArray();

            case INT_ARR:
                return readIntArray();

            case LONG_ARR:
                return readLongArray();

            case FLOAT_ARR:
                return readFloatArray();

            case DOUBLE_ARR:
                return readDoubleArray();

            case CHAR_ARR:
                return readCharArray();

            case BOOLEAN_ARR:
                return readBooleanArray();

            case OBJ_ARR:
                return readArray(readClass());

            case STR:
                return readString();

            case UUID:
                return readUuid();

            case PROPS:
                return readProperties();

            case ARRAY_LIST:
                return readArrayList();

            case HASH_MAP:
                return readHashMap(false);

            case HASH_SET:
                return readHashSet(HASH_SET_MAP_OFF);

            case LINKED_LIST:
                return readLinkedList();

            case LINKED_HASH_MAP:
                return readLinkedHashMap(false);

            case LINKED_HASH_SET:
                return readLinkedHashSet(HASH_SET_MAP_OFF);

            case DATE:
                return readDate();

            case CLS:
                return readClass();

            case ENUM:
            case EXTERNALIZABLE:
            case SERIALIZABLE:
                int typeId = readInt();

                OptimizedClassDescriptor desc = typeId == 0 ?
                    classDescriptor(clsMap, U.forName(readUTF(), clsLdr), ctx, mapper):
                    classDescriptor(clsMap, typeId, clsLdr, ctx, mapper);

                curCls = desc.describedClass();

                return desc.read(this);

            default:
                SB msg = new SB("Unexpected error occurred during unmarshalling");

                if (curCls != null)
                    msg.a(" of an instance of the class: ").a(curCls.getName());

                msg.a(". Check that all nodes are running the same version of Ignite and that all nodes have " +
                    "GridOptimizedMarshaller configured with identical optimized classes lists, if any " +
                    "(see setClassNames and setClassNamesPath methods). If your serialized classes implement " +
                    "java.io.Externalizable interface, verify that serialization logic is correct.");

                throw new IOException(msg.toString());
        }
    }

    /**
     * @return Class.
     * @throws ClassNotFoundException If class was not found.
     * @throws IOException In case of other error.
     */
    private Class<?> readClass() throws ClassNotFoundException, IOException {
        int compTypeId = readInt();

        return compTypeId == 0 ? U.forName(readUTF(), clsLdr) :
            classDescriptor(clsMap, compTypeId, clsLdr, ctx, mapper).describedClass();
    }

    /**
     * Reads array from this stream.
     *
     * @param compType Array component type.
     * @return Array.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("unchecked")
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
    void readFields(Object obj, OptimizedClassDescriptor.ClassFields fieldOffs) throws ClassNotFoundException,
        IOException {
        for (int i = 0; i < fieldOffs.size(); i++) {
            OptimizedClassDescriptor.FieldInfo t = fieldOffs.get(i);

            switch ((t.type())) {
                case BYTE:
                    byte resByte = readByte();

                    if (t.field() != null)
                        setByte(obj, t.offset(), resByte);

                    break;

                case SHORT:
                    short resShort = readShort();

                    if (t.field() != null)
                        setShort(obj, t.offset(), resShort);

                    break;

                case INT:
                    int resInt = readInt();

                    if (t.field() != null)
                        setInt(obj, t.offset(), resInt);

                    break;

                case LONG:
                    long resLong = readLong();

                    if (t.field() != null)
                        setLong(obj, t.offset(), resLong);

                    break;

                case FLOAT:
                    float resFloat = readFloat();

                    if (t.field() != null)
                        setFloat(obj, t.offset(), resFloat);

                    break;

                case DOUBLE:
                    double resDouble = readDouble();

                    if (t.field() != null)
                        setDouble(obj, t.offset(), resDouble);

                    break;

                case CHAR:
                    char resChar = readChar();

                    if (t.field() != null)
                        setChar(obj, t.offset(), resChar);

                    break;

                case BOOLEAN:
                    boolean resBoolean = readBoolean();

                    if (t.field() != null)
                        setBoolean(obj, t.offset(), resBoolean);

                    break;

                case OTHER:
                    Object resObject = readObject();

                    if (t.field() != null)
                        setObject(obj, t.offset(), resObject);
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
        OptimizedClassDescriptor.Fields fields) throws ClassNotFoundException, IOException {
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
                curFields = fields.fields(i);

                try {
                    mtd.invoke(obj, this);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IOException(e);
                }
            }
            else
                readFields(obj, fields.fields(i));
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
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
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
        /** Field info. */
        private final OptimizedClassDescriptor.ClassFields fieldInfo;

        /** Values. */
        private final Object[] objs;

        /**
         * @param in Stream.
         * @throws IOException In case of error.
         * @throws ClassNotFoundException If class not found.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private GetFieldImpl(OptimizedObjectInputStream in) throws IOException, ClassNotFoundException {
            fieldInfo = in.curFields;

            objs = new Object[fieldInfo.size()];

            for (int i = 0; i < fieldInfo.size(); i++) {
                OptimizedClassDescriptor.FieldInfo t = fieldInfo.get(i);

                Object obj = null;

                switch (t.type()) {
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

                objs[i] = obj;
            }
        }

        /** {@inheritDoc} */
        @Override public ObjectStreamClass getObjectStreamClass() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean defaulted(String name) throws IOException {
            return objs[fieldInfo.getIndex(name)] == null;
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
        @SuppressWarnings("unchecked")
        private <T> T value(String name, T dflt) {
            return objs[fieldInfo.getIndex(name)] != null ? (T)objs[fieldInfo.getIndex(name)] : dflt;
        }
    }
}