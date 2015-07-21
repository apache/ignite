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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.*;

/**
 * Optimized object input stream.
 */
public class OptimizedObjectInputStream extends ObjectInputStream implements OptimizedFieldsReader {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Dummy object for HashSet. */
    private static final Object DUMMY = new Object();

    /** */
    private MarshallerContext ctx;

    /** */
    private OptimizedMarshallerIdMapper mapper;

    /** */
    private ClassLoader clsLdr;

    /** */
    private GridDataInput in;

    /** */
    private ConcurrentMap<Class, OptimizedClassDescriptor> clsMap;

    /** */
    private OptimizedMarshallerIndexingHandler idxHandler;

    /** */
    private Object curObj;

    /** */
    private OptimizedClassDescriptor.ClassFields curFields;

    /** */
    private Class<?> curCls;

    /** */
    private boolean curHasFooter;

    /** */
    private final HandleTable handles = new HandleTable(10);

    /** */
    private Stack<HashMap<Integer, Object>> marshalAwareValues;

    /** */
    private FieldRange range;

    /**
     * @param in Input.
     * @throws IOException In case of error.
     */
    protected OptimizedObjectInputStream(GridDataInput in) throws IOException {
        this.in = in;
    }

    /**
     * @param clsMap Class descriptors by class map.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @param clsLdr Class loader.
     * @param idxHandler Fields indexing handler.
     */
    protected void context(
        ConcurrentMap<Class, OptimizedClassDescriptor> clsMap,
        MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper,
        ClassLoader clsLdr,
        OptimizedMarshallerIndexingHandler idxHandler
        )
    {
        this.clsMap = clsMap;
        this.ctx = ctx;
        this.mapper = mapper;
        this.clsLdr = clsLdr;
        this.idxHandler = idxHandler;
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
        idxHandler = null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void reset() throws IOException {
        in.reset();
        handles.clear();

        curObj = null;
        curFields = null;
        curHasFooter = false;
        marshalAwareValues = null;
    }

    /** {@inheritDoc} */
    @Override public Object readObjectOverride() throws ClassNotFoundException, IOException {
        curObj = null;
        curFields = null;
        curHasFooter = false;

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
            case MARSHAL_AWARE:
                int typeId = readInt();

                OptimizedClassDescriptor desc = typeId == 0 ?
                    classDescriptor(clsMap, U.forName(readUTF(), clsLdr), ctx, mapper, idxHandler):
                    classDescriptor(clsMap, typeId, clsLdr, ctx, mapper, idxHandler);

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
            classDescriptor(clsMap, compTypeId, clsLdr, ctx, mapper, idxHandler).describedClass();
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
     * @param hasFooter Whether footer is included in the marshalled form.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void readFields(Object obj, OptimizedClassDescriptor.ClassFields fieldOffs, boolean hasFooter)
        throws ClassNotFoundException,
        IOException {

        for (int i = 0; i < fieldOffs.size(); i++) {
            OptimizedClassDescriptor.FieldInfo t = fieldOffs.get(i);

            switch ((t.type())) {
                case BYTE:
                    if (hasFooter)
                        readFieldType();

                    byte resByte = readByte();

                    if (t.field() != null)
                        setByte(obj, t.offset(), resByte);

                    break;

                case SHORT:
                    if (hasFooter)
                        readFieldType();

                    short resShort = readShort();

                    if (t.field() != null)
                        setShort(obj, t.offset(), resShort);

                    break;

                case INT:
                    if (hasFooter)
                        readFieldType();

                    int resInt = readInt();

                    if (t.field() != null)
                        setInt(obj, t.offset(), resInt);

                    break;

                case LONG:
                    if (hasFooter)
                        readFieldType();

                    long resLong = readLong();

                    if (t.field() != null)
                        setLong(obj, t.offset(), resLong);

                    break;

                case FLOAT:
                    if (hasFooter)
                        readFieldType();

                    float resFloat = readFloat();

                    if (t.field() != null)
                        setFloat(obj, t.offset(), resFloat);

                    break;

                case DOUBLE:
                    if (hasFooter)
                        readFieldType();

                    double resDouble = readDouble();

                    if (t.field() != null)
                        setDouble(obj, t.offset(), resDouble);

                    break;

                case CHAR:
                    if (hasFooter)
                        readFieldType();

                    char resChar = readChar();

                    if (t.field() != null)
                        setChar(obj, t.offset(), resChar);

                    break;

                case BOOLEAN:
                    if (hasFooter)
                        readFieldType();

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
     * Reads all the fields that are specified in {@link OptimizedObjectMetadata} and places them in {@link HashMap}
     * for furher usage.
     *
     * @param meta Metadata.
     * @return Map containing field ID to value mapping.
     * @throws ClassNotFoundException In case of error.
     * @throws IOException In case of error.
     */
    HashMap<Integer, Object> readFields(OptimizedObjectMetadata meta) throws ClassNotFoundException,
        IOException {

        HashMap<Integer, Object> fieldsValues = new HashMap<>();

        for (Map.Entry<Integer, OptimizedObjectMetadata.FieldInfo> entry : meta.metaList()) {
            Object val;

            switch ((entry.getValue().type())) {
                case BYTE:
                    readFieldType();

                    val = readByte();

                    break;

                case SHORT:
                    readFieldType();

                    val = readShort();

                    break;

                case INT:
                    readFieldType();

                    val = readInt();

                    break;

                case LONG:
                    readFieldType();

                    val = readLong();

                    break;

                case FLOAT:
                    readFieldType();

                    val = readFloat();

                    break;

                case DOUBLE:
                    readFieldType();

                    val = readDouble();

                    break;

                case CHAR:
                    readFieldType();

                    val = readChar();


                    break;

                case BOOLEAN:
                    readFieldType();

                    val = readBoolean();

                    break;

                case OTHER:
                    val = readObject();

                    break;

                default:
                    throw new IgniteException("Unknown field type: " + entry.getValue().type());
            }

            fieldsValues.put(entry.getKey(), val);
        }

        return fieldsValues;
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
     * Reads {@link OptimizedMarshalAware} object.
     *
     * @param constructor Constructor.
     * @param readResolveMtd {@code readResolve} method.
     * @param typeId Object type ID.
     * @return Object.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    Object readMarshalAware(Constructor<?> constructor, Method readResolveMtd, int typeId)
        throws ClassNotFoundException, IOException {
        Object obj;

        try {
            obj = constructor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        }

        int handle = handles.assign(obj);

        OptimizedObjectMetadata meta = idxHandler.metaHandler().metadata(typeId);

        assert meta != null;

        HashMap<Integer, Object> fieldsValues = readFields(meta);

        if (marshalAwareValues == null)
            marshalAwareValues = new Stack<>();

        marshalAwareValues.push(fieldsValues);

        OptimizedMarshalAware extObj = ((OptimizedMarshalAware)obj);

        extObj.readFields(this);

        if (readResolveMtd != null) {
            try {
                obj = readResolveMtd.invoke(obj);

                handles.set(handle, obj);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        marshalAwareValues.pop();

        if (marshalAwareValues.empty())
            marshalAwareValues = null;

        skipFooter();

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

        boolean hasFooter = idxHandler.isFieldsIndexingSupported() && in.readBoolean();

        for (int i = 0; i < mtds.size(); i++) {
            Method mtd = mtds.get(i);

            if (mtd != null) {
                curObj = obj;
                curFields = fields.fields(i);
                curHasFooter = hasFooter;

                try {
                    mtd.invoke(obj, this);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IOException(e);
                }
            }
            else
                readFields(obj, fields.fields(i), hasFooter);
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

        if (hasFooter)
            skipFooter();

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

        readFields(curObj, curFields, curHasFooter);
    }

    /** {@inheritDoc} */
    @Override public ObjectInputStream.GetField readFields() throws IOException, ClassNotFoundException {
        if (curObj == null)
            throw new NotActiveException("Not in readObject() call.");

        return new GetFieldImpl(this);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T readObject(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray(String fieldName) throws IOException {
        return getField(fieldName);
    }

    /**
     * Gets field of {@link OptimizedMarshalAware} object.
     *
     * @param fieldName Field name.
     * @param <F> Field type.
     * @return Field.
     */
    private <F> F getField(String fieldName) {
        return (F)marshalAwareValues.peek().get(OptimizedMarshallerUtils.resolveFieldId(fieldName));
    }

    /**
     * Skips footer.
     */
    protected void skipFooter() throws IOException {
        short footerLen = in.readShort();

        if (footerLen != EMPTY_FOOTER)
            in.skipBytes(footerLen - 2);
    }

    /**
     * Reads field type.
     *
     * @return Field type.
     * @throws IOException In case of error.
     */
    protected int readFieldType() throws IOException {
        return in.readByte();
    }


    /**
     * Checks whether the object has a field with name {@code fieldName}.
     *
     * @param fieldName Field name.
     * @return {@code true} if field exists, {@code false} otherwise.
     * @throws IOException in case of error.
     */
    public boolean hasField(String fieldName) throws IOException {
        int start = in.position();

        byte type = in.readByte(start);

        if (type != SERIALIZABLE && type != MARSHAL_AWARE)
            return false;

        FieldRange range;

        try {
            range = fieldRange(fieldName, start);
        }
        catch (IgniteFieldNotFoundException e) {
            // Ignore
            return false;
        }

        return range != null && range.start >= 0;
    }

    /**
     * Looks up field with the given name and returns it in one of the following representations. If the field is
     * serializable and has a footer then it's not deserialized but rather returned wrapped by {@link CacheObjectImpl}
     * for future processing. In all other cases the field is fully deserialized.
     *
     * @param fieldName Field name.
     * @return Field.
     * @throws IgniteFieldNotFoundException In case if there is no such a field.
     * @throws IOException In case of error.
     * @throws ClassNotFoundException In case of error.
     */
    public <F> F readField(String fieldName, CacheObjectContext objCtx) throws IgniteFieldNotFoundException, IOException, ClassNotFoundException {
        int start = in.position();

        byte type = in.readByte(start);

        if (type != SERIALIZABLE && type != MARSHAL_AWARE)
            throw new IgniteFieldNotFoundException("Object doesn't support fields indexing.");

        FieldRange range = fieldRange(fieldName, start);

        if (range != null && range.start >= 0) {
            byte fieldType = in.readByte(range.start);

            if ((fieldType == SERIALIZABLE && idxHandler.metaHandler().metadata(in.readInt(range.start + 1)) != null)
                || fieldType == MARSHAL_AWARE)
                return  (F)new CacheIndexedObjectImpl(objCtx, in.array(), range.start, range.len);
            else {
                in.position(range.start);

                F obj = (F)readObjectOverride();

                in.position(start);

                return obj;
            }
        }
        else
            throw new IgniteFieldNotFoundException("Object doesn't have a field with the name: " + fieldName);
    }

    /**
     * Returns field offset in the byte stream.
     *
     * @param fieldName Field name.
     * @param start Object's start offset.
     * @return positive range or {@code null} if the object doesn't have such a field.
     * @throws IOException in case of error.
     * @throws IgniteFieldNotFoundException In case if there is no such a field.
     */
    private FieldRange fieldRange(String fieldName, int start) throws IOException, IgniteFieldNotFoundException {
        int pos = start + 1;

        int typeId = in.readInt(pos);
        pos += 4;

        if (typeId == 0) {
            int oldPos = in.position();

            in.position(pos);

            typeId = OptimizedMarshallerUtils.resolveTypeId(readUTF(), mapper);

            in.position(oldPos);
        }

        OptimizedObjectMetadata meta = idxHandler.metaHandler().metadata(typeId);

        if (meta == null)
            // TODO: IGNITE-950 add warning!
            return null;

        int end = in.size();

        short footerLen = in.readShort(end - FOOTER_LEN_OFF);

        if (footerLen == EMPTY_FOOTER)
            throw new IgniteFieldNotFoundException("Object doesn't have a field named: " + fieldName);

        if (range == null)
            range = new FieldRange();

        // Calculating start footer offset. +2 - skipping length at the beginning
        pos = (end - footerLen) + 2;

        int fieldIdx = meta.fieldIndex(fieldName);
        int fieldsCnt = meta.size();

        if (fieldIdx >= fieldsCnt)
            throw new IOException("Wrong field index for field name [idx=" + fieldIdx + ", name=" + fieldName + "]");

        boolean hasHandles = in.readByte(end - FOOTER_HANDLES_FLAG_OFF) == 1;

        if (hasHandles) {
            long fieldInfo = in.readLong(pos + fieldIdx * 8);

            boolean isHandle = ((fieldInfo & FOOTER_BODY_IS_HANDLE_MASK) >> FOOTER_BODY_HANDLE_MASK_BIT) == 1;

            range.start = (int)(fieldInfo & FOOTER_BODY_OFF_MASK);

            if (isHandle) {
                range.len = (int)(fieldInfo >>> 32);

                if (range.len == 0)
                    // Field refers to its object.
                    range.len = (end - start) - footerLen;

                return range;
            }
        }
        else
            range.start = in.readInt(pos + fieldIdx * 4) & FOOTER_BODY_OFF_MASK;

        if (fieldIdx == 0) {
            if (fieldsCnt > 1) {
                int nextFieldOff = in.readInt(pos + (fieldIdx + 1) * 4);
                range.len = nextFieldOff - range.start;
            }
            else
                range.len = (end - footerLen) - range.start;
        }
        else if (fieldIdx == fieldsCnt - 1)
            range.len = (end - footerLen) - range.start;
        else {
            int nextFieldOff = in.readInt(pos + (fieldIdx + 1) * 4);
            range.len = nextFieldOff - range.start;
        }

        return range;
    }

    /**
     *
     */
    private static class FieldRange {
        /** */
        private int start;

        /** */
        private int len;

        /**
         * Constructor.
         */
        public FieldRange() {
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FieldRange.class, this);
        }
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
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readByte();

                        break;

                    case SHORT:
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readShort();

                        break;

                    case INT:
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readInt();

                        break;

                    case LONG:
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readLong();

                        break;

                    case FLOAT:
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readFloat();

                        break;

                    case DOUBLE:
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readDouble();

                        break;

                    case CHAR:
                        if (in.curHasFooter)
                            in.readFieldType();

                        obj = in.readChar();

                        break;

                    case BOOLEAN:
                        if (in.curHasFooter)
                            in.readFieldType();

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
