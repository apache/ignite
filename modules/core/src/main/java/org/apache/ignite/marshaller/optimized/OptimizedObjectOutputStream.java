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
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridHandleTable;
import org.apache.ignite.internal.util.io.GridDataOutput;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.MarshallerContext;

import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HANDLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.JDK;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.JDK_MARSH;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.NULL;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.classDescriptor;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getBoolean;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getByte;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getChar;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getDouble;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getFloat;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getInt;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getLong;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getObject;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.getShort;

/**
 * Optimized object output stream.
 */
class OptimizedObjectOutputStream extends ObjectOutputStream {
    /** */
    private final GridHandleTable handles = new GridHandleTable(10, 3.00f);

    /** */
    private final GridDataOutput out;

    /** */
    private MarshallerContext ctx;

    /** */
    private OptimizedMarshallerIdMapper mapper;

    /** */
    private boolean requireSer;

    /** */
    private Object curObj;

    /** */
    private OptimizedClassDescriptor.ClassFields curFields;

    /** */
    private PutFieldImpl curPut;

    /** */
    private ConcurrentMap<Class, OptimizedClassDescriptor> clsMap;

    /**
     * @param out Output.
     * @throws IOException In case of error.
     */
    OptimizedObjectOutputStream(GridDataOutput out) throws IOException {
        this.out = out;
    }

    /**
     * @param clsMap Class descriptors by class map.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @param requireSer Require {@link Serializable} flag.
     */
    void context(ConcurrentMap<Class, OptimizedClassDescriptor> clsMap,
        MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper,
        boolean requireSer) {
        this.clsMap = clsMap;
        this.ctx = ctx;
        this.mapper = mapper;
        this.requireSer = requireSer;
    }

    /**
     * @return Require {@link Serializable} flag.
     */
    boolean requireSerializable() {
        return requireSer;
    }

    /**
     * @return Output.
     */
    public GridDataOutput out() {
        return out;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        reset();

        ctx = null;
        clsMap = null;
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) throws IOException {
        out.write(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    /** {@inheritDoc} */
    @Override protected void writeObjectOverride(Object obj) throws IOException {
        writeObject0(obj);
    }

    /**
     * Writes object to stream.
     *
     * @param obj Object.
     * @throws IOException In case of error.
     */
    private void writeObject0(Object obj) throws IOException {
        curObj = null;
        curFields = null;
        curPut = null;

        if (obj == null)
            writeByte(NULL);
        else {
            if (obj instanceof Throwable && !(obj instanceof Externalizable)) {
                writeByte(JDK);

                try {
                    JDK_MARSH.marshal(obj, this);
                }
                catch (IgniteCheckedException e) {
                    IOException ioEx = e.getCause(IOException.class);

                    if (ioEx != null)
                        throw ioEx;
                    else
                        throw new IOException("Failed to serialize object with JDK marshaller: " + obj, e);
                }
            }
            else {
                OptimizedClassDescriptor desc = classDescriptor(
                    clsMap,
                    obj instanceof Object[] ? Object[].class : obj.getClass(),
                    ctx,
                    mapper);

                if (desc.excluded()) {
                    writeByte(NULL);

                    return;
                }

                Object obj0 = desc.replace(obj);

                if (obj0 == null) {
                    writeByte(NULL);

                    return;
                }

                int handle = -1;

                if (!desc.isPrimitive() && !desc.isEnum() && !desc.isClass() && !desc.isProxy())
                    handle = handles.lookup(obj);

                if (obj0 != obj) {
                    obj = obj0;

                    desc = classDescriptor(clsMap,
                        obj instanceof Object[] ? Object[].class : obj.getClass(),
                        ctx,
                        mapper);
                }

                if (handle >= 0) {
                    writeByte(HANDLE);
                    writeInt(handle);
                }
                else
                    desc.write(this, obj);
            }
        }
    }

    /**
     * Writes array to this stream.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void writeArray(Object[] arr) throws IOException {
        int len = arr.length;

        writeInt(len);

        for (int i = 0; i < len; i++) {
            Object obj = arr[i];

            writeObject0(obj);
        }
    }

    /**
     * Writes {@link UUID} to this stream.
     *
     * @param uuid UUID.
     * @throws IOException In case of error.
     */
    void writeUuid(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    /**
     * Writes {@link Properties} to this stream.
     *
     * @param props Properties.
     * @param dfltsFieldOff Defaults field offset.
     * @throws IOException In case of error.
     */
    void writeProperties(Properties props, long dfltsFieldOff) throws IOException {
        Properties dflts = (Properties)getObject(props, dfltsFieldOff);

        if (dflts == null)
            writeBoolean(true);
        else {
            writeBoolean(false);

            writeObject0(dflts);
        }

        Set<String> names = props.stringPropertyNames();

        writeInt(names.size());

        for (String name : names) {
            writeUTF(name);
            writeUTF(props.getProperty(name));
        }
    }

    /**
     * Writes externalizable object.
     *
     * @param obj Object.
     * @throws IOException In case of error.
     */
    void writeExternalizable(Object obj) throws IOException {
        Externalizable extObj = (Externalizable)obj;

        extObj.writeExternal(this);
    }

    /**
     * Writes serializable object.
     *
     * @param obj Object.
     * @param mtds {@code writeObject} methods.
     * @param fields class fields details.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void writeSerializable(Object obj, List<Method> mtds, OptimizedClassDescriptor.Fields fields)
        throws IOException {
        for (int i = 0; i < mtds.size(); i++) {
            Method mtd = mtds.get(i);

            if (mtd != null) {
                curObj = obj;
                curFields = fields.fields(i);

                try {
                    mtd.invoke(obj, this);
                }
                catch (IllegalAccessException e) {
                    throw new IOException(e);
                }
                catch (InvocationTargetException e) {
                    throw new IOException(e.getCause());
                }
            }
            else
                writeFields(obj, fields.fields(i));
        }
    }

    /**
     * Writes {@link ArrayList}.
     *
     * @param list List.
     * @throws IOException In case of error.
     */
    @SuppressWarnings({"ForLoopReplaceableByForEach", "TypeMayBeWeakened"})
    void writeArrayList(ArrayList<?> list) throws IOException {
        int size = list.size();

        writeInt(size);

        for (int i = 0; i < size; i++)
            writeObject0(list.get(i));
    }

    /**
     * Writes {@link HashMap}.
     *
     * @param map Map.
     * @param loadFactorFieldOff Load factor field offset.
     * @param set Whether writing underlying map from {@link HashSet}.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    void writeHashMap(HashMap<?, ?> map, long loadFactorFieldOff, boolean set) throws IOException {
        int size = map.size();

        writeInt(size);
        writeFloat(getFloat(map, loadFactorFieldOff));

        for (Map.Entry<?, ?> e : map.entrySet()) {
            writeObject0(e.getKey());

            if (!set)
                writeObject0(e.getValue());
        }
    }

    /**
     * Writes {@link HashSet}.
     *
     * @param set Set.
     * @param mapFieldOff Map field offset.
     * @param loadFactorFieldOff Load factor field offset.
     * @throws IOException In case of error.
     */
    void writeHashSet(HashSet<?> set, long mapFieldOff, long loadFactorFieldOff) throws IOException {
        writeHashMap((HashMap<?, ?>)getObject(set, mapFieldOff), loadFactorFieldOff, true);
    }

    /**
     * Writes {@link LinkedList}.
     *
     * @param list List.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    void writeLinkedList(LinkedList<?> list) throws IOException {
        int size = list.size();

        writeInt(size);

        for (Object obj : list)
            writeObject0(obj);
    }

    /**
     * Writes {@link LinkedHashMap}.
     *
     * @param map Map.
     * @param loadFactorFieldOff Load factor field offset.
     * @param accessOrderFieldOff access order field offset.
     * @param set Whether writing underlying map from {@link LinkedHashSet}.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    void writeLinkedHashMap(LinkedHashMap<?, ?> map, long loadFactorFieldOff, long accessOrderFieldOff, boolean set)
        throws IOException {
        int size = map.size();

        writeInt(size);
        writeFloat(getFloat(map, loadFactorFieldOff));

        if (accessOrderFieldOff >= 0)
            writeBoolean(getBoolean(map, accessOrderFieldOff));
        else
            writeBoolean(false);

        for (Map.Entry<?, ?> e : map.entrySet()) {
            writeObject0(e.getKey());

            if (!set)
                writeObject0(e.getValue());
        }
    }

    /**
     * Writes {@link LinkedHashSet}.
     *
     * @param set Set.
     * @param mapFieldOff Map field offset.
     * @param loadFactorFieldOff Load factor field offset.
     * @throws IOException In case of error.
     */
    void writeLinkedHashSet(LinkedHashSet<?> set, long mapFieldOff, long loadFactorFieldOff) throws IOException {
        LinkedHashMap<?, ?> map = (LinkedHashMap<?, ?>)getObject(set, mapFieldOff);

        writeLinkedHashMap(map, loadFactorFieldOff, -1, true);
    }

    /**
     * Writes {@link Date}.
     *
     * @param date Date.
     * @throws IOException In case of error.
     */
    void writeDate(Date date) throws IOException {
        writeLong(date.getTime());
    }

    /**
     * Writes all non-static and non-transient field values to this stream.
     *
     * @param obj Object.
     * @param fields Fields.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void writeFields(Object obj, OptimizedClassDescriptor.ClassFields fields) throws IOException {
        for (int i = 0; i < fields.size(); i++) {
            OptimizedClassDescriptor.FieldInfo t = fields.get(i);

            switch (t.type()) {
                case BYTE:
                    if (t.field() != null)
                        writeByte(getByte(obj, t.offset()));

                    break;

                case SHORT:
                    if (t.field() != null)
                        writeShort(getShort(obj, t.offset()));

                    break;

                case INT:
                    if (t.field() != null)
                        writeInt(getInt(obj, t.offset()));

                    break;

                case LONG:
                    if (t.field() != null)
                        writeLong(getLong(obj, t.offset()));

                    break;

                case FLOAT:
                    if (t.field() != null)
                        writeFloat(getFloat(obj, t.offset()));

                    break;

                case DOUBLE:
                    if (t.field() != null)
                        writeDouble(getDouble(obj, t.offset()));

                    break;

                case CHAR:
                    if (t.field() != null)
                        writeChar(getChar(obj, t.offset()));

                    break;

                case BOOLEAN:
                    if (t.field() != null)
                        writeBoolean(getBoolean(obj, t.offset()));

                    break;

                case OTHER:
                    if (t.field() != null)
                        writeObject0(getObject(obj, t.offset()));
            }
        }
    }

    /**
     * Writes array of {@code byte}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeByteArray(byte[] arr) throws IOException {
        out.writeByteArray(arr);
    }

    /**
     * Writes array of {@code short}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeShortArray(short[] arr) throws IOException {
        out.writeShortArray(arr);
    }

    /**
     * Writes array of {@code int}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeIntArray(int[] arr) throws IOException {
        out.writeIntArray(arr);
    }

    /**
     * Writes array of {@code long}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeLongArray(long[] arr) throws IOException {
        out.writeLongArray(arr);
    }

    /**
     * Writes array of {@code float}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeFloatArray(float[] arr) throws IOException {
        out.writeFloatArray(arr);
    }

    /**
     * Writes array of {@code double}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeDoubleArray(double[] arr) throws IOException {
        out.writeDoubleArray(arr);
    }

    /**
     * Writes array of {@code char}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeCharArray(char[] arr) throws IOException {
        out.writeCharArray(arr);
    }

    /**
     * Writes array of {@code boolean}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeBooleanArray(boolean[] arr) throws IOException {
        out.writeBooleanArray(arr);
    }

    /**
     * Writes {@link String}.
     *
     * @param str String.
     * @throws IOException In case of error.
     */
    void writeString(String str) throws IOException {
        out.writeUTF(str);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean v) throws IOException {
        out.writeBoolean(v);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        out.writeByte(v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        out.writeShort(v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        out.writeChar(v);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int v) throws IOException {
        out.writeInt(v);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long v) throws IOException {
        out.writeLong(v);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float v) throws IOException {
        out.writeFloat(v);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double v) throws IOException {
        out.writeDouble(v);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        writeByte(b);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(String s) throws IOException {
        out.writeBytes(s);
    }

    /** {@inheritDoc} */
    @Override public void writeChars(String s) throws IOException {
        out.writeChars(s);
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(String s) throws IOException {
        out.writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void useProtocolVersion(int ver) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeUnshared(Object obj) throws IOException {
        writeObject0(obj);
    }

    /** {@inheritDoc} */
    @Override public void defaultWriteObject() throws IOException {
        if (curObj == null)
            throw new NotActiveException("Not in writeObject() call.");

        writeFields(curObj, curFields);
    }

    /** {@inheritDoc} */
    @Override public ObjectOutputStream.PutField putFields() throws IOException {
        if (curObj == null)
            throw new NotActiveException("Not in writeObject() call or fields already written.");

        if (curPut == null)
            curPut = new PutFieldImpl(this);

        return curPut;
    }

    /** {@inheritDoc} */
    @Override public void writeFields() throws IOException {
        if (curObj == null)
            throw new NotActiveException("Not in writeObject() call.");

        if (curPut == null)
            throw new NotActiveException("putFields() was not called.");

        for (IgniteBiTuple<OptimizedFieldType, Object> t : curPut.objs) {
            switch (t.get1()) {
                case BYTE:
                    writeByte((Byte)t.get2());

                    break;

                case SHORT:
                    writeShort((Short)t.get2());

                    break;

                case INT:
                    writeInt((Integer)t.get2());

                    break;

                case LONG:
                    writeLong((Long)t.get2());

                    break;

                case FLOAT:
                    writeFloat((Float)t.get2());

                    break;

                case DOUBLE:
                    writeDouble((Double)t.get2());

                    break;

                case CHAR:
                    writeChar((Character)t.get2());

                    break;

                case BOOLEAN:
                    writeBoolean((Boolean)t.get2());

                    break;

                case OTHER:
                    writeObject0(t.get2());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() throws IOException {
        out.reset();
        handles.clear();

        curObj = null;
        curFields = null;
        curPut = null;
    }

    /** {@inheritDoc} */
    @Override public void flush() throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void drain() throws IOException {
        // No-op.
    }

    /**
     * Returns objects that were added to handles table.
     * Used ONLY for test purposes.
     *
     * @return Handled objects.
     */
    Object[] handledObjects() {
        return handles.objects();
    }

    /**
     * {@link PutField} implementation.
     */
    private static class PutFieldImpl extends PutField {
        /** Stream. */
        private final OptimizedObjectOutputStream out;

        /** Fields info. */
        private final OptimizedClassDescriptor.ClassFields curFields;
        /** Values. */
        private final IgniteBiTuple<OptimizedFieldType, Object>[] objs;

        /**
         * @param out Output stream.
         */
        @SuppressWarnings("unchecked")
        private PutFieldImpl(OptimizedObjectOutputStream out) {
            this.out = out;

            curFields = out.curFields;

            objs = new IgniteBiTuple[curFields.size()];
        }

        /** {@inheritDoc} */
        @Override public void put(String name, boolean val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, byte val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, char val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, short val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, int val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, long val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, float val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, double val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void put(String name, Object val) {
            value(name, val);
        }

        /** {@inheritDoc} */
        @Override public void write(ObjectOutput out) throws IOException {
            if (out != this.out)
                throw new IllegalArgumentException("Wrong stream.");

            this.out.writeFields();
        }

        /**
         * @param name Field name.
         * @param val Value.
         */
        private void value(String name, Object val) {
            int i = curFields.getIndex(name);

            OptimizedClassDescriptor.FieldInfo info = curFields.get(i);

            objs[i] = F.t(info.type(), val);
        }
    }
}
