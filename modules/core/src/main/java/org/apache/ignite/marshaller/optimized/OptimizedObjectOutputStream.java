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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.*;

/**
 * Optimized object output stream.
 */
class OptimizedObjectOutputStream extends ObjectOutputStream {
    /** */
    private static final Collection<String> CONVERTED_ERR = F.asList(
        "weblogic/management/ManagementException",
        "Externalizable class doesn't have default constructor: class " +
            "org.apache.ignite.internal.processors.email.IgniteEmailProcessor$2"
    );

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
    private Footer curFooter;

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
        try {
            writeObject0(obj);
        }
        catch (IOException e) {
            Throwable t = e;

            do {
                if (CONVERTED_ERR.contains(t.getMessage()))
                    throw new IOException("You are trying to serialize internal classes that are not supposed " +
                        "to be serialized. Check that all non-serializable fields are transient. Consider using " +
                        "static inner classes instead of non-static inner classes and anonymous classes.", e);
            }
            while ((t = t.getCause()) != null);

            throw e;
        }
    }

    /**
     * Writes object to stream.
     *
     * @param obj Object.
     * @throws IOException In case of error.
     *
     * @return Handle ID that has already written {@code obj} or -1 if the {@code obj} has not been written before.
     */
    private int writeObject0(Object obj) throws IOException {
        curObj = null;
        curFields = null;
        curPut = null;
        curFooter = null;

        int handle = -1;

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

                    return handle;
                }

                Object obj0 = desc.replace(obj);

                if (obj0 == null) {
                    writeByte(NULL);

                    return handle;
                }

                if (!desc.isPrimitive() && !desc.isEnum() && !desc.isClass())
                    handle = handles.lookup(obj, out.size());

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

        return handle;
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
     * @param headerPos Object's header position in the OutputStream.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void writeSerializable(Object obj, List<Method> mtds, OptimizedClassDescriptor.Fields fields, int headerPos)
        throws IOException {
        Footer footer = new Footer(fields);

        footer.headerPos(headerPos);
        footer.fieldsDataPos(out.size());

        for (int i = 0; i < mtds.size(); i++) {
            Method mtd = mtds.get(i);

            if (mtd != null) {
                curObj = obj;
                curFields = fields.fields(i);
                curFooter = footer;

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
                writeFields(obj, fields.fields(i), footer);
        }

        footer.write();
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
    private void writeFields(Object obj, OptimizedClassDescriptor.ClassFields fields, Footer footer)
        throws IOException {
        int size;
        int relOff = 0;
        boolean skipPut = false;

        for (int i = 0; i < fields.size(); i++) {
            OptimizedClassDescriptor.FieldInfo t = fields.get(i);

            size = out.size();

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
                    if (t.field() != null) {
                        int handle = writeObject0(getObject(obj, t.offset()));

                        if (handle >= 0) {
                            footer.putHandle(handle, t.id());
                            skipPut = true;
                        }
                    }
            }

            if (t.field() != null) {
                int fieldLen = out.size() - size;

                if (!skipPut)
                    footer.put(t.id(), relOff, fieldLen);
                else
                    skipPut = false;

                relOff += fieldLen;
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

        writeFields(curObj, curFields, curFooter);
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

        int size;
        int relOff = 0;
        boolean skipPut = false;

        Footer footer = curPut.curFooter;

        for (IgniteBiTuple<OptimizedClassDescriptor.FieldInfo, Object> t : curPut.objs) {

            size = out.size();

            switch (t.get1().type()) {
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
                    int handle = writeObject0(t.get2());

                    if (handle >= 0) {
                        footer.putHandle(handle, t.get1().id());
                        skipPut = true;
                    }
            }

            int fieldLen = out.size() - size;

            if (!skipPut)
                footer.put(t.get1().id(), relOff, fieldLen);
            else
                skipPut = false;

            relOff += fieldLen;
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
        private final IgniteBiTuple<OptimizedClassDescriptor.FieldInfo, Object>[] objs;

        /** Footer. */
        private final Footer curFooter;

        /**
         * @param out Output stream.
         */
        @SuppressWarnings("unchecked")
        private PutFieldImpl(OptimizedObjectOutputStream out) {
            this.out = out;

            curFields = out.curFields;
            curFooter = out.curFooter;

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

            objs[i] = F.t(info, val);
        }
    }

    /**
     *
     */
    private class Footer {
        /** */
        private int[] data;

        /** */
        private int pos;

        /** */
        private int fieldsDataPos;

        /** */
        private int headerPos;

        /** */
        private HashMap<Integer, Integer> lenForOff;

        /**
         * Constructor.
         *
         * @param fields Fields.
         */
        private Footer(OptimizedClassDescriptor.Fields fields) {
            if (fields.fieldsIndexingEnabled()) {
                int totalFooterSize = 0;

                for (int i = 0; i < fields.hierarchyLevels(); i++)
                    totalFooterSize += fields.fields(i).size() * 3;

                data = new int[totalFooterSize];

                lenForOff = new HashMap<>();
            }
            else
                data = null;
        }

        /**
         * Returns start position of fields' data section.
         *
         * @return Absolute position.
         */
        private int fieldsDataPos() {
            return fieldsDataPos;
        }

        /**
         * Sets field's data section absolute position.
         *
         * @param pos Absolute position.
         */
        private void fieldsDataPos(int pos) {
            fieldsDataPos = pos;
        }

        /**
         * Sets field's header absolute position.
         *
         * @param pos Absolute position.
         */
        private void headerPos(int pos) {
            headerPos = pos;
        }

        /**
         * Puts type ID and its value len to the footer.
         *
         * @param typeId Type ID.
         * @param relOff Offset of an object in fields' data section.
         * @param len Total number of bytes occupied by type's value.
         */
        private void put(int typeId, int relOff, int len) {
            if (data == null)
                return;

            data[pos++] = typeId;
            data[pos++] = relOff;
            data[pos++] = len;

            lenForOff.put(relOff, len);
        }

        /**
         * Puts handle's info to the footer.
         *
         * @param handle Handle.
         * @param typeId Type ID.
         */
        private void putHandle(int handle, int typeId) {
            if (data == null)
                return;

            int handleOff = handles.objectOffset(handle);
            int relOff = fieldsDataPos - handleOff;

            Integer len = lenForOff.get(relOff);

            if (len == null) {
                // this can be a handle to an outer object, we won't be able to process such cases when a field
                // is detached
                data = null;
                lenForOff = null;

                return;
            }

            put(typeId, relOff, len);
        }


        /**
         * Writes footer content to the OutputStream.
         *
         * @throws IOException In case of error.
         */
        private void write() throws IOException {
            if (data == null)
                writeInt(EMPTY_FOOTER);
            else {
                int footerStartPos = out.size();

                writeInt(fieldsDataPos);

                for (int i = 0; i < data.length; i++)
                    writeInt(data[i]);

                // object total len
                writeInt((out.size() - headerPos) + 4);
                // footer len
                writeInt(out.size() - footerStartPos);
            }
        }
    }
}
