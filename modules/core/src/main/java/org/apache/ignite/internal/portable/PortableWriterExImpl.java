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

package org.apache.ignite.internal.portable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.portable.streams.PortableHeapOutputStream;
import org.apache.ignite.internal.portable.streams.PortableOutputStream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableRawWriter;
import org.apache.ignite.portable.PortableWriter;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLASS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP_ENTRY;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OPTM_MARSH;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PORTABLE_OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.RAW_DATA_OFF_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TOTAL_LEN_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID_ARR;

 /**
 * Portable writer implementation.
 */
public class PortableWriterExImpl implements PortableWriter, PortableRawWriterEx, ObjectOutput {
    /** Length: integer. */
    private static final int LEN_INT = 4;

    /** */
    private static final int INIT_CAP = 1024;

    /** */
    private static final ConcurrentHashMap<Class<?>, Boolean> useOptMarshCache = new ConcurrentHashMap<>();

    /** */
    private final PortableContext ctx;

    /** */
    private final WriterContext wCtx;

    /** */
    private final int start;

    /** */
    private int mark;

    /** */
    private Class<?> cls;

    /** */
    private int typeId;

    /** */
    private boolean allowFields = true;

    /** */
    private boolean metaEnabled;

    /** */
    private int metaHashSum;

    /**
     * @param ctx Context.
     * @param off Start offset.
     */
    PortableWriterExImpl(PortableContext ctx, int off) {
        this.ctx = ctx;

        PortableOutputStream out = new PortableHeapOutputStream(off + INIT_CAP);

        out.position(off);

        wCtx = new WriterContext(out, null);

        start = off;
    }

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param off Start offset.
     */
    PortableWriterExImpl(PortableContext ctx, PortableOutputStream out, int off) {
        this.ctx = ctx;

        wCtx = new WriterContext(out, null);

        start = off;
    }

    /**
     * @param ctx Context.
     * @param off Start offset.
     * @param typeId Type ID.
     */
    public PortableWriterExImpl(PortableContext ctx, int off, int typeId, boolean metaEnabled) {
        this(ctx, off);

        this.typeId = typeId;

        this.metaEnabled = metaEnabled;
    }

    /**
     * @param ctx Context.
     * @param wCtx Writer context.
     */
    private PortableWriterExImpl(PortableContext ctx, WriterContext wCtx) {
        this.ctx = ctx;
        this.wCtx = wCtx;

        start = wCtx.out.position();
    }

    /**
     * Close the writer releasing resources if necessary.
     */
    @Override public void close() {
        wCtx.out.close();
    }

    /**
     * @return Meta data hash sum or {@code null} if meta data is disabled.
     */
    @Nullable Integer metaDataHashSum() {
        return metaEnabled ? metaHashSum : null;
    }

    /**
     * @param obj Object.
     * @param detached Detached or not.
     * @throws PortableException In case of error.
     */
    void marshal(Object obj, boolean detached) throws PortableException {
        assert obj != null;

        if (useOptimizedMarshaller(obj)) {
            writeByte(OPTM_MARSH);

            try {
                byte[] arr = ctx.optimizedMarsh().marshal(obj);

                writeInt(arr.length);

                write(arr);
            }
            catch (IgniteCheckedException e) {
                throw new PortableException("Failed to marshal object with optimized marshaller: " + obj, e);
            }

            return;
        }

        cls = obj.getClass();

        PortableClassDescriptor desc = ctx.descriptorForClass(cls);

        if (desc == null)
            throw new PortableException("Object is not portable: [class=" + cls + ']');

        if (desc.excluded()) {
            doWriteByte(NULL);
            return;
        }

        if (desc.getWriteReplaceMethod() != null) {
            Object replace;

            try {
                replace = desc.getWriteReplaceMethod().invoke(obj);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof PortableException)
                    throw (PortableException)e.getTargetException();

                throw new PortableException("Failed to execute writeReplace() method on " + obj, e);
            }

            if (replace == null) {
                doWriteByte(NULL);
                return;
            }

            if (cls != replace.getClass()) {
                cls = replace.getClass();

                desc = ctx.descriptorForClass(cls);

                if (desc == null)
                    throw new PortableException("Object is not portable: [class=" + cls + ']');
            }

            obj = replace;
        }

        typeId = desc.typeId();

        metaEnabled = ctx.isMetaDataEnabled(typeId);

        if (detached)
            wCtx.resetHandles();

        desc.write(obj, this);
    }

     /**
      * Determines whether to use {@link org.apache.ignite.marshaller.optimized.OptimizedMarshaller} for serialization
      * or not.
      *
      * @param obj Object to serialize.
      * @return {@code true} if to use, {@code false} otherwise.
      */
     private boolean useOptimizedMarshaller(Object obj) {
         Class<?> cls = obj.getClass();

         Boolean use = useOptMarshCache.get(cls);

         if (use != null)
             return use;

         if (ctx.isPredefinedClass(cls))
             use = false;
         else {
             try {
                 Method writeObj = cls.getDeclaredMethod("writeObject", ObjectOutputStream.class);
                 Method readObj = cls.getDeclaredMethod("readObject", ObjectInputStream.class);

                 if (!Modifier.isStatic(writeObj.getModifiers()) && !Modifier.isStatic(readObj.getModifiers()) &&
                     writeObj.getReturnType() == void.class && readObj.getReturnType() == void.class)
                     use = true;
                 else
                     use = false;

             } catch (NoSuchMethodException e) {
                 use = false;
             }
         }

         useOptMarshCache.putIfAbsent(cls, use);

         return use;
     }

    /**
     * @param obj Object.
     * @return Handle.
     */
    int handle(Object obj) {
        assert obj != null;

        return wCtx.handle(obj);
    }

    /**
     * @return Array.
     */
    public byte[] array() {
        return wCtx.out.arrayCopy();
    }

    /**
     * @return Output stream.
     */
    public PortableOutputStream outputStream() {
        return wCtx.out;
    }

    /**
     * @return Stream current position.
     */
    int position() {
        return wCtx.out.position();
    }

    /**
     * Sets new position.
     *
     * @param pos Position.
     */
    void position(int pos) {
        wCtx.out.position(pos);
    }

     /**
     * @param bytes Number of bytes to reserve.
     * @return Offset.
     */
    public int reserve(int bytes) {
        int pos = wCtx.out.position();

        wCtx.out.position(pos + bytes);

        return pos;
    }

    /**
     * @param bytes Number of bytes to reserve.
     * @return Offset.
     */
    public int reserveAndMark(int bytes) {
        int off0 = reserve(bytes);

        mark = wCtx.out.position();

        return off0;
    }

    /**
     * @param off Offset.
     */
    public void writeDelta(int off) {
        wCtx.out.writeInt(off, wCtx.out.position() - mark);
    }

    /**
     *
     */
    public void writeLength() {
        wCtx.out.writeInt(start + TOTAL_LEN_POS, wCtx.out.position() - start);
    }

    /**
     *
     */
    public void writeRawOffsetIfNeeded() {
        if (allowFields)
            wCtx.out.writeInt(start + RAW_DATA_OFF_POS, wCtx.out.position() - start);
    }

    /**
     * @param val Byte array.
     */
    public void write(byte[] val) {
        assert val != null;

        wCtx.out.writeByteArray(val);
    }

    /**
     * @param val Byte array.
     * @param off Offset.
     * @param len Length.
     */
    public void write(byte[] val, int off, int len) {
        assert val != null;

        wCtx.out.write(val, off, len);
    }

    /**
     * @param val Value.
     */
    public void doWriteByte(byte val) {
        wCtx.out.writeByte(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteShort(short val) {
        wCtx.out.writeShort(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteInt(int val) {
        wCtx.out.writeInt(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteLong(long val) {
        wCtx.out.writeLong(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteFloat(float val) {
        wCtx.out.writeFloat(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteDouble(double val) {
        wCtx.out.writeDouble(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteChar(char val) {
        wCtx.out.writeChar(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteBoolean(boolean val) {
        wCtx.out.writeBoolean(val);
    }

    /**
     * @param val String value.
     */
    public void doWriteDecimal(@Nullable BigDecimal val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(DECIMAL);

            BigInteger intVal = val.unscaledValue();

            if (intVal.signum() == -1) {
                intVal = intVal.negate();

                wCtx.out.writeInt(val.scale() | 0x80000000);
            }
            else
                wCtx.out.writeInt(val.scale());

            byte[] vals = intVal.toByteArray();

            wCtx.out.writeInt(vals.length);
            wCtx.out.writeByteArray(vals);
        }
    }

    /**
     * @param val String value.
     */
    public void doWriteString(@Nullable String val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(STRING);

            if (ctx.isConvertString()) {
                doWriteBoolean(true);

                byte[] strArr = val.getBytes(UTF_8);

                doWriteInt(strArr.length);

                wCtx.out.writeByteArray(strArr);
            }
            else {
                doWriteBoolean(false);

                char[] strArr = val.toCharArray();

                doWriteInt(strArr.length);

                wCtx.out.writeCharArray(strArr);
            }
        }
    }

    /**
     * @param uuid UUID.
     */
    public void doWriteUuid(@Nullable UUID uuid) {
        if (uuid == null)
            doWriteByte(NULL);
        else {
            doWriteByte(UUID);
            doWriteLong(uuid.getMostSignificantBits());
            doWriteLong(uuid.getLeastSignificantBits());
        }
    }

    /**
     * @param date Date.
     */
    public void doWriteDate(@Nullable Date date) {
        if (date == null)
            doWriteByte(NULL);
        else {
            doWriteByte(DATE);
            doWriteLong(date.getTime());
            doWriteInt(0);
        }
    }

    /**
     * @param ts Timestamp.
     */
    public void doWriteTimestamp(@Nullable Timestamp ts) {
        if (ts == null)
            doWriteByte(NULL);
        else {
            doWriteByte(DATE);
            doWriteLong(ts.getTime());
            doWriteInt(ts.getNanos() % 1000000);
        }
    }

    /**
     * @param obj Object.
     * @param detached Detached or not.
     * @throws PortableException In case of error.
     */
    public void doWriteObject(@Nullable Object obj, boolean detached) throws PortableException {
        if (obj == null)
            doWriteByte(NULL);
        else {
            WriterContext wCtx = detached ? new WriterContext(this.wCtx.out, this.wCtx.handles) : this.wCtx;

            PortableWriterExImpl writer = new PortableWriterExImpl(ctx, wCtx);

            writer.marshal(obj, detached);

            if (detached)
                this.wCtx.out = wCtx.out;
        }
    }

    /**
     * @param val Byte array.
     */
    void doWriteByteArray(@Nullable byte[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(BYTE_ARR);
            doWriteInt(val.length);

            wCtx.out.writeByteArray(val);
        }
    }

    /**
     * @param val Short array.
     */
    void doWriteShortArray(@Nullable short[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(SHORT_ARR);
            doWriteInt(val.length);

            wCtx.out.writeShortArray(val);
        }
    }

    /**
     * @param val Integer array.
     */
    void doWriteIntArray(@Nullable int[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(INT_ARR);
            doWriteInt(val.length);

            wCtx.out.writeIntArray(val);
        }
    }

    /**
     * @param val Long array.
     */
    void doWriteLongArray(@Nullable long[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(LONG_ARR);
            doWriteInt(val.length);

            wCtx.out.writeLongArray(val);
        }
    }

    /**
     * @param val Float array.
     */
    void doWriteFloatArray(@Nullable float[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(FLOAT_ARR);
            doWriteInt(val.length);

            wCtx.out.writeFloatArray(val);
        }
    }

    /**
     * @param val Double array.
     */
    void doWriteDoubleArray(@Nullable double[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(DOUBLE_ARR);
            doWriteInt(val.length);

            wCtx.out.writeDoubleArray(val);
        }
    }

    /**
     * @param val Char array.
     */
    void doWriteCharArray(@Nullable char[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(CHAR_ARR);
            doWriteInt(val.length);

            wCtx.out.writeCharArray(val);
        }
    }

    /**
     * @param val Boolean array.
     */
    void doWriteBooleanArray(@Nullable boolean[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(BOOLEAN_ARR);
            doWriteInt(val.length);

            wCtx.out.writeBooleanArray(val);
        }
    }

    /**
     * @param val Array of strings.
     */
    void doWriteDecimalArray(@Nullable BigDecimal[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(DECIMAL_ARR);
            doWriteInt(val.length);

            for (BigDecimal str : val)
                doWriteDecimal(str);
        }
    }

    /**
     * @param val Array of strings.
     */
    void doWriteStringArray(@Nullable String[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(STRING_ARR);
            doWriteInt(val.length);

            for (String str : val)
                doWriteString(str);
        }
    }

    /**
     * @param val Array of UUIDs.
     */
    void doWriteUuidArray(@Nullable UUID[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(UUID_ARR);
            doWriteInt(val.length);

            for (UUID uuid : val)
                doWriteUuid(uuid);
        }
    }

    /**
     * @param val Array of dates.
     */
    void doWriteDateArray(@Nullable Date[] val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            doWriteByte(DATE_ARR);
            doWriteInt(val.length);

            for (Date date : val)
                doWriteDate(date);
        }
    }

    /**
     * @param val Array of objects.
     * @throws PortableException In case of error.
     */
    void doWriteObjectArray(@Nullable Object[] val) throws PortableException {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            PortableContext.Type type = ctx.typeId(val.getClass().getComponentType());

            doWriteByte(OBJ_ARR);

            if (type.registered())
                doWriteInt(type.id());
            else {
                doWriteInt(UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getComponentType().getName());
            }

            doWriteInt(val.length);

            for (Object obj : val)
                doWriteObject(obj, false);
        }
    }

    /**
     * @param col Collection.
     * @throws PortableException In case of error.
     */
    void doWriteCollection(@Nullable Collection<?> col) throws PortableException {
        if (col == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(col))
                return;

            doWriteByte(COL);
            doWriteInt(col.size());
            doWriteByte(ctx.collectionType(col.getClass()));

            for (Object obj : col)
                doWriteObject(obj, false);
        }
    }

    /**
     * @param map Map.
     * @throws PortableException In case of error.
     */
    void doWriteMap(@Nullable Map<?, ?> map) throws PortableException {
        if (map == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(map))
                return;

            doWriteByte(MAP);
            doWriteInt(map.size());
            doWriteByte(ctx.mapType(map.getClass()));

            for (Map.Entry<?, ?> e : map.entrySet()) {
                doWriteObject(e.getKey(), false);
                doWriteObject(e.getValue(), false);
            }
        }
    }

    /**
     * @param e Map entry.
     * @throws PortableException In case of error.
     */
    void doWriteMapEntry(@Nullable Map.Entry<?, ?> e) throws PortableException {
        if (e == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(e))
                return;

            doWriteByte(MAP_ENTRY);
            doWriteObject(e.getKey(), false);
            doWriteObject(e.getValue(), false);
        }
    }

    /**
     * @param val Value.
     */
    void doWriteEnum(@Nullable Enum<?> val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            PortableContext.Type type = ctx.typeId(val.getClass());

            doWriteByte(ENUM);

            if (type.registered())
                doWriteInt(type.id());
            else {
                doWriteInt(UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getName());
            }

            doWriteInt(val.ordinal());
        }
    }

    /**
     * @param val Array.
     */
    void doWriteEnumArray(@Nullable Object[] val) {
        assert val == null || val.getClass().getComponentType().isEnum();

        if (val == null)
            doWriteByte(NULL);
        else {
            PortableContext.Type type = ctx.typeId(val.getClass().getComponentType());

            doWriteByte(ENUM_ARR);

            if (type.registered())
                doWriteInt(type.id());
            else {
                doWriteInt(UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getComponentType().getName());
            }

            doWriteInt(val.length);

            // TODO: Denis: Redundant data for each element of the array.
            for (Object o : val)
                doWriteEnum((Enum<?>)o);
        }
    }

    /**
     * @param val Class.
     */
    void doWriteClass(@Nullable Class val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            PortableContext.Type type = ctx.typeId(val);

            doWriteByte(CLASS);

            if (type.registered())
                doWriteInt(type.id());
            else {
                doWriteInt(UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getName());
            }
        }
    }

    /**
     * @param po Portable object.
     */
    public void doWritePortableObject(@Nullable PortableObjectImpl po) {
        if (po == null)
            doWriteByte(NULL);
        else {
            doWriteByte(PORTABLE_OBJ);

            byte[] poArr = po.array();

            doWriteInt(poArr.length);

            wCtx.out.writeByteArray(poArr);

            doWriteInt(po.start());
        }
    }

    /**
     * @param val Value.
     */
    void writeByteField(@Nullable Byte val) {
        doWriteInt(val != null ? 2 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(BYTE);
            doWriteByte(val);
        }
    }

    /**
     * @param val Class.
     */
    void writeClassField(@Nullable Class val) {
        int lenPos = reserveAndMark(4);

        doWriteClass(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeShortField(@Nullable Short val) {
        doWriteInt(val != null ? 3 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(SHORT);
            doWriteShort(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeIntField(@Nullable Integer val) {
        doWriteInt(val != null ? 5 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(INT);
            doWriteInt(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeLongField(@Nullable Long val) {
        doWriteInt(val != null ? 9 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(LONG);
            doWriteLong(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeFloatField(@Nullable Float val) {
        doWriteInt(val != null ? 5 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(FLOAT);
            doWriteFloat(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeDoubleField(@Nullable Double val) {
        doWriteInt(val != null ? 9 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(DOUBLE);
            doWriteDouble(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeCharField(@Nullable Character val) {
        doWriteInt(val != null ? 3 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(CHAR);
            doWriteChar(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeBooleanField(@Nullable Boolean val) {
        doWriteInt(val != null ? 2 : 1);

        if (val == null)
            doWriteByte(NULL);
        else {
            doWriteByte(BOOLEAN);
            doWriteBoolean(val);
        }
    }

    /**
     * @param val Value.
     */
    void writeDecimalField(@Nullable BigDecimal val) {
        int lenPos = reserveAndMark(4);

        doWriteDecimal(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeStringField(@Nullable String val) {
        int lenPos = reserveAndMark(4);

        doWriteString(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeUuidField(@Nullable UUID val) {
        doWriteInt(val != null ? 17 : 1);
        doWriteUuid(val);
    }

    /**
     * @param val Value.
     */
    void writeDateField(@Nullable Date val) {
        doWriteInt(val != null ? 13 : 1);
        doWriteDate(val);
    }

    /**
     * @param val Value.
     */
    void writeTimestampField(@Nullable Timestamp val) {
        doWriteInt(val != null ? 13 : 1);
        doWriteTimestamp(val);
    }

    /**
     * @param obj Object.
     * @throws PortableException In case of error.
     */
    void writeObjectField(@Nullable Object obj) throws PortableException {
        int lenPos = reserveAndMark(4);

        doWriteObject(obj, false);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeByteArrayField(@Nullable byte[] val) {
        int lenPos = reserveAndMark(4);

        doWriteByteArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeShortArrayField(@Nullable short[] val) {
        int lenPos = reserveAndMark(4);

        doWriteShortArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeIntArrayField(@Nullable int[] val) {
        int lenPos = reserveAndMark(4);

        doWriteIntArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeLongArrayField(@Nullable long[] val) {
        int lenPos = reserveAndMark(4);

        doWriteLongArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeFloatArrayField(@Nullable float[] val) {
        int lenPos = reserveAndMark(4);

        doWriteFloatArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeDoubleArrayField(@Nullable double[] val) {
        int lenPos = reserveAndMark(4);

        doWriteDoubleArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeCharArrayField(@Nullable char[] val) {
        int lenPos = reserveAndMark(4);

        doWriteCharArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeBooleanArrayField(@Nullable boolean[] val) {
        int lenPos = reserveAndMark(4);

        doWriteBooleanArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeDecimalArrayField(@Nullable BigDecimal[] val) {
        int lenPos = reserveAndMark(4);

        doWriteDecimalArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeStringArrayField(@Nullable String[] val) {
        int lenPos = reserveAndMark(4);

        doWriteStringArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeUuidArrayField(@Nullable UUID[] val) {
        int lenPos = reserveAndMark(4);

        doWriteUuidArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeDateArrayField(@Nullable Date[] val) {
        int lenPos = reserveAndMark(4);

        doWriteDateArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     * @throws PortableException In case of error.
     */
    void writeObjectArrayField(@Nullable Object[] val) throws PortableException {
        int lenPos = reserveAndMark(4);

        doWriteObjectArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param col Collection.
     * @throws PortableException In case of error.
     */
    void writeCollectionField(@Nullable Collection<?> col) throws PortableException {
        int lenPos = reserveAndMark(4);

        doWriteCollection(col);

        writeDelta(lenPos);
    }

    /**
     * @param map Map.
     * @throws PortableException In case of error.
     */
    void writeMapField(@Nullable Map<?, ?> map) throws PortableException {
        int lenPos = reserveAndMark(4);

        doWriteMap(map);

        writeDelta(lenPos);
    }

    /**
     * @param e Map entry.
     * @throws PortableException In case of error.
     */
    void writeMapEntryField(@Nullable Map.Entry<?, ?> e) throws PortableException {
        int lenPos = reserveAndMark(4);

        doWriteMapEntry(e);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeEnumField(@Nullable Enum<?> val) {
        int lenPos = reserveAndMark(4);

        doWriteEnum(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeEnumArrayField(@Nullable Object[] val) {
        int lenPos = reserveAndMark(4);

        doWriteEnumArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param po Portable object.
     * @throws PortableException In case of error.
     */
    void writePortableObjectField(@Nullable PortableObjectImpl po) throws PortableException {
        int lenPos = reserveAndMark(4);

        doWritePortableObject(po);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws PortableException {
        writeFieldId(fieldName, BYTE);
        writeByteField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws PortableException {
        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws PortableException {
        writeFieldId(fieldName, SHORT);
        writeShortField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws PortableException {
        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws PortableException {
        writeFieldId(fieldName, INT);
        writeIntField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws PortableException {
        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws PortableException {
        writeFieldId(fieldName, LONG);
        writeLongField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws PortableException {
        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws PortableException {
        writeFieldId(fieldName, FLOAT);
        writeFloatField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws PortableException {
        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws PortableException {
        writeFieldId(fieldName, DOUBLE);
        writeDoubleField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws PortableException {
        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws PortableException {
        writeFieldId(fieldName, CHAR);
        writeCharField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws PortableException {
        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws PortableException {
        writeFieldId(fieldName, BOOLEAN);
        writeBooleanField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws PortableException {
        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws PortableException {
        writeFieldId(fieldName, DECIMAL);
        writeDecimalField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(@Nullable BigDecimal val) throws PortableException {
        doWriteDecimal(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws PortableException {
        writeFieldId(fieldName, STRING);
        writeStringField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws PortableException {
        doWriteString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws PortableException {
        writeFieldId(fieldName, UUID);
        writeUuidField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws PortableException {
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws PortableException {
        writeFieldId(fieldName, DATE);
        writeDateField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(@Nullable Date val) throws PortableException {
        doWriteDate(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws PortableException {
        writeFieldId(fieldName, DATE);
        writeTimestampField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(@Nullable Timestamp val) throws PortableException {
        doWriteTimestamp(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws PortableException {
        writeFieldId(fieldName, OBJ);
        writeObjectField(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(@Nullable Object obj) throws PortableException {
        doWriteObject(obj, false);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectDetached(@Nullable Object obj) throws PortableException {
        doWriteObject(obj, true);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws PortableException {
        writeFieldId(fieldName, BYTE_ARR);
        writeByteArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws PortableException {
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws PortableException {
        writeFieldId(fieldName, SHORT_ARR);
        writeShortArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws PortableException {
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws PortableException {
        writeFieldId(fieldName, INT_ARR);
        writeIntArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws PortableException {
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws PortableException {
        writeFieldId(fieldName, LONG_ARR);
        writeLongArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws PortableException {
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws PortableException {
        writeFieldId(fieldName, FLOAT_ARR);
        writeFloatArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws PortableException {
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws PortableException {
        writeFieldId(fieldName, DOUBLE_ARR);
        writeDoubleArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws PortableException {
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws PortableException {
        writeFieldId(fieldName, CHAR_ARR);
        writeCharArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws PortableException {
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws PortableException {
        writeFieldId(fieldName, BOOLEAN_ARR);
        writeBooleanArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws PortableException {
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val)
        throws PortableException {
        writeFieldId(fieldName, DECIMAL_ARR);
        writeDecimalArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(@Nullable BigDecimal[] val) throws PortableException {
        doWriteDecimalArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws PortableException {
        writeFieldId(fieldName, STRING_ARR);
        writeStringArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws PortableException {
        doWriteStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws PortableException {
        writeFieldId(fieldName, UUID_ARR);
        writeUuidArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws PortableException {
        doWriteUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws PortableException {
        writeFieldId(fieldName, DATE_ARR);
        writeDateArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(@Nullable Date[] val) throws PortableException {
        doWriteDateArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws PortableException {
        writeFieldId(fieldName, OBJ_ARR);
        writeObjectArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws PortableException {
        doWriteObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws PortableException {
        writeFieldId(fieldName, COL);
        writeCollectionField(col);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws PortableException {
        doWriteCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws PortableException {
        writeFieldId(fieldName, MAP);
        writeMapField(map);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws PortableException {
        doWriteMap(map);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws PortableException {
        writeFieldId(fieldName, ENUM);
        writeEnumField(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(T val) throws PortableException {
        doWriteEnum(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws PortableException {
        writeFieldId(fieldName, ENUM_ARR);
        writeEnumArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(T[] val) throws PortableException {
        doWriteEnumArray(val);
    }

    /** {@inheritDoc} */
    @Override public PortableRawWriter rawWriter() {
        if (allowFields) {
            wCtx.out.writeInt(start + RAW_DATA_OFF_POS, wCtx.out.position() - start);

            allowFields = false;
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public PortableOutputStream out() {
        return wCtx.out;
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeByte(s.charAt(i));
    }

    /** {@inheritDoc} */
    @Override public void writeChars(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeChar(s.charAt(i));
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(String s) throws IOException {
        writeString(s);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        doWriteByte((byte)v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        doWriteShort((short)v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        doWriteChar((char)v);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        doWriteByte((byte)b);
    }

    /** {@inheritDoc} */
    @Override public void flush() throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int reserveInt() {
        return reserve(LEN_INT);
    }

     /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) throws PortableException {
        wCtx.out.writeInt(pos, val);
    }

    /**
     * @param fieldName Field name.
     * @throws PortableException If fields are not allowed.
     */
    private void writeFieldId(String fieldName, byte fieldType) throws PortableException {
        A.notNull(fieldName, "fieldName");

        if (!allowFields)
            throw new PortableException("Individual field can't be written after raw writer is acquired " +
                "via rawWriter() method. Consider fixing serialization logic for class: " + cls.getName());

        int id = ctx.fieldId(typeId, fieldName);

        if (metaEnabled)
            metaHashSum = 31 * metaHashSum + (id + fieldType);

        doWriteInt(id);
    }

     /**
      * Attempts to write the object as a handle.
      *
      * @param obj Object to write.
      * @return {@code true} if the object has been written as a handle.
      */
     boolean tryWriteAsHandle(Object obj) {
         int handle = handle(obj);

         if (handle >= 0) {
             doWriteByte(GridPortableMarshaller.HANDLE);
             doWriteInt(handle);

             return true;
         }

         return false;
     }

    /**
     * Create new writer with same context.
     * @param typeId type
     * @return New writer.
     */
    public PortableWriterExImpl newWriter(int typeId) {
        PortableWriterExImpl res = new PortableWriterExImpl(ctx, wCtx);

        res.typeId = typeId;

        return res;
    }

    /**
     * @return Portable context.
     */
    public PortableContext context() {
        return ctx;
    }

    /** */
    private static class WriterContext {
        /** */
        private Map<Object, Integer> handles = new IdentityHashMap<>();

        /** Output stream. */
        private PortableOutputStream out;

        /**
         * Constructor.
         *
         * @param out Output stream.
         * @param handles Handles.
         */
        private WriterContext(PortableOutputStream out, Map<Object, Integer> handles) {
            this.out = out;
            this.handles = handles == null ? new IdentityHashMap<Object, Integer>() : handles;
        }

        /**
         * @param obj Object.
         * @return Handle.
         */
        private int handle(Object obj) {
            assert obj != null;

            Integer h = handles.get(obj);

            if (h != null)
                return out.position() - h;
            else {
                handles.put(obj, out.position());

                return -1;
            }
        }

        /**
         *
         */
        private void resetHandles() {
            handles = new IdentityHashMap<>();
        }
    }
}
