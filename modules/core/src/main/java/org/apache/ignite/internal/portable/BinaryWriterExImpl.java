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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.internal.portable.streams.PortableHeapOutputStream;
import org.apache.ignite.internal.portable.streams.PortableOutputStream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryWriter;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;

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
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLAGS_POS;
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
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SCHEMA_ID_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TOTAL_LEN_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID_ARR;

/**
 * Portable writer implementation.
 */
public class BinaryWriterExImpl implements BinaryWriter, BinaryRawWriterEx, ObjectOutput {
    /** Length: integer. */
    private static final int LEN_INT = 4;

    /** Initial capacity. */
    private static final int INIT_CAP = 1024;

    /** Maximum offset which fits in 1 byte. */
    private static final int MAX_OFFSET_1 = 1 << 8;

    /** Maximum offset which fits in 2 bytes. */
    private static final int MAX_OFFSET_2 = 1 << 16;

    /** Thread-local schema. */
    private static final ThreadLocal<SchemaHolder> SCHEMA = new ThreadLocal<>();

    /** */
    private final PortableContext ctx;

    /** */
    private final int start;

    /** */
    private Class<?> cls;

    /** */
    private int typeId;

    /** Raw offset position. */
    private int rawOffPos;

    /** */
    private boolean metaEnabled;

    /** */
    private int metaHashSum;

    /** Handles. */
    private Map<Object, Integer> handles;

    /** Output stream. */
    private PortableOutputStream out;

    /** Schema. */
    private SchemaHolder schema;

    /** Schema ID. */
    private int schemaId = PortableUtils.schemaInitialId();

    /** Amount of written fields. */
    private int fieldCnt;

    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /**
     * @param ctx Context.
     */
    BinaryWriterExImpl(PortableContext ctx) {
        this(ctx, new PortableHeapOutputStream(INIT_CAP));
    }

    /**
     * @param ctx Context.
     * @param out Output stream.
     */
    BinaryWriterExImpl(PortableContext ctx, PortableOutputStream out) {
        this(ctx, out, new IdentityHashMap<Object, Integer>());
    }

     /**
      * @param ctx Context.
      * @param out Output stream.
      * @param handles Handles.
      */
     private BinaryWriterExImpl(PortableContext ctx, PortableOutputStream out, Map<Object, Integer> handles) {
         this.ctx = ctx;
         this.out = out;
         this.handles = handles;

         start = out.position();
     }

    /**
     * @param ctx Context.
     * @param typeId Type ID.
     */
    public BinaryWriterExImpl(PortableContext ctx, int typeId, boolean metaEnabled) {
        this(ctx);

        this.typeId = typeId;
        this.metaEnabled = metaEnabled;
    }

    /**
     * Close the writer releasing resources if necessary.
     */
    @Override public void close() {
        out.close();
    }

    /**
     * @return Meta data hash sum or {@code null} if meta data is disabled.
     */
    @Nullable Integer metaDataHashSum() {
        return metaEnabled ? metaHashSum : null;
    }

    /**
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void marshal(Object obj) throws BinaryObjectException {
        marshal(obj, true);
    }

    /**
     * @param obj Object.
     * @param enableReplace Object replacing enabled flag.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void marshal(Object obj, boolean enableReplace) throws BinaryObjectException {
        assert obj != null;

        cls = obj.getClass();

        PortableClassDescriptor desc = ctx.descriptorForClass(cls);

        if (desc == null)
            throw new BinaryObjectException("Object is not portable: [class=" + cls + ']');

        if (desc.excluded()) {
            doWriteByte(NULL);
            return;
        }

        if (desc.useOptimizedMarshaller()) {
            writeByte(OPTM_MARSH);

            try {
                byte[] arr = ctx.optimizedMarsh().marshal(obj);

                writeInt(arr.length);

                write(arr);
            }
            catch (IgniteCheckedException e) {
                throw new BinaryObjectException("Failed to marshal object with optimized marshaller: " + obj, e);
            }

            return;
        }

        if (enableReplace && desc.getWriteReplaceMethod() != null) {
            Object replacedObj;

            try {
                replacedObj = desc.getWriteReplaceMethod().invoke(obj);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof BinaryObjectException)
                    throw (BinaryObjectException)e.getTargetException();

                throw new BinaryObjectException("Failed to execute writeReplace() method on " + obj, e);
            }

            if (replacedObj == null) {
                doWriteByte(NULL);
                return;
            }

            marshal(replacedObj, false);

            return;
        }

        typeId = desc.typeId();
        metaEnabled = desc.userType();

        desc.write(obj, this);
    }

    /**
     * @param obj Object.
     * @return Handle.
     */
    int handle(Object obj) {
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
     * @return Array.
     */
    public byte[] array() {
        return out.arrayCopy();
    }

    /**
     * @return Stream current position.
     */
    int position() {
        return out.position();
    }

    /**
     * Sets new position.
     *
     * @param pos Position.
     */
    void position(int pos) {
        out.position(pos);
    }

    /**
     * @param bytes Number of bytes to reserve.
     * @return Offset.
     */
    public int reserve(int bytes) {
        int pos = out.position();

        out.position(pos + bytes);

        return pos;
    }

    /**
     * Perform post-write activity. This includes:
     * - writing flags;
     * - writing object length;
     * - writing schema offset;
     * - writing schema to the tail.
     *
     * @param userType User type flag.
     */
    public void postWrite(boolean userType) {
        short flags = userType ? PortableUtils.FLAG_USR_TYP : 0;

        if (ctx.isCompactFooter())
            flags |= PortableUtils.FLAG_COMPACT_FOOTER;

        if (schema != null) {
            // Write schema ID.
            out.writeInt(start + SCHEMA_ID_POS, schemaId);

            // Write schema offset.
            out.writeInt(start + SCHEMA_OR_RAW_OFF_POS, out.position() - start);

            // Write the schema.
            int offsetByteCnt = schema.write(this, fieldCnt, ctx.isCompactFooter());

            // Write raw offset if needed.
            if (rawOffPos != 0)
                out.writeInt(rawOffPos - start);

            if (offsetByteCnt == PortableUtils.OFFSET_1)
                flags |= PortableUtils.FLAG_OFFSET_ONE_BYTE;
            else if (offsetByteCnt == PortableUtils.OFFSET_2)
                flags |= PortableUtils.FLAG_OFFSET_TWO_BYTES;
        }
        else {
            // Write raw-only flag is needed.
            flags |= PortableUtils.FLAG_RAW_ONLY;

            // If there are no schema, we are free to write raw offset to schema offset.
            out.writeInt(start + SCHEMA_OR_RAW_OFF_POS, (rawOffPos == 0 ? out.position() : rawOffPos) - start);
        }

        // Write flags as we know them at this point.
        out.writeShort(start + FLAGS_POS, flags);

        // Write length.
        out.writeInt(start + TOTAL_LEN_POS, out.position() - start);
    }

    /**
     * Pop schema.
     */
    public void popSchema() {
        if (schema != null) {
            assert fieldCnt > 0;

            schema.pop(fieldCnt);
        }
    }

    /**
     * @param val Byte array.
     */
    public void write(byte[] val) {
        assert val != null;

        out.writeByteArray(val);
    }

    /**
     * @param val Byte array.
     * @param off Offset.
     * @param len Length.
     */
    public void write(byte[] val, int off, int len) {
        assert val != null;

        out.write(val, off, len);
    }

    /**
     * @param val Value.
     */
    public void doWriteByte(byte val) {
        out.writeByte(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteShort(short val) {
        out.writeShort(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteInt(int val) {
        out.writeInt(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteLong(long val) {
        out.writeLong(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteFloat(float val) {
        out.writeFloat(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteDouble(double val) {
        out.writeDouble(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteChar(char val) {
        out.writeChar(val);
    }

    /**
     * @param val Value.
     */
    public void doWriteBoolean(boolean val) {
        out.writeBoolean(val);
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

                out.writeInt(val.scale() | 0x80000000);
            }
            else
                out.writeInt(val.scale());

            byte[] vals = intVal.toByteArray();

            out.writeInt(vals.length);
            out.writeByteArray(vals);
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

            byte[] strArr = val.getBytes(UTF_8);

            doWriteInt(strArr.length);

            out.writeByteArray(strArr);
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
        }
    }

    /**
     * @param ts Timestamp.
     */
    public void doWriteTimestamp(@Nullable Timestamp ts) {
        if (ts== null)
            doWriteByte(NULL);
        else {
            doWriteByte(TIMESTAMP);
            doWriteLong(ts.getTime());
            doWriteInt(ts.getNanos() % 1000000);
        }
    }

    /**
     * Write object.
     *
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public void doWriteObject(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            doWriteByte(NULL);
        else {
            BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, out, handles);

            writer.marshal(obj);
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

            out.writeByteArray(val);
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

            out.writeShortArray(val);
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

            out.writeIntArray(val);
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

            out.writeLongArray(val);
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

            out.writeFloatArray(val);
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

            out.writeDoubleArray(val);
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

            out.writeCharArray(val);
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

            out.writeBooleanArray(val);
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
      * @param val Array of timestamps.
      */
     void doWriteTimestampArray(@Nullable Timestamp[] val) {
         if (val == null)
             doWriteByte(NULL);
         else {
             if (tryWriteAsHandle(val))
                 return;

             doWriteByte(TIMESTAMP_ARR);
             doWriteInt(val.length);

             for (Timestamp ts : val)
                 doWriteTimestamp(ts);
         }
     }

    /**
     * @param val Array of objects.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteObjectArray(@Nullable Object[] val) throws BinaryObjectException {
        if (val == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            PortableClassDescriptor desc = ctx.descriptorForClass(val.getClass().getComponentType());

            doWriteByte(OBJ_ARR);

            if (desc.registered())
                doWriteInt(desc.typeId());
            else {
                doWriteInt(UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getComponentType().getName());
            }

            doWriteInt(val.length);

            for (Object obj : val)
                doWriteObject(obj);
        }
    }

    /**
     * @param col Collection.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteCollection(@Nullable Collection<?> col) throws BinaryObjectException {
        if (col == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(col))
                return;

            doWriteByte(COL);
            doWriteInt(col.size());
            doWriteByte(ctx.collectionType(col.getClass()));

            for (Object obj : col)
                doWriteObject(obj);
        }
    }

    /**
     * @param map Map.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteMap(@Nullable Map<?, ?> map) throws BinaryObjectException {
        if (map == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(map))
                return;

            doWriteByte(MAP);
            doWriteInt(map.size());
            doWriteByte(ctx.mapType(map.getClass()));

            for (Map.Entry<?, ?> e : map.entrySet()) {
                doWriteObject(e.getKey());
                doWriteObject(e.getValue());
            }
        }
    }

    /**
     * @param e Map entry.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteMapEntry(@Nullable Map.Entry<?, ?> e) throws BinaryObjectException {
        if (e == null)
            doWriteByte(NULL);
        else {
            if (tryWriteAsHandle(e))
                return;

            doWriteByte(MAP_ENTRY);
            doWriteObject(e.getKey());
            doWriteObject(e.getValue());
        }
    }

    /**
     * @param val Value.
     */
    void doWriteEnum(@Nullable Enum<?> val) {
        if (val == null)
            doWriteByte(NULL);
        else {
            PortableClassDescriptor desc = ctx.descriptorForClass(val.getClass());

            doWriteByte(ENUM);

            if (desc.registered())
                doWriteInt(desc.typeId());
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
            PortableClassDescriptor desc = ctx.descriptorForClass(val.getClass().getComponentType());
            doWriteByte(ENUM_ARR);

            if (desc.registered())
                doWriteInt(desc.typeId());
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
            PortableClassDescriptor desc = ctx.descriptorForClass(val);

            doWriteByte(CLASS);

            if (desc.registered())
                doWriteInt(desc.typeId());
            else {
                doWriteInt(UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getName());
            }
        }
    }

    /**
     * @param po Portable object.
     */
    public void doWritePortableObject(@Nullable BinaryObjectImpl po) {
        if (po == null)
            doWriteByte(NULL);
        else {
            doWriteByte(PORTABLE_OBJ);

            byte[] poArr = po.array();

            doWriteInt(poArr.length);

            out.writeByteArray(poArr);

            doWriteInt(po.start());
        }
    }

    /**
     * @param val Value.
     */
    void writeByteField(@Nullable Byte val) {
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
        doWriteClass(val);
    }

    /**
     * @param val Value.
     */
    void writeShortField(@Nullable Short val) {
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
        doWriteDecimal(val);
    }

    /**
     * @param val Value.
     */
    void writeStringField(@Nullable String val) {
        doWriteString(val);
    }

    /**
     * @param val Value.
     */
    void writeUuidField(@Nullable UUID val) {
        doWriteUuid(val);
    }

    /**
     * @param val Value.
     */
    void writeDateField(@Nullable Date val) {
        doWriteDate(val);
    }

    /**
     * @param val Value.
     */
    void writeTimestampField(@Nullable Timestamp val) {
        doWriteTimestamp(val);
    }

    /**
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeObjectField(@Nullable Object obj) throws BinaryObjectException {
        doWriteObject(obj);
    }

    /**
     * @param val Value.
     */
    void writeByteArrayField(@Nullable byte[] val) {
        doWriteByteArray(val);
    }

    /**
     * @param val Value.
     */
    void writeShortArrayField(@Nullable short[] val) {
        doWriteShortArray(val);
    }

    /**
     * @param val Value.
     */
    void writeIntArrayField(@Nullable int[] val) {
        doWriteIntArray(val);
    }

    /**
     * @param val Value.
     */
    void writeLongArrayField(@Nullable long[] val) {
        doWriteLongArray(val);
    }

    /**
     * @param val Value.
     */
    void writeFloatArrayField(@Nullable float[] val) {
        doWriteFloatArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDoubleArrayField(@Nullable double[] val) {
        doWriteDoubleArray(val);
    }

    /**
     * @param val Value.
     */
    void writeCharArrayField(@Nullable char[] val) {
        doWriteCharArray(val);
    }

    /**
     * @param val Value.
     */
    void writeBooleanArrayField(@Nullable boolean[] val) {
        doWriteBooleanArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDecimalArrayField(@Nullable BigDecimal[] val) {
        doWriteDecimalArray(val);
    }

    /**
     * @param val Value.
     */
    void writeStringArrayField(@Nullable String[] val) {
        doWriteStringArray(val);
    }

    /**
     * @param val Value.
     */
    void writeUuidArrayField(@Nullable UUID[] val) {
        doWriteUuidArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDateArrayField(@Nullable Date[] val) {
        doWriteDateArray(val);
    }

    /**
     * @param val Value.
     */
    void writeTimestampArrayField(@Nullable Timestamp[] val) {
        doWriteTimestampArray(val);
    }

    /**
     * @param val Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeObjectArrayField(@Nullable Object[] val) throws BinaryObjectException {
        doWriteObjectArray(val);
    }

    /**
     * @param col Collection.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeCollectionField(@Nullable Collection<?> col) throws BinaryObjectException {
        doWriteCollection(col);
    }

    /**
     * @param map Map.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeMapField(@Nullable Map<?, ?> map) throws BinaryObjectException {
        doWriteMap(map);
    }

    /**
     * @param e Map entry.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeMapEntryField(@Nullable Map.Entry<?, ?> e) throws BinaryObjectException {
        doWriteMapEntry(e);
    }

    /**
     * @param val Value.
     */
    void writeEnumField(@Nullable Enum<?> val) {
        doWriteEnum(val);
    }

    /**
     * @param val Value.
     */
    void writeEnumArrayField(@Nullable Object[] val) {
        doWriteEnumArray(val);
    }

    /**
     * @param po Portable object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writePortableObjectField(@Nullable BinaryObjectImpl po) throws BinaryObjectException {
        doWritePortableObject(po);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws BinaryObjectException {
        writeFieldId(fieldName, BYTE);
        writeByteField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws BinaryObjectException {
        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws BinaryObjectException {
        writeFieldId(fieldName, SHORT);
        writeShortField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws BinaryObjectException {
        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws BinaryObjectException {
        writeFieldId(fieldName, INT);
        writeIntField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws BinaryObjectException {
        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws BinaryObjectException {
        writeFieldId(fieldName, LONG);
        writeLongField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws BinaryObjectException {
        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws BinaryObjectException {
        writeFieldId(fieldName, FLOAT);
        writeFloatField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws BinaryObjectException {
        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws BinaryObjectException {
        writeFieldId(fieldName, DOUBLE);
        writeDoubleField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws BinaryObjectException {
        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws BinaryObjectException {
        writeFieldId(fieldName, CHAR);
        writeCharField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws BinaryObjectException {
        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws BinaryObjectException {
        writeFieldId(fieldName, BOOLEAN);
        writeBooleanField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws BinaryObjectException {
        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws BinaryObjectException {
        writeFieldId(fieldName, DECIMAL);
        writeDecimalField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(@Nullable BigDecimal val) throws BinaryObjectException {
        doWriteDecimal(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws BinaryObjectException {
        writeFieldId(fieldName, STRING);
        writeStringField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws BinaryObjectException {
        doWriteString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws BinaryObjectException {
        writeFieldId(fieldName, UUID);
        writeUuidField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws BinaryObjectException {
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws BinaryObjectException {
        writeFieldId(fieldName, DATE);
        writeDateField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(@Nullable Date val) throws BinaryObjectException {
        doWriteDate(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws BinaryObjectException {
        writeFieldId(fieldName, TIMESTAMP);
        writeTimestampField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(@Nullable Timestamp val) throws BinaryObjectException {
        doWriteTimestamp(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws BinaryObjectException {
        writeFieldId(fieldName, OBJ);
        writeObjectField(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(@Nullable Object obj) throws BinaryObjectException {
        doWriteObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectDetached(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            doWriteByte(NULL);
        else {
            BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, out, new IdentityHashMap<Object, Integer>());

            writer.marshal(obj);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws BinaryObjectException {
        writeFieldId(fieldName, BYTE_ARR);
        writeByteArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws BinaryObjectException {
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws BinaryObjectException {
        writeFieldId(fieldName, SHORT_ARR);
        writeShortArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws BinaryObjectException {
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws BinaryObjectException {
        writeFieldId(fieldName, INT_ARR);
        writeIntArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws BinaryObjectException {
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws BinaryObjectException {
        writeFieldId(fieldName, LONG_ARR);
        writeLongArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws BinaryObjectException {
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws BinaryObjectException {
        writeFieldId(fieldName, FLOAT_ARR);
        writeFloatArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws BinaryObjectException {
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName, DOUBLE_ARR);
        writeDoubleArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws BinaryObjectException {
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws BinaryObjectException {
        writeFieldId(fieldName, CHAR_ARR);
        writeCharArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws BinaryObjectException {
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName, BOOLEAN_ARR);
        writeBooleanArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws BinaryObjectException {
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName, DECIMAL_ARR);
        writeDecimalArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(@Nullable BigDecimal[] val) throws BinaryObjectException {
        doWriteDecimalArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName, STRING_ARR);
        writeStringArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws BinaryObjectException {
        doWriteStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws BinaryObjectException {
        writeFieldId(fieldName, UUID_ARR);
        writeUuidArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws BinaryObjectException {
        doWriteUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws BinaryObjectException {
        writeFieldId(fieldName, DATE_ARR);
        writeDateArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(@Nullable Date[] val) throws BinaryObjectException {
        doWriteDateArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(String fieldName, @Nullable Timestamp[] val) throws BinaryObjectException {
        writeFieldId(fieldName, TIMESTAMP_ARR);
        writeTimestampArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(@Nullable Timestamp[] val) throws BinaryObjectException {
        doWriteTimestampArray(val);
    }

     /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws BinaryObjectException {
        writeFieldId(fieldName, OBJ_ARR);
        writeObjectArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws BinaryObjectException {
        doWriteObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws BinaryObjectException {
        writeFieldId(fieldName, COL);
        writeCollectionField(col);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws BinaryObjectException {
        doWriteCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws BinaryObjectException {
        writeFieldId(fieldName, MAP);
        writeMapField(map);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws BinaryObjectException {
        doWriteMap(map);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws BinaryObjectException {
        writeFieldId(fieldName, ENUM);
        writeEnumField(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(T val) throws BinaryObjectException {
        doWriteEnum(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws BinaryObjectException {
        writeFieldId(fieldName, ENUM_ARR);
        writeEnumArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(T[] val) throws BinaryObjectException {
        doWriteEnumArray(val);
    }

    /** {@inheritDoc} */
    @Override public BinaryRawWriter rawWriter() {
        if (rawOffPos == 0)
            rawOffPos = out.position();

        return this;
    }

    /** {@inheritDoc} */
    @Override public PortableOutputStream out() {
        return out;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void writeBytes(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeByte(s.charAt(i));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void writeChars(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeChar(s.charAt(i));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void writeUTF(String s) throws IOException {
        writeString(s);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        doWriteByte((byte) v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        doWriteShort((short) v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        doWriteChar((char) v);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        doWriteByte((byte) b);
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
    @Override public void writeInt(int pos, int val) throws BinaryObjectException {
        out.writeInt(pos, val);
    }

    /**
     * @param fieldName Field name.
     * @throws org.apache.ignite.binary.BinaryObjectException If fields are not allowed.
     */
    private void writeFieldId(String fieldName, byte fieldType) throws BinaryObjectException {
        A.notNull(fieldName, "fieldName");

        if (rawOffPos != 0)
            throw new BinaryObjectException("Individual field can't be written after raw writer is acquired " +
                "via rawWriter() method. Consider fixing serialization logic for class: " + cls.getName());

        if (idMapper == null)
            idMapper = ctx.userTypeIdMapper(typeId);

        int id = idMapper.fieldId(typeId, fieldName);

        writeFieldId(id);

        if (metaEnabled)
            metaHashSum = 31 * metaHashSum + (id + fieldType);
    }

    /**
     * Write field ID.
     * @param fieldId Field ID.
     */
    public void writeFieldId(int fieldId) {
        int fieldOff = out.position() - start;

        if (schema == null) {
            schema = SCHEMA.get();

            if (schema == null) {
                schema = new SchemaHolder();

                SCHEMA.set(schema);
            }
        }

        schemaId = PortableUtils.updateSchemaId(schemaId, fieldId);

        schema.push(fieldId, fieldOff);

        fieldCnt++;
    }

    /**
     * @return Current writer's schema.
     */
    public PortableSchema currentSchema() {
        PortableSchema.Builder builder = PortableSchema.Builder.newBuilder();

        if (schema != null)
            schema.build(builder, fieldCnt);

        return builder.build();
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
     *
     * @param typeId type
     * @return New writer.
     */
    public BinaryWriterExImpl newWriter(int typeId) {
        BinaryWriterExImpl res = new BinaryWriterExImpl(ctx, out, handles);

        res.typeId = typeId;

        return res;
    }

    /**
     * @return Portable context.
     */
    public PortableContext context() {
        return ctx;
    }

    /**
     * Schema holder.
     */
    private static class SchemaHolder {
        /** Grow step. */
        private static final int GROW_STEP = 64;

        /** Maximum stable size. */
        private static final int MAX_SIZE = 1024;

        /** Data. */
        private int[] data;

        /** Index. */
        private int idx;

        /**
         * Constructor.
         */
        public SchemaHolder() {
            data = new int[GROW_STEP];
        }

        /**
         * Push another frame.
         *
         * @param id Field ID.
         * @param off Field offset.
         */
        public void push(int id, int off) {
            if (idx == data.length) {
                int[] data0 = new int[data.length + GROW_STEP];

                System.arraycopy(data, 0, data0, 0, data.length);

                data = data0;
            }

            data[idx] = id;
            data[idx + 1] = off;

            idx += 2;
        }

        /**
         * Build the schema.
         *
         * @param builder Builder.
         * @param fieldCnt Fields count.
         */
        public void build(PortableSchema.Builder builder, int fieldCnt) {
            for (int curIdx = idx - fieldCnt * 2; curIdx < idx; curIdx += 2)
                builder.addField(data[curIdx]);
        }

        /**
         * Write collected frames and pop them.
         *
         * @param writer Writer.
         * @param fieldCnt Count.
         * @param compactFooter Whether footer should be written in compact form.
         * @return Amount of bytes dedicated to each field offset. Could be 1, 2 or 4.
         */
        public int write(BinaryWriterExImpl writer, int fieldCnt, boolean compactFooter) {
            int startIdx = idx - fieldCnt * 2;

            assert startIdx >= 0;

            int lastOffset = data[idx - 1];

            int res;

            if (compactFooter) {
                if (lastOffset < MAX_OFFSET_1) {
                    for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2)
                        writer.writeByte((byte)data[curIdx]);

                    res = PortableUtils.OFFSET_1;
                }
                else if (lastOffset < MAX_OFFSET_2) {
                    for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2)
                        writer.writeShort((short)data[curIdx]);

                    res = PortableUtils.OFFSET_2;
                }
                else {
                    for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2)
                        writer.writeInt(data[curIdx]);

                    res = PortableUtils.OFFSET_4;
                }
            }
            else {
                if (lastOffset < MAX_OFFSET_1) {
                    for (int curIdx = startIdx; curIdx < idx;) {
                        writer.writeInt(data[curIdx++]);
                        writer.writeByte((byte) data[curIdx++]);
                    }

                    res = PortableUtils.OFFSET_1;
                }
                else if (lastOffset < MAX_OFFSET_2) {
                    for (int curIdx = startIdx; curIdx < idx;) {
                        writer.writeInt(data[curIdx++]);
                        writer.writeShort((short)data[curIdx++]);
                    }

                    res = PortableUtils.OFFSET_2;
                }
                else {
                    for (int curIdx = startIdx; curIdx < idx;) {
                        writer.writeInt(data[curIdx++]);
                        writer.writeInt(data[curIdx++]);
                    }

                    res = PortableUtils.OFFSET_4;
                }
            }

            return res;
        }

        /**
         * Pop current object's frame.
         */
        public void pop(int fieldCnt) {
            idx = idx - fieldCnt * 2;

            // Shrink data array if needed.
            if (idx == 0 && data.length > MAX_SIZE)
                data = new int[MAX_SIZE];
        }
    }
}
