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

package org.apache.ignite.internal.schema.marshaller.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;

/**
 * Serialize SchemaDescriptor object to byte array and vice versa.
 */
public class SchemaSerializerImpl extends AbstractSchemaSerializer {
    /** Instance. */
    public static final AbstractSchemaSerializer INSTANCE = new SchemaSerializerImpl();

    /** String array length. */
    private static final int STRING_HEADER = 4;

    /** Array length. */
    private static final int ARRAY_HEADER_LENGTH = 4;

    /** Byte. */
    private static final int BYTE = 1;

    /** Short. */
    private static final int SHORT = 2;

    /** Int. */
    private static final int INT = 4;

    /** Long. */
    private static final int LONG = 8;

    /** Float. */
    private static final int FLOAT = 4;

    /** Double. */
    private static final int DOUBLE = 8;

    /** Schema version. */
    private static final short SCHEMA_VER = 1;

    /**
     * Default constructor.
     */
    public SchemaSerializerImpl() {
        super(SCHEMA_VER);
    }

    /** {@inheritDoc} */
    @Override public void writeTo(SchemaDescriptor desc, ByteBuffer byteBuf) {
        byteBuf.putShort(SCHEMA_VER);
        byteBuf.putInt(desc.version());

        appendColumns(desc.keyColumns(), byteBuf);
        appendColumns(desc.valueColumns(), byteBuf);

        Column[] affinityCols = desc.affinityColumns();

        byteBuf.putInt(affinityCols.length);

        for (Column column : affinityCols)
            appendString(column.name(), byteBuf);

        appendColumnMapping(desc.columnMapping(), desc.length(), byteBuf);
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor readFrom(ByteBuffer byteBuf) {
        int ver = byteBuf.getInt();

        Column[] keyCols = readColumns(byteBuf);
        Column[] valCols = readColumns(byteBuf);

        int affinityColsSize = byteBuf.getInt();

        String[] affinityCols = new String[affinityColsSize];

        for (int i = 0; i < affinityColsSize; i++)
            affinityCols[i] = readString(byteBuf);

        SchemaDescriptor descriptor = new SchemaDescriptor(ver, keyCols, affinityCols, valCols);

        ColumnMapper mapper = readColumnMapping(descriptor, byteBuf);

        descriptor.columnMapping(mapper);

        return descriptor;
    }

    /** {@inheritDoc} */
    @Override public int size(SchemaDescriptor desc) {
        return SHORT +                      //Assembler version
            INT +                          //Descriptor version
            getColumnsSize(desc.keyColumns()) +
            getColumnsSize(desc.valueColumns()) +
            ARRAY_HEADER_LENGTH +          //Affinity columns length
            getStringArraySize(desc.affinityColumns()) +
            getColumnMappingSize(desc.columnMapping(), desc.length());
    }

    /**
     * Gets column mapping size in bytes.
     *
     * @param len Column array length (both key and value columns).
     * @return Size of column mapping.
     */
    private int getColumnMappingSize(ColumnMapper mapper, int len) {
        int size = INT;

        for (int i = 0; i < len; i++) {
            if (mapper.map(i) != i) {
                size += INT;
                size += INT;

                if (mapper.map(i) == -1)
                    size += getColumnSize(mapper.mappedColumn(i));
            }
        }

        return size;
    }

    /**
     * Gets column names array size in bytes.
     *
     * @param cols Column array.
     * @return Size of an array with column names.
     */
    private int getStringArraySize(Column[] cols) {
        int size = ARRAY_HEADER_LENGTH;      //String array size header
        for (Column column : cols)
            size += getStringSize(column.name());

        return size;
    }

    /**
     * Gets columns array size in bytes.
     *
     * @param cols Column array.
     * @return Size of column array, including column name and column native type.
     */
    private int getColumnsSize(Columns cols) {
        int size = ARRAY_HEADER_LENGTH; //cols array length

        for (Column column : cols.columns())
            size += getColumnSize(column);

        return size;
    }

    /**
     * Gets column size in bytes.
     *
     * @param col Column object.
     * @return Column size in bytes.
     */
    private int getColumnSize(Column col) {
        return INT +                      //Schema index
            BYTE +                         //nullable flag
            getStringSize(col.name()) +
            getNativeTypeSize(col.type()) +
            BYTE + getDefaultObjectSize(col.type(), col.defaultValue());
    }

    /**
     * Gets default object size in bytes based on object native type.
     *
     * @param type Column native type.
     * @param val Object.
     * @return Object size in bytes.
     */
    private int getDefaultObjectSize(NativeType type, Object val) {
        if (val == null)
            return 0;

        switch (type.spec()) {
            case INT8:
                return BYTE;

            case INT16:
                return SHORT;

            case INT32:
                return INT;

            case INT64:
                return LONG;

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case DECIMAL:
                return INT + INT + ((BigDecimal)val).unscaledValue().toByteArray().length;

            case UUID:
                return LONG + LONG;

            case STRING:
                return getStringSize(((String)val));

            case BYTES:
                return INT + ((byte[])val).length;

            case BITMASK:
                return INT + ((BitSet)val).toByteArray().length;

            case NUMBER:
                return INT + ((BigInteger)val).toByteArray().length;
        }

        return 0;
    }

    /**
     * Gets native type size in bytes.
     *
     * @param type Native type.
     * @return Native type size depending on NativeTypeSpec params.
     */
    private int getNativeTypeSize(NativeType type) {
        int typeSize = 0;

        switch (type.spec()) {
            case STRING:
            case BYTES:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case NUMBER:
            case BITMASK:
                typeSize += INT; //For precision, len or bits

                break;
            case DECIMAL:
                typeSize += INT; //For precision
                typeSize += INT; //For scale

                break;
            default:
                break;
        }

        return getStringSize(type.spec().name()) + //native type name
            typeSize;
    }

    /**
     * Gets string size in bytes.
     *
     * @param str String.
     * @return Byte array size.
     */
    private int getStringSize(String str) {
        return STRING_HEADER + //string byte array header
            str.getBytes().length; // string byte array length
    }

    /**
     * Appends column mapping to byte buffer.
     *
     * @param mapper ColumnMapper object.
     * @param len Column array length (both key and value columns).
     * @param buff Allocated ByteBuffer.
     */
    private void appendColumnMapping(ColumnMapper mapper, int len, ByteBuffer buff) {
        int mappingSize = 0;
        for (int i = 0; i < len; i++) {
            if (mapper.map(i) != i)
                mappingSize += 1;
        }

        buff.putInt(mappingSize);

        for (int i = 0; i < len; i++) {
            if (mapper.map(i) != i) {
                buff.putInt(i);
                buff.putInt(mapper.map(i));

                if (mapper.map(i) == -1)
                    appendColumn(mapper.mappedColumn(i), buff);
            }
        }
    }

    /**
     * Appends column array to byte buffer.
     *
     * @param buf Byte buffer.
     * @param cols Column array.
     */
    private void appendColumns(Columns cols, ByteBuffer buf) {
        Column[] colArr = cols.columns();

        buf.putInt(colArr.length);

        for (Column column : colArr)
            appendColumn(column, buf);
    }

    /**
     * Appends column to byte buffer.
     *
     * @param buf Byte buffer.
     * @param col Column.
     */
    private void appendColumn(Column col, ByteBuffer buf) {
        buf.putInt(col.schemaIndex());
        buf.put((byte)(col.nullable() ? 1 : 0));

        appendString(col.name(), buf);        
        appendNativeType(buf, col.type());

        appendDefaultValue(buf, col.type(), col.defaultValue());
    }

    /**
     * Appends default value object to byte buffer based on native type.
     *
     * @param buf Allocated ByteBuffer.
     * @param type Column native type.
     * @param val Default object value.
     */
    private void appendDefaultValue(ByteBuffer buf, NativeType type, Object val) {
        boolean isPresent = val != null;

        buf.put((byte)(isPresent ? 1 : 0));

        if (!isPresent)
            return;

        switch (type.spec()) {
            case INT8: {
                buf.put((byte)val);

                break;
            }
            case INT16: {
                buf.putShort((short)val);

                break;
            }
            case INT32: {
                buf.putInt((int)val);

                break;
            }
            case INT64: {
                buf.putLong((long)val);

                break;
            }
            case FLOAT: {
                buf.putFloat((float)val);

                break;
            }
            case DOUBLE: {
                buf.putDouble((double)val);

                break;
            }
            case DECIMAL: {
                BigDecimal decimal = (BigDecimal)val;

                buf.putInt(decimal.scale());
                appendByteArray(decimal.unscaledValue().toByteArray(), buf);

                break;
            }
            case UUID: {
                UUID uuid = (UUID)val;

                buf.putLong(uuid.getMostSignificantBits());
                buf.putLong(uuid.getLeastSignificantBits());

                break;
            }
            case STRING: {
                appendString((String)val, buf);

                break;
            }
            case BYTES: {
                appendByteArray((byte[])val, buf);

                break;
            }
            case BITMASK: {
                BitSet bitSet = (BitSet)val;
                appendByteArray(bitSet.toByteArray(), buf);

                break;
            }
            case NUMBER: {
                BigInteger bigInt = (BigInteger)val;

                appendByteArray(bigInt.toByteArray(), buf);

                break;
            }
        }
    }

    /**
     * Appends native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Native type.
     */
    private void appendNativeType(ByteBuffer buf, NativeType type) {
        appendString(type.spec().name(), buf);

        switch (type.spec()) {
            case STRING:
            case BYTES: {
                int len = ((VarlenNativeType)type).length();

                buf.putInt(len);

                break;
            }
            case BITMASK: {
                int bits = ((BitmaskNativeType)type).bits();

                buf.putInt(bits);

                break;
            }
            case DECIMAL: {
                int precision = ((DecimalNativeType)type).precision();
                int scale = ((DecimalNativeType)type).scale();

                buf.putInt(precision);
                buf.putInt(scale);

                break;
            }
            case TIME:
            case DATETIME:
            case TIMESTAMP: {
                int precision = ((TemporalNativeType)type).precision();

                buf.putInt(precision);

                break;
            }
            case NUMBER: {
                int precision = ((NumberNativeType)type).precision();

                buf.putInt(precision);

                break;
            }
            default:
                break;
        }
    }

    /**
     * Appends string byte representation to byte buffer.
     *
     * @param buf Byte buffer.
     * @param str String.
     */
    private void appendString(String str, ByteBuffer buf) {
        appendByteArray(str.getBytes(), buf);
    }

    /**
     * Appends byte array to byte buffer.
     *
     * @param buf Byte buffer.
     * @param bytes Byte array.
     */
    private void appendByteArray(byte[] bytes, ByteBuffer buf) {
        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    /**
     * Reads column mapping from byte buffer.
     *
     * @param desc SchemaDescriptor.
     * @param buf Byte buffer.
     * @return ColumnMapper object.
     */
    private ColumnMapper readColumnMapping(SchemaDescriptor desc, ByteBuffer buf) {
        int mappingSize = buf.getInt();

        if (mappingSize == 0)
            return ColumnMapping.identityMapping();

        ColumnMapper mapper = ColumnMapping.createMapper(desc);

        for (int i = 0; i < mappingSize; i++) {
            int from = buf.getInt();
            int to = buf.getInt();

            if (to == -1) {
                Column col = readColumn(buf);
                mapper.add(col);
            } else
                mapper.add(from, to);
        }

        return mapper;
    }

    /**
     * Reads column array from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Column array.
     */
    private Column[] readColumns(ByteBuffer buf) {
        int size = buf.getInt();

        Column[] colArr = new Column[size];

        for (int i = 0; i < size; i++)
            colArr[i] = readColumn(buf);

        return colArr;
    }

    /**
     * Reads column from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Column.
     */
    private Column readColumn(ByteBuffer buf) {
        int schemaIdx = buf.getInt();
        boolean nullable = buf.get() == 1;
        String name = readString(buf);

        NativeType nativeType = fromByteBuffer(buf);

        Object object = readDefaultValue(buf, nativeType);

        return new Column(name, nativeType, nullable, () -> object).copy(schemaIdx);
    }

    /**
     * Reads default value object or null.
     *
     * @param buf ByteBuffer.
     * @param type Column native type.
     * @return Column default value.
     */
    private Object readDefaultValue(ByteBuffer buf, NativeType type) {

        boolean isPresent = buf.get() == 1;

        if (!isPresent)
            return null;

        switch (type.spec()) {
            case INT8:
                return buf.get();

            case INT16:
                return buf.getShort();

            case INT32:
                return buf.getInt();

            case INT64:
                return buf.getLong();

            case FLOAT:
                return buf.getFloat();

            case DOUBLE:
                return buf.getDouble();

            case DECIMAL: {
                int scale = buf.getInt();
                byte[] bytes = readByteArray(buf);

                return new BigDecimal(new BigInteger(bytes), scale);
            }
            case UUID:
                return new UUID(buf.getLong(), buf.getLong());

            case STRING:
                return readString(buf);

            case BYTES:
                return readByteArray(buf);

            case BITMASK:
                return BitSet.valueOf(readByteArray(buf));

            case NUMBER:
                return new BigInteger(readByteArray(buf));
        }

        return null;
    }

    /**
     * Reads native type from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Native type.
     */
    private NativeType fromByteBuffer(ByteBuffer buf) {
        String nativeTypeSpecName = readString(buf);

        NativeTypeSpec spec = NativeTypeSpec.valueOf(nativeTypeSpecName);

        switch (spec) {
            case STRING:
                int strLen = buf.getInt();

                return NativeTypes.stringOf(strLen);

            case BYTES:
                int len = buf.getInt();

                return NativeTypes.blobOf(len);

            case BITMASK:
                int bits = buf.getInt();

                return NativeTypes.bitmaskOf(bits);

            case DECIMAL: {
                int precision = buf.getInt();
                int scale = buf.getInt();

                return NativeTypes.decimalOf(precision, scale);
            }
            case TIME: {
                int precision = buf.getInt();

                return NativeTypes.time(precision);
            }
            case DATETIME: {
                int precision = buf.getInt();

                return NativeTypes.datetime(precision);
            }
            case TIMESTAMP: {
                int precision = buf.getInt();

                return NativeTypes.timestamp(precision);
            }
            case NUMBER: {
                int precision = buf.getInt();

                return NativeTypes.numberOf(precision);
            }
            case INT8:
                return NativeTypes.INT8;

            case INT16:
                return NativeTypes.INT16;

            case INT32:
                return NativeTypes.INT32;

            case INT64:
                return NativeTypes.INT64;

            case FLOAT:
                return NativeTypes.FLOAT;

            case DOUBLE:
                return NativeTypes.DOUBLE;

            case UUID:
                return NativeTypes.UUID;

            case DATE:
                return NativeTypes.DATE;
        }

        throw new InvalidTypeException("Unexpected type " + spec);
    }

    /**
     * Reads string from byte buffer.
     *
     * @param buf Byte buffer.
     * @return String.
     */
    private String readString(ByteBuffer buf) {
        return new String(readByteArray(buf));
    }

    /**
     * Reads byte array from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Byte array.
     */
    private byte[] readByteArray(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] arr = new byte[len];

        buf.get(arr);

        return arr;
    }
}
