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

package org.apache.ignite.internal.schema;

import java.util.BitSet;
import org.apache.ignite.schema.ColumnType;

/**
 * A thin wrapper over {@link NativeTypeSpec} to instantiate parameterized constrained types.
 */
public class NativeTypes {
    /** */
    public static final NativeType BYTE = new NativeType(NativeTypeSpec.BYTE, 1);

    /** */
    public static final NativeType SHORT = new NativeType(NativeTypeSpec.SHORT, 2);

    /** */
    public static final NativeType INTEGER = new NativeType(NativeTypeSpec.INTEGER, 4);

    /** */
    public static final NativeType LONG = new NativeType(NativeTypeSpec.LONG, 8);

    /** */
    public static final NativeType FLOAT = new NativeType(NativeTypeSpec.FLOAT, 4);

    /** */
    public static final NativeType DOUBLE = new NativeType(NativeTypeSpec.DOUBLE, 8);

    /** */
    public static final NativeType UUID = new NativeType(NativeTypeSpec.UUID, 16);

    /** */
    public static final NativeType STRING = new VarlenNativeType(NativeTypeSpec.STRING, Integer.MAX_VALUE);

    /** */
    public static final NativeType BYTES = new VarlenNativeType(NativeTypeSpec.BYTES, Integer.MAX_VALUE);

    /** Don't allow to create an instance. */
    private NativeTypes() {
    }

    /**
     * Creates a bitmask type of size <code>bits</code>. In row will round up to the closest full byte.
     *
     * @param bits The number of bits in the bitmask.
     */
    public static NativeType bitmaskOf(int bits) {
        return new BitmaskNativeType(bits);
    }

    /**
     * Creates a STRING type with maximal length is <code>len</code>.
     *
     * @param len Maximum length of the string.
     */
    public static NativeType stringOf(int len) {
        return new VarlenNativeType(NativeTypeSpec.STRING, len);
    }

    /**
     * Creates a BYTES type with maximal length is <code>len</code>.
     *
     * @param len Maximum length of the byte array.
     */
    public static NativeType blobOf(int len) {
        return new VarlenNativeType(NativeTypeSpec.BYTES, len);
    }

    /**
     * Return the native type for specified object.
     *
     * @return {@code null} for {@code null} value. Otherwise returns NativeType according to the value's type.
     */
    public static NativeType fromObject(Object val) {
        NativeTypeSpec spec = NativeTypeSpec.fromObject(val);

        if (spec == null)
            return null;

        switch (spec) {
            case BYTE:
                return BYTE;

            case SHORT:
                return SHORT;

            case INTEGER:
                return INTEGER;

            case LONG:
                return LONG;

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case UUID:
                return UUID;

            case STRING:
                return NativeTypes.stringOf(((CharSequence)val).length());

            case BYTES:
                return NativeTypes.blobOf(((byte[])val).length);

            case BITMASK:
                return NativeTypes.bitmaskOf(((BitSet)val).length());

            default:
                assert false : "Unexpected type: " + spec;

                return null;
        }
    }

    /** */
    public static NativeType from(ColumnType type) {
        switch (type.typeSpec()) {
            case INT8:
                return BYTE;

            case INT16:
                return SHORT;

            case INT32:
                return INTEGER;

            case INT64:
                return LONG;

            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
                throw new UnsupportedOperationException("Unsigned types are not supported yet.");

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case DECIMAL:
                ColumnType.NumericColumnType numType = (ColumnType.NumericColumnType)type;
                return new NumericNativeType(numType.precision(), numType.scale());

            case UUID:
                return UUID;

            case BITMASK:
                return new BitmaskNativeType(((ColumnType.VarLenColumnType)type).length());

            case STRING:
                return new VarlenNativeType(
                    NativeTypeSpec.STRING,
                    ((ColumnType.VarLenColumnType)type).length() > 0 ?
                        ((ColumnType.VarLenColumnType)type).length() : Integer.MAX_VALUE
                );

            case BLOB:
                return new VarlenNativeType(
                    NativeTypeSpec.BYTES,
                    ((ColumnType.VarLenColumnType)type).length() > 0 ?
                        ((ColumnType.VarLenColumnType)type).length() : Integer.MAX_VALUE
                );

            default:
                throw new InvalidTypeException("Unexpected type " + type);
        }
    }
}
