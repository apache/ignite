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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import org.apache.ignite.schema.definition.ColumnType;

/**
 * A thin wrapper over {@link NativeTypeSpec} to instantiate parameterized constrained types.
 */
public class NativeTypes {
    /** */
    public static final NativeType INT8 = new NativeType(NativeTypeSpec.INT8, 1);

    /** */
    public static final NativeType INT16 = new NativeType(NativeTypeSpec.INT16, 2);

    /** */
    public static final NativeType INT32 = new NativeType(NativeTypeSpec.INT32, 4);

    /** */
    public static final NativeType INT64 = new NativeType(NativeTypeSpec.INT64, 8);

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

    /** Timezone-free three-part value representing a year, month, and day. */
    public static final NativeType DATE = new NativeType(NativeTypeSpec.DATE, 3);

    /** Don't allow to create an instance. */
    private NativeTypes() {
    }

    /**
     * Creates a bitmask type of size <code>bits</code>. In row will round up to the closest full byte.
     *
     * @param bits The number of bits in the bitmask.
     * @return Native type.
     */
    public static NativeType bitmaskOf(int bits) {
        return new BitmaskNativeType(bits);
    }

    /**
     * Creates a number type with maximal precision.
     *
     * @param precision The number of digits in the number value.
     * @return Native type.
     */
    public static NativeType numberOf(int precision) {
        return new NumberNativeType(precision);
    }

    /**
     * Creates a STRING type with maximal length is <code>len</code>.
     *
     * @param len Maximum length of the string.
     * @return Native type.
     */
    public static NativeType stringOf(int len) {
        return new VarlenNativeType(NativeTypeSpec.STRING, len);
    }

    /**
     * Creates a BYTES type with maximal length is <code>len</code>.
     *
     * @param len Maximum length of the byte array.
     * @return Native type.
     */
    public static NativeType blobOf(int len) {
        return new VarlenNativeType(NativeTypeSpec.BYTES, len);
    }

    /**
     * Creates a DECIMAL type with maximal precision and scale.
     *
     * @param precision Precision.
     * @param scale Scale.
     * @return Native type.
     */
    public static NativeType decimalOf(int precision, int scale) {
        return new DecimalNativeType(precision, scale);
    }

    /**
     * Creates a TIME type with given precision.
     *
     * @param precision Fractional seconds meaningful digits. Allowed values are 0-9 for second to nanosecond precision.
     * @return Native type.
     */
    public static NativeType time(int precision) {
        return TemporalNativeType.time(precision);
    }

    /**
     * Creates DATETIME type as pair (date, time).
     *
     * @param precision Fractional seconds meaningful digits. Allowed values are 0-9 for second to nanosecond precision.
     * @return Native type.
     */
    public static NativeType datetime(int precision) {
        return TemporalNativeType.datetime(precision);
    }

    /**
     * Creates TIMESTAMP type.
     *
     * @param precision Fractional seconds meaningful digits. Allowed values are 0-9 for second to nanosecond precision.
     * @return Native type.
     */
    public static NativeType timestamp(int precision) {
        return TemporalNativeType.timestamp(precision);
    }

    /**
     * Creates a TIME type with default precision.
     *
     * @return Native type.
     * @see #time(int)
     */
    public static NativeType time() {
        return TemporalNativeType.time(ColumnType.TemporalColumnType.DEFAULT_PRECISION);
    }

    /**
     * Creates DATETIME type with default precision.
     *
     * @return Native type.
     * @see #datetime(int)
     */
    public static NativeType datetime() {
        return TemporalNativeType.datetime(ColumnType.TemporalColumnType.DEFAULT_PRECISION);
    }

    /**
     * Creates TIMESTAMP type with default precision.
     *
     * @return Native type.
     * @see #timestamp(int)
     */
    public static NativeType timestamp() {
        return TemporalNativeType.timestamp(ColumnType.TemporalColumnType.DEFAULT_PRECISION);
    }

    /**
     * Return the native type for specified object.
     *
     * @param val Object to map to native type.
     * @return {@code null} for {@code null} value. Otherwise returns NativeType according to the value's type.
     */
    public static NativeType fromObject(Object val) {
        NativeTypeSpec spec = NativeTypeSpec.fromObject(val);

        if (spec == null)
            return null;

        switch (spec) {
            case INT8:
                return INT8;

            case INT16:
                return INT16;

            case INT32:
                return INT32;

            case INT64:
                return INT64;

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case UUID:
                return UUID;

            case DATE:
                return DATE;

            case TIME:
                return time();

            case DATETIME:
                return datetime();

            case TIMESTAMP:
                return timestamp();

            case STRING:
                return stringOf(((CharSequence)val).length());

            case BYTES:
                return blobOf(((byte[])val).length);

            case BITMASK:
                return bitmaskOf(((BitSet)val).length());

            case NUMBER:
                return numberOf(new BigDecimal((BigInteger)val).precision());

            case DECIMAL:
                return decimalOf(((BigDecimal)val).precision(), ((BigDecimal)val).scale());

            default:
                assert false : "Unexpected type: " + spec;

                return null;
        }
    }
}
