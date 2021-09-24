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

package org.apache.ignite.schema.definition;

import java.util.Objects;

/**
 * Predefined column types.
 */
@SuppressWarnings("PublicInnerClass")
public class ColumnType {
    /** 8-bit signed int. */
    public static final ColumnType INT8 = new ColumnType(ColumnTypeSpec.INT8);

    /** 16-bit signed int. */
    public static final ColumnType INT16 = new ColumnType(ColumnTypeSpec.INT16);

    /** 32-bit signed int. */
    public static final ColumnType INT32 = new ColumnType(ColumnTypeSpec.INT32);

    /** 64-bit signed int. */
    public static final ColumnType INT64 = new ColumnType(ColumnTypeSpec.INT64);

    /** 8-bit unsigned int. */
    public static final ColumnType UINT8 = new ColumnType(ColumnTypeSpec.UINT8);

    /** 16-bit unsigned int. */
    public static final ColumnType UINT16 = new ColumnType(ColumnTypeSpec.UINT16);

    /** 32-bit unsigned int. */
    public static final ColumnType UINT32 = new ColumnType(ColumnTypeSpec.UINT32);

    /** 64-bit unsigned int. */
    public static final ColumnType UINT64 = new ColumnType(ColumnTypeSpec.UINT64);

    /** 32-bit float. */
    public static final ColumnType FLOAT = new ColumnType(ColumnTypeSpec.FLOAT);

    /** 64-bit double. */
    public static final ColumnType DOUBLE = new ColumnType(ColumnTypeSpec.DOUBLE);

    /** 128-bit UUID. */
    public static final ColumnType UUID = new ColumnType(ColumnTypeSpec.UUID);

    /** Timezone-free three-part value representing a year, month, and day. */
    public static final ColumnType DATE = new ColumnType(ColumnTypeSpec.DATE);

    /** String varlen type of unlimited length. */
    private static final VarLenColumnType UNLIMITED_STRING = stringOf(0);

    /** Blob varlen type of unlimited length. */
    private static final VarLenColumnType UNLIMITED_BLOB = blobOf(0);

    /** Number type with unlimited precision. */
    public static final NumberColumnType UNLIMITED_NUMBER = new NumberColumnType(ColumnTypeSpec.NUMBER, NumberColumnType.UNLIMITED_PRECISION);

    /**
     * Returns bit mask type.
     *
     * @param bits Bit mask size in bits.
     * @return Bitmap type.
     */
    public static VarLenColumnType bitmaskOf(int bits) {
        return new VarLenColumnType(ColumnTypeSpec.BITMASK, bits);
    }

    /**
     * Returns string type of unlimited length.
     *
     * @return String type.
     */
    public static VarLenColumnType string() {
        return UNLIMITED_STRING;
    }

    /**
     * Return string type of limited size.
     *
     * @param length String length in chars.
     * @return String type.
     */
    public static VarLenColumnType stringOf(int length) {
        return new VarLenColumnType(ColumnTypeSpec.STRING, length);
    }

    /**
     * Returns blob type of unlimited length.
     *
     * @return Blob type.
     * @see #blobOf(int)
     */
    public static VarLenColumnType blobOf() {
        return UNLIMITED_BLOB;
    }

    /**
     * Return blob type of limited length.
     *
     * @param length Blob length in bytes.
     * @return Blob type.
     */
    public static VarLenColumnType blobOf(int length) {
        return new VarLenColumnType(ColumnTypeSpec.BLOB, length);
    }

    /**
     * Returns number type with given precision.
     *
     * @param precision Precision of value.
     * @return Number type.
     * @throws IllegalArgumentException If precision value was invalid.
     */
    public static NumberColumnType numberOf(int precision) {
        if (precision <= 0)
            throw new IllegalArgumentException("Precision [" + precision + "] must be positive integer value.");

        return new NumberColumnType(ColumnTypeSpec.NUMBER, precision);
    }

    /**
     * Returns number type with the default precision.
     *
     * @return Number type.
     * @see #numberOf(int)
     */
    public static NumberColumnType numberOf() {
        return UNLIMITED_NUMBER;
    }

    /**
     * Returns decimal type with given precision and scale.
     *
     * @param precision Precision.
     * @param scale Scale.
     * @return Decimal type.
     * @throws IllegalArgumentException If precision and/or scale values were invalid.
     */
    public static DecimalColumnType decimalOf(int precision, int scale) {
        if (precision <= 0)
            throw new IllegalArgumentException("Precision [" + precision + "] must be positive integer value.");

        if (scale < 0)
            throw new IllegalArgumentException("Scale [" + scale + "] must be non-negative integer value.");

        if (precision < scale)
            throw new IllegalArgumentException("Precision [" + precision + "] must be" +
                                                   " not lower than scale [ " + scale + " ].");

        return new DecimalColumnType(ColumnTypeSpec.DECIMAL, precision, scale);
    }

    /**
     * Returns decimal type with default precision and scale values.
     *
     * @return Decimal type.
     * @see #decimalOf(int, int)
     */
    public static DecimalColumnType decimalOf() {
        return new DecimalColumnType(
            ColumnTypeSpec.DECIMAL,
            DecimalColumnType.DEFAULT_PRECISION,
            DecimalColumnType.DEFAULT_SCALE
        );
    }

    /**
     * Returns timezone-free type representing a time of day in hours, minutes, seconds, and fractional seconds
     * with the default precision of 6 (microseconds).
     *
     * @return Native type.
     * @see TemporalColumnType#DEFAULT_PRECISION
     * @see #time(int)
     */
    public static TemporalColumnType time() {
        return new TemporalColumnType(ColumnTypeSpec.TIME, TemporalColumnType.DEFAULT_PRECISION);
    }

    /**
     * Returns timezone-free type representing a time of day in hours, minutes, seconds, and fractional seconds.
     * <p>
     * Precision is a number of digits in fractional seconds part,
     * from 0 - whole seconds precision up to 9 - nanoseconds precision.
     *
     * @param precision The number of digits in fractional seconds part. Accepted values are in range [0-9].
     * @return Native type.
     * @throws IllegalArgumentException If precision value was invalid.
     */
    public static TemporalColumnType time(int precision) {
        if (precision < 0 || precision > 9)
            throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);

        return new TemporalColumnType(ColumnTypeSpec.TIME, precision);
    }

    /**
     * Returns timezone-free datetime encoded as (date, time) with the default time precision of 6 (microseconds).
     *
     * @return Native type.
     * @see TemporalColumnType#DEFAULT_PRECISION
     * @see #datetime(int)
     */
    public static TemporalColumnType datetime() {
        return new TemporalColumnType(ColumnTypeSpec.DATETIME, TemporalColumnType.DEFAULT_PRECISION);
    }

    /**
     * Returns timezone-free datetime encoded as (date, time).
     * <p>
     * Precision is a number of digits in fractional seconds part of time,
     * from 0 - whole seconds precision up to 9 - nanoseconds precision.
     *
     * @param precision The number of digits in fractional seconds part. Accepted values are in range [0-9].
     * @return Native type.
     * @throws IllegalArgumentException If precision value was invalid.
     */
    public static TemporalColumnType datetime(int precision) {
        if (precision < 0 || precision > 9)
            throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);

        return new TemporalColumnType(ColumnTypeSpec.DATETIME, precision);
    }

    /**
     * Returns point in time as number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone)
     * with the default precision of 6 (microseconds).
     *
     * @return Native type.
     * @see TemporalColumnType#DEFAULT_PRECISION
     * @see #timestamp(int)
     */
    public static TemporalColumnType timestamp() {
        return new TemporalColumnType(ColumnTypeSpec.TIMESTAMP, TemporalColumnType.DEFAULT_PRECISION);
    }

    /**
     * Returns point in time as number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone).
     * Ticks that are stored can be precised to second, millisecond, microsecond or nanosecond.
     * <p>
     * Precision is a number of digits in fractional seconds part of time,
     * from 0 - whole seconds precision up to 9 - nanoseconds precision.
     *
     * @param precision The number of digits in fractional seconds part. Accepted values are in range [0-9].
     * @return Native type.
     * @throws IllegalArgumentException If precision value was invalid.
     */
    public static TemporalColumnType timestamp(int precision) {
        if (precision < 0 || precision > 9)
            throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);

        return new TemporalColumnType(ColumnTypeSpec.TIMESTAMP, precision);
    }

    /**
     * Column type of variable length.
     */
    public static class VarLenColumnType extends ColumnType {
        /** Max length. */
        private final int length;

        /**
         * Creates variable-length column type.
         *
         * @param typeSpec Type spec.
         * @param length Type max length.
         */
        private VarLenColumnType(ColumnTypeSpec typeSpec, int length) {
            super(typeSpec);

            this.length = length;
        }

        /**
         * Max column value length.
         *
         * @return Max column value length or {@code 0} if unlimited.
         */
        public int length() {
            return length;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            VarLenColumnType type = (VarLenColumnType)o;

            return length == type.length;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), length);
        }
    }

    /**
     * Decimal column type.
     */
    public static class DecimalColumnType extends ColumnType {
        /** Default precision. */
        public static final int DEFAULT_PRECISION = 19;

        /** Default scale. */
        public static final int DEFAULT_SCALE = 3;

        /** Precision. */
        private final int precision;

        /** Scale. */
        private final int scale;

        /**
         * Creates numeric column type.
         *
         * @param typeSpec Type spec.
         * @param precision Precision.
         * @param scale Scale.
         */
        private DecimalColumnType(ColumnTypeSpec typeSpec, int precision, int scale) {
            super(typeSpec);

            this.precision = precision;
            this.scale = scale;
        }

        /**
         * Returns column precision.
         *
         * @return Precision.
         */
        public int precision() {
            return precision;
        }

        /**
         * Returns column scale.
         *
         * @return Scale.
         */
        public int scale() {
            return scale;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            DecimalColumnType type = (DecimalColumnType)o;

            return precision == type.precision && scale == type.scale;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision, scale);
        }
    }

    /**
     * Number column type.
     */
    public static class NumberColumnType extends ColumnType {
        /** Undefined precision. */
        private static final int UNLIMITED_PRECISION = 0;

        /** Max precision of value. If -1, column has no precision restrictions. */
        private final int precision;

        /**
         * Constructor.
         *
         * @param typeSpec Type specification.
         * @param precision Precision.
         */
        private NumberColumnType(ColumnTypeSpec typeSpec, int precision) {
            super(typeSpec);

            this.precision = precision;
        }

        /**
         * Returns column precision.
         *
         * @return Max number of digits.
         */
        public int precision() {
            return precision;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            NumberColumnType type = (NumberColumnType)o;

            return precision == type.precision;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision);
        }
    }

    /**
     * Column type of variable length.
     */
    public static class TemporalColumnType extends ColumnType {
        /** Default temporal type precision: microseconds. */
        public static final int DEFAULT_PRECISION = 6;

        /** Fractional seconds precision. */
        private final int precision;

        /**
         * Creates temporal type.
         *
         * @param typeSpec Type spec.
         * @param precision Fractional seconds meaningful digits. Allowed values are 0-9,
         * where {@code 0} means second precision, {@code 9} means 1-ns precision.
         */
        private TemporalColumnType(ColumnTypeSpec typeSpec, int precision) {
            super(typeSpec);

            assert precision >= 0 && precision < 10;

            this.precision = precision;
        }

        /**
         * Return column precision.
         *
         * @return Number of fractional seconds meaningful digits.
         */
        public int precision() {
            return precision;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            TemporalColumnType type = (TemporalColumnType)o;

            return precision == type.precision;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision);
        }
    }

    /**
     * Column type spec.
     */
    public enum ColumnTypeSpec {
        /** 8-bit signed integer. */
        INT8,

        /** 16-bit signed integer. */
        INT16,

        /** 32-bit signed integer. */
        INT32,

        /** 64-bit signed integer. */
        INT64,

        /** 8-bit unsigned integer. */
        UINT8,

        /** 16-bit unsigned integer. */
        UINT16,

        /** 32-bit unsigned integer. */
        UINT32,

        /** 64-bit unsigned integer. */
        UINT64,

        /** 32-bit single-precision floating-point number. */
        FLOAT,

        /** 64-bit double-precision floating-point number. */
        DOUBLE,

        /** A decimal floating-point number. */
        DECIMAL,

        /** Timezone-free date. */
        DATE,

        /** Timezone-free time with precision. */
        TIME,

        /** Timezone-free datetime. */
        DATETIME,

        /** Number of ticks since Jan 1, 1970 00:00:00.000 (with no timezone). Tick unit depends on precision. */
        TIMESTAMP,

        /** 128-bit UUID. */
        UUID,

        /** Bit mask. */
        BITMASK,

        /** String. */
        STRING,

        /** Binary data. */
        BLOB,

        /** Number. */
        NUMBER,
    }

    /** Type spec. */
    private final ColumnTypeSpec typeSpec;

    /**
     * Creates column type.
     *
     * @param typeSpec Type spec.
     */
    private ColumnType(ColumnTypeSpec typeSpec) {
        this.typeSpec = typeSpec;
    }

    /**
     * Returns column type spec.
     *
     * @return Type spec.
     */
    public ColumnTypeSpec typeSpec() {
        return typeSpec;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ColumnType type = (ColumnType)o;

        return typeSpec == type.typeSpec;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(typeSpec);
    }
}
