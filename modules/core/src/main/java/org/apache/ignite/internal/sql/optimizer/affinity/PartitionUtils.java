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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for partition extractor.
 */
// TODO: Rename to PartitionDataTypeUtils.
public class PartitionUtils {

    /** Decimal representation of maximum long value. */
    private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    /** Decimal representation of minimum long value. */
    private static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    /**
     * Convert argument to the given type.
     *
     * @param arg Argument.
     * @param targetType Type.
     * @return Converted argument.
     * @throws IgniteDataTypeConversionException If convertation failed.
     */
    @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
    public static Object convert(Object arg, PartitionParameterType targetType)
        throws IgniteDataTypeConversionException {
        assert targetType != null;

        if (arg == null)
            return null;

        PartitionParameterType argType = typeFromClass(arg.getClass());

        if (argType == targetType && !(argType == PartitionParameterType.DATE ||
            argType == PartitionParameterType.TIME || argType == PartitionParameterType.TIMESTAMP))
            return arg;

        try {
            switch (targetType) {
                case BOOLEAN:
                    return getBoolean(arg, targetType, argType);

                case BYTE:
                    return getByte(arg, targetType, argType);

                case SHORT:
                    return getShort(arg, targetType, argType);

                case INT:
                    return getInt(arg, targetType, argType);

                case LONG:
                    return getLong(arg, targetType, argType);

                case DECIMAL:
                    return getDecimal(arg, targetType, argType);

                case DOUBLE:
                    return getDouble(arg, targetType, argType);

                case FLOAT:
                    return getFloat(arg, targetType, argType);

                case STRING:
                    return getString(arg, targetType, argType);

                case UUID:
                    return getUUID(arg, targetType, argType);

                default:
                    throw new IgniteDataTypeConversionException("Unable to convert arg [" + arg + "] " +
                        "with type [" + argType + "] to [" + targetType + "].");
            }
        }
        catch (IllegalArgumentException e) {
            throw new IgniteDataTypeConversionException("Unable to convert arg [" + arg + "] " +
                "with derived type [" + argType + "] to [" + targetType + "].", e);
        }
    }

    /**
     * Convert argument to <code>UUID</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getUUID(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
                String s = String.valueOf(arg);
                return stringToUUID(s);
            case DECIMAL:
                String p = ((BigDecimal)arg).toPlainString();
                s = p.length() < 40 ? p : arg.toString();
                return stringToUUID(s);
            case STRING:
                // TODO: Remove and user UUID.fromString(). If there are any subtle differences between
                // TODO: UUID.fromString and H2 logic, they should be explained in comments so that we understand
                // TODO: what part of conversions are absent in our logic.
                return stringToUUID((String)arg);
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>String</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    private static Object getString(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return (Boolean)arg ? "TRUE" : "FALSE";
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
                return String.valueOf(arg);
            case DECIMAL: {
                // TODO: What for? Explain in comments difference between toPlainString and toString
                String p = ((BigDecimal)arg).toPlainString();
                return p.length() < 40 ? p : arg.toString();
            }
            case UUID:
                return ((UUID)arg).toString();
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Float</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getFloat(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? (float)1 : (float)0;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DECIMAL:
            case DOUBLE:
                return ((Number)arg).floatValue();
            case STRING:
                return Float.parseFloat(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Double</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getDouble(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? (double)1 : (double)0;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DECIMAL:
            case FLOAT:
                return ((Number)arg).doubleValue();
            case STRING:
                return Double.parseDouble(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Decimal</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getDecimal(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? BigDecimal.ONE : BigDecimal.ZERO;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return BigDecimal.valueOf(((Number)arg).longValue());
            case DOUBLE: {
                double d = (double)arg;
                if (Double.isInfinite(d) || Double.isNaN(d)) {
                    throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                        "with derived type double to deimal.");
                }
                return BigDecimal.valueOf(d);
            }
            case FLOAT: {
                float f = (float)arg;
                if (Float.isInfinite(f) || Float.isNaN(f)) {
                    throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                        "with derived type float to deimal.");
                }
                return new BigDecimal(Float.toString(f));
            }
            case STRING:
                return new BigDecimal(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Long</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getLong(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? 1L : 0L;
            case BYTE:
            case SHORT:
            case INT:
                return ((Number)arg).longValue();
            case DECIMAL:
                return convertToLong((BigDecimal)arg, argType);
            case DOUBLE:
                return convertToLong((Double)arg, argType);
            case FLOAT:
                return convertToLong((Float)arg, argType);
            case STRING:
                return Long.parseLong(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Integer</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getInt(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? 1 : 0;
            case BYTE:
            case SHORT:
                return ((Number)arg).intValue();
            case LONG:
                return convertToInt((Long)arg, argType);
            case DECIMAL:
                return convertToInt(convertToLong((BigDecimal)arg, argType), argType);
            case DOUBLE:
                return convertToInt(convertToLong((Double)arg, argType), argType);
            case FLOAT:
                return convertToInt(convertToLong((Float)arg, argType), argType);
            case STRING:
                return Integer.parseInt(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Short</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    @NotNull private static Object getShort(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? (short)1 : (short)0;
            case BYTE:
                return ((Byte)arg).shortValue();
            case INT:
                return convertToShort((Integer)arg, argType);
            case LONG:
                return convertToShort((Long)arg, argType);
            case DECIMAL:
                return convertToShort(convertToLong((BigDecimal)arg, argType), argType);
            case DOUBLE:
                return convertToShort(convertToLong((Double)arg, argType), argType);
            case FLOAT:
                return convertToShort(convertToLong((Float)arg, argType), argType);
            case STRING:
                return Short.parseShort(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Byte</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    // TODO: Investigate other vendors: what happens for out-of-bound case? e.g. convert(257) -> error or -1 or something else?
    @NotNull private static Object getByte(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BOOLEAN:
                return arg.equals(Boolean.TRUE) ? (byte)1 : (byte)0;
            case SHORT:
            case INT:
            case LONG:
                return convertToByte(((Number)arg).longValue(), argType);
            case DECIMAL:
                // TODO: Remove intermediate
                return convertToByte(convertToLong((BigDecimal)arg, argType), argType);
            case DOUBLE:
                // TODO: Remove intermediate
                return convertToByte(convertToLong((Double)arg, argType), argType);
            case FLOAT:
                // TODO: Remove intermediate
                return convertToByte(convertToLong((Float)arg, argType), argType);
            case STRING:
                return Byte.parseByte(((String)arg).trim());
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Convert argument to <code>Boolean</code>.
     *
     * @param arg Value to convert.
     * @param targetType Target type.
     * @param argType Type of the argument.
     * @return Converted value.
     */
    // TODO: Investigate other vendors: what happens for -1?
    @NotNull private static Object getBoolean(Object arg, PartitionParameterType targetType,
        PartitionParameterType argType) {
        switch (argType) {
            case BYTE:
                return (Byte)arg != 0;
            case SHORT:
                return (Short)arg != 0;
            case INT:
                return (Integer)arg != 0;
            case LONG:
                return (Long)arg != 0;
            case DECIMAL:
                return !arg.equals(BigDecimal.ZERO);
            case DOUBLE:
                // TODO: signum
                return (Double)arg != 0;
            case FLOAT:
                // TODO: signum
                return (Float)arg != 0;
            case STRING: {
                String sVal = (String)arg;
                if ("true".equalsIgnoreCase(sVal) ||
                    "t".equalsIgnoreCase(sVal) ||
                    "yes".equalsIgnoreCase(sVal) ||
                    "y".equalsIgnoreCase(sVal))
                    return Boolean.TRUE;
                else if ("false".equalsIgnoreCase(sVal) ||
                    "f".equalsIgnoreCase(sVal) ||
                    "no".equalsIgnoreCase(sVal) ||
                    "n".equalsIgnoreCase(sVal))
                    return Boolean.FALSE;
                else
                    return new BigDecimal(sVal).signum() != 0;
            }
            case TIME:
            case DATE:
            case TIMESTAMP:
            case UUID:
            default:
                throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                    "with type [" + argType + "] to [" + targetType + "].");
        }
    }

    /**
     * Private constructor.
     */
    private PartitionUtils() {
        // No-op.
    }

    /**
     * Get the <code>PartitionParameterType</code> type for the given Java class.
     *
     * @param c The Java class.
     * @return The <code>PartitionParameterType</code> type.
     */
    // TODO: Do we need it?
    private static PartitionParameterType typeFromClass(Class<?> c) {
        assert c != null;

        if (String.class == c)
            return PartitionParameterType.STRING;
        else if (Integer.class == c)
            return PartitionParameterType.INT;
        else if (Long.class == c)
            return PartitionParameterType.LONG;
        else if (Boolean.class == c)
            return PartitionParameterType.BOOLEAN;
        else if (Double.class == c)
            return PartitionParameterType.DOUBLE;
        else if (Byte.class == c)
            return PartitionParameterType.BYTE;
        else if (Short.class == c)
            return PartitionParameterType.SHORT;
        else if (Float.class == c)
            return PartitionParameterType.FLOAT;
        else if (UUID.class == c)
            return PartitionParameterType.UUID;
        else if (BigDecimal.class.isAssignableFrom(c))
            return PartitionParameterType.DECIMAL;
        else if (Date.class.isAssignableFrom(c))
            return PartitionParameterType.DATE;
        else if (Time.class.isAssignableFrom(c))
            return PartitionParameterType.TIME;
        else if (Timestamp.class.isAssignableFrom(c))
            return PartitionParameterType.TIMESTAMP;
        else if (java.util.Date.class.isAssignableFrom(c))
            return PartitionParameterType.TIMESTAMP;
        else if (LocalDate.class == c)
            return PartitionParameterType.DATE;
        else if (LocalTime.class == c)
            return PartitionParameterType.TIME;
        else if (LocalDateTime.class == c)
            return PartitionParameterType.TIMESTAMP;
        else
            throw new IllegalArgumentException("Unable to convert arg: type [" + c + "] not supported.");
    }

    /**
     * Utility method that helps to convert long to byte.
     *
     * @param val long value.
     * @return byte value.
     */
    private static byte convertToByte(long val, PartitionParameterType orignialArgType) {
        if (val > Byte.MAX_VALUE || val < Byte.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with type [" + orignialArgType + "] to [byte]: arg is out of bounds.");
        }
        return (byte)val;
    }

    /**
     * Utility method that helps to convert long to short.
     *
     * @param val long value.
     * @return short value.
     */
    private static short convertToShort(long val, PartitionParameterType orignialArgType) {
        if (val > Short.MAX_VALUE || val < Short.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with type [" + orignialArgType + "] to [short]: arg is out of bounds.");
        }
        return (short)val;
    }

    /**
     * Utility method that helps to convert long to int.
     *
     * @param val long value.
     * @return int value.
     */
    private static int convertToInt(long val, PartitionParameterType orignialArgType) {
        if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with type [" + orignialArgType + "] to [int]: arg is out of bounds.");
        }
        return (int)val;
    }

    /**
     * Utility method that helps to convert double to long.
     *
     * @param val double value.
     * @return long value.
     */
    private static long convertToLong(double val, PartitionParameterType orignialArgType) {
        if (val > Long.MAX_VALUE || val < Long.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with type [" + orignialArgType + "] to [double]: arg is out of bounds.");
        }
        return Math.round(val);
    }

    /**
     * Utility method that helps to convert decimal to long.
     *
     * @param val decimal value.
     * @return long value.
     */
    private static long convertToLong(BigDecimal val, PartitionParameterType orignialArgType) {
        if (val.compareTo(MAX_LONG_DECIMAL) > 0 || val.compareTo(MIN_LONG_DECIMAL) < 0) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with type [" + orignialArgType + "] to [long]: arg is out of bounds.");
        }
        return val.setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
    }

    /**
     * Utility method that helps to convert String to UUID.
     *
     * @param s String to
     * @return UUID.
     */
    private static UUID stringToUUID(String s) {
        long low = 0, high = 0;
        for (int i = 0, j = 0, len = s.length(); i < len; i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9')
                low = (low << 4) | (c - '0');
            else if (c >= 'a' && c <= 'f')
                low = (low << 4) | (c - 'a' + 0xa);
            else if (c == '-')
                continue;
            else if (c >= 'A' && c <= 'F')
                low = (low << 4) | (c - 'A' + 0xa);
            else if (c <= ' ')
                continue;
            else
                throw new IllegalArgumentException("Unable to convert arg.");
            if (j++ == 15) {
                high = low;
                low = 0;
            }
        }

        return new UUID(high, low);
    }
}
