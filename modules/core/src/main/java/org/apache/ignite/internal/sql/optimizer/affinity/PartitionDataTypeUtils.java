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
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for partition extractor.
 */
public class PartitionDataTypeUtils {
    /** Decimal representation of maximum long value. */
    private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    /** Decimal representation of minimum long value. */
    private static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    /** Decimal representation of maximum int value. */
    private static final BigDecimal MAX_INTEGER_DECIMAL = new BigDecimal(Integer.MAX_VALUE);

    /** Decimal representation of minimum int value. */
    private static final BigDecimal MIN_INTEGER_DECIMAL = new BigDecimal(Integer.MIN_VALUE);

    /** Decimal representation of maximum short value. */
    private static final BigDecimal MAX_SHORT_DECIMAL = new BigDecimal(Short.MAX_VALUE);

    /** Decimal representation of minimum short value. */
    private static final BigDecimal MIN_SHORT_DECIMAL = new BigDecimal(Short.MIN_VALUE);

    /** Decimal representation of maximum byte value. */
    private static final BigDecimal MAX_BYTE_DECIMAL = new BigDecimal(Byte.MAX_VALUE);

    /** Decimal representation of minimum byte value. */
    private static final BigDecimal MIN_BYTE_DECIMAL = new BigDecimal(Byte.MIN_VALUE);

    /** Convertation failure marker. */
    public static final Object CONVERTATION_FAILURE = new Object();

    /**
     * Convert argument to the given type.
     *
     * @param arg Argument.
     * @param targetType Type.
     * @return Converted argument or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    public static Object convert(Object arg, PartitionParameterType targetType) {
        assert targetType != null;

        if (arg == null)
            return null;

        PartitionParameterType argType = typeFromClass(arg.getClass());

        if (argType == null)
            return CONVERTATION_FAILURE;

        if (argType == targetType)
            return arg;

        try {
            switch (targetType) {
                case BOOLEAN:
                    return getBoolean(arg, argType);

                case BYTE:
                    return getIntegerNumeric(
                        arg,
                        argType,
                        Byte.MIN_VALUE,
                        Byte.MAX_VALUE,
                        MIN_BYTE_DECIMAL,
                        MAX_BYTE_DECIMAL,
                        PartitionParameterType.BYTE
                    );

                case SHORT:
                    return getIntegerNumeric(
                        arg,
                        argType,
                        Short.MIN_VALUE,
                        Short.MAX_VALUE,
                        MIN_SHORT_DECIMAL,
                        MAX_SHORT_DECIMAL,
                        PartitionParameterType.SHORT
                    );

                case INT:
                    return getIntegerNumeric(
                        arg,
                        argType,
                        Integer.MIN_VALUE,
                        Integer.MAX_VALUE,
                        MIN_INTEGER_DECIMAL,
                        MAX_INTEGER_DECIMAL,
                        PartitionParameterType.INT
                    );

                case LONG:
                    return getIntegerNumeric(
                        arg,
                        argType,
                        Long.MIN_VALUE,
                        Long.MAX_VALUE, MIN_LONG_DECIMAL,
                        MAX_LONG_DECIMAL,
                        PartitionParameterType.LONG
                    );

                case FLOAT:
                    return getFloat(arg, argType);

                case DOUBLE:
                    return getDouble(arg, argType);

                case DECIMAL:
                    return getDecimal(arg, argType);

                case STRING:
                    return getString(arg, argType);

                case UUID:
                    return getUUID(arg, argType);

                default:
                    return CONVERTATION_FAILURE;
            }
        }
        catch (NumberFormatException e) {
            return CONVERTATION_FAILURE;
        }
    }

    /**
     * Convert argument to <code>UUID</code>.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    @NotNull private static Object getUUID(Object arg, PartitionParameterType argType) {
        switch (argType) {
            case STRING:
                return stringToUUID((String)arg);

            default:
                return CONVERTATION_FAILURE;
        }
    }

    /**
     * Convert argument to <code>String</code>.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    private static Object getString(Object arg, PartitionParameterType argType) {
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
                // We had to use such kind of convertation instead of common arg.toString() in order to match
                // H2 convertation results. In case of using arg.toString() we will have inconsistant convertation
                // results for values similar to BigDecimal.valueOf(12334535345456700.12345634534534578901).
                // Main difference between toPlainString() and toString() is that toPlainString()
                // returns a string representation of a {@code BigDecimal}  without an exponent field.
                String p = ((BigDecimal)arg).toPlainString();

                return p.length() < 40 ? p : arg.toString();
            }

            case UUID:
                return arg.toString();

            default:
                return CONVERTATION_FAILURE;
        }
    }

    /**
     * Convert argument to <code>Float</code>.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    @NotNull private static Object getFloat(Object arg, PartitionParameterType argType) {
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
                return CONVERTATION_FAILURE;
        }
    }

    /**
     * Convert argument to <code>Double</code>.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    @NotNull private static Object getDouble(Object arg, PartitionParameterType argType) {
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
                return CONVERTATION_FAILURE;
        }
    }

    /**
     * Convert argument to <code>Decimal</code>.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    @NotNull private static Object getDecimal(Object arg, PartitionParameterType argType) {
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

                if (Double.isInfinite(d) || Double.isNaN(d))
                    return CONVERTATION_FAILURE;

                return BigDecimal.valueOf(d);
            }
            case FLOAT: {
                float f = (float)arg;

                if (Float.isInfinite(f) || Float.isNaN(f))
                    return CONVERTATION_FAILURE;

                return new BigDecimal(Float.toString(f));
            }

            case STRING:
                return new BigDecimal(((String)arg).trim());

            default:
                return CONVERTATION_FAILURE;
        }
    }

    /**
     * Convert argument to integer numeric: byte, short, int, long.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
    @NotNull private static Object getIntegerNumeric(
        Object arg,
        PartitionParameterType argType,
        long minVal,
        long maxVal,
        BigDecimal minValDecimal,
        BigDecimal maxValDecimal,
        PartitionParameterType targetType
    ) {
        assert targetType == PartitionParameterType.BYTE || targetType == PartitionParameterType.SHORT ||
            targetType == PartitionParameterType.INT || targetType == PartitionParameterType.LONG;

        long res;

        switch (argType) {
            case BOOLEAN:
                res = arg.equals(Boolean.TRUE) ? 1L : 0L;

                break;

            case BYTE:
            case SHORT:
            case INT:
            case LONG: {
                res = ((Number)arg).longValue();

                if (res > maxVal || res < minVal)
                    return CONVERTATION_FAILURE;

                break;
            }
            case DECIMAL: {
                BigDecimal d = (BigDecimal)arg;

                if (d.compareTo(maxValDecimal) > 0 || d.compareTo(minValDecimal) < 0)
                    return CONVERTATION_FAILURE;
                else
                    res = d.setScale(0, BigDecimal.ROUND_HALF_UP).longValue();

                break;
            }

            case DOUBLE: {
                Double d = (Double)arg;

                if (d > maxVal || d < minVal)
                    return CONVERTATION_FAILURE;
                else
                    res = Math.round(d);

                break;
            }

            case FLOAT: {
                Float f = (Float)arg;

                if (f > maxVal || f < minVal)
                    return CONVERTATION_FAILURE;
                else
                    res = Math.round(f);

                break;
            }

            case STRING: {
                res = Long.parseLong(((String)arg).trim());

                if (res > maxVal || res < minVal)
                    return CONVERTATION_FAILURE;

                break;
            }

            default:
                return CONVERTATION_FAILURE;
        }

        switch (targetType) {
            case BYTE:
                return (byte)res;
            case SHORT:
                return (short)res;
            case INT:
                return (int)res;
            case LONG:
                return res;
        }

        return CONVERTATION_FAILURE;
    }

    /**
     * Convert argument to <code>Boolean</code>.
     *
     * @param arg Argument to convert.
     * @param argType Argument type.
     * @return Converted value or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    @NotNull private static Object getBoolean(Object arg, PartitionParameterType argType) {
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
                return Math.signum((Double)arg) != 0;

            case FLOAT:
                return Math.signum((Float)arg) != 0;

            case STRING: {
                String sVal = (String)arg;

                if ("true".equalsIgnoreCase(sVal) ||
                    "t".equalsIgnoreCase(sVal) ||
                    "yes".equalsIgnoreCase(sVal) ||
                    "y".equalsIgnoreCase(sVal) ||
                    "1".equals(sVal))
                    return Boolean.TRUE;
                else if ("false".equalsIgnoreCase(sVal) ||
                    "f".equalsIgnoreCase(sVal) ||
                    "no".equalsIgnoreCase(sVal) ||
                    "n".equalsIgnoreCase(sVal) ||
                    "0".equals(sVal))
                    return Boolean.FALSE;
            }
            default:
                return CONVERTATION_FAILURE;
        }
    }

    /**
     * Get the <code>PartitionParameterType</code> type for the given Java class.
     *
     * @param c The Java class.
     * @return The <code>PartitionParameterType</code> type.
     */
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
        else if (BigDecimal.class == c)
            return PartitionParameterType.DECIMAL;
        else
            return null;
    }

    /**
     * Utility method that helps to convert String to UUID. Given method is more fault tolerant than more common
     * <code>UUID.fromString()</code>. For example it supports String represendation of UUID-without-hyphens
     * conversion, that is not supported by mentioned above <code>UUID.fromString()</code>.
     *
     * @param s String to
     * @return UUID or <code>CONVERTATION_FAILURE</code> if convertation failed.
     */
    public static Object stringToUUID(String s) {
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
                return CONVERTATION_FAILURE;

            if (j++ == 15) {
                high = low;

                low = 0;
            }
        }

        return new UUID(high, low);
    }

    /**
     * Private constructor.
     */
    private PartitionDataTypeUtils() {
        // No-op.
    }
}
