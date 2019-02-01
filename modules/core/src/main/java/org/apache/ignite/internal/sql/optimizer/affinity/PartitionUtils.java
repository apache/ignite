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
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods for partition extractor.
 */
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
     */
    @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
    public static Object convert(Object arg, @Nullable PartitionParameterType targetType) {
        assert targetType != null;

        if (arg == null)
            return null;

        PartitionParameterType argType = typeFromClass(arg.getClass());

        if (argType == targetType)
            return arg;

        try {
            switch (targetType) {
                case BOOLEAN: {
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
                            return (Double)arg != 0;
                        case FLOAT:
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
                            throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                                "with derived type [" + argType + "] to [" + targetType + "].");
                    }
                    break;
                }

                case BYTE: {
                    switch (argType) {
                        case BOOLEAN:
                            return arg.equals(Boolean.TRUE) ? (byte)1 : (byte)0;
                        case SHORT:
                        case INT:
                        case LONG:
                            return convertToByte(((Number)arg).longValue());
                        case DECIMAL:
                            return convertToByte(convertToLong((BigDecimal)arg));
                        case DOUBLE:
                            return convertToByte(convertToLong((Double)arg));
                        case FLOAT:
                            return convertToByte(convertToLong((Float)arg));
                        case STRING:
                            return Byte.parseByte(((String)arg).trim());
                    }
                    break;
                }
                case SHORT: {
                    switch (argType) {
                        case BOOLEAN:
                            return arg.equals(Boolean.TRUE) ? (short)1 : (short)0;
                        case BYTE:
                            return ((Byte)arg).shortValue();
                        case INT:
                            return convertToShort((Integer)arg);
                        case LONG:
                            return convertToShort((Long)arg);
                        case DECIMAL:
                            return convertToShort(convertToLong((BigDecimal)arg));
                        case DOUBLE:
                            return convertToShort(convertToLong((Double)arg));
                        case FLOAT:
                            return convertToShort(convertToLong((Float)arg));
                        case STRING:
                            return Short.parseShort(((String)arg).trim());
                    }
                    break;
                }
                case INT: {
                    switch (argType) {
                        case BOOLEAN:
                            return arg.equals(Boolean.TRUE) ? 1 : 0;
                        case BYTE:
                        case SHORT:
                            return ((Number)arg).intValue();
                        case LONG:
                            return convertToInt((Long)arg);
                        case DECIMAL:
                            return convertToInt(convertToLong((BigDecimal)arg));
                        case DOUBLE:
                            return convertToInt(convertToLong((Double)arg));
                        case FLOAT:
                            return convertToInt(convertToLong((Float)arg));
                        case STRING:
                            return Integer.parseInt(((String)arg).trim());
                    }
                    break;
                }
                case LONG: {
                    switch (argType) {
                        case BOOLEAN:
                            return arg.equals(Boolean.TRUE) ? 1L : 0L;
                        case BYTE:
                        case SHORT:
                        case INT:
                            return ((Number)arg).longValue();
                        case DECIMAL:
                            return convertToLong((BigDecimal)arg);
                        case DOUBLE:
                            return convertToLong((Double)arg);
                        case FLOAT:
                            return convertToLong((Float)arg);
                        case STRING:
                            return Long.parseLong(((String)arg).trim());
                    }
                    break;
                }
                case DECIMAL: {
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
                    }
                    break;
                }
                case DOUBLE: {
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
                    }
                    break;
                }
                case FLOAT: {
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
                    }
                    break;
                }
                case STRING: {
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
                            String p = ((BigDecimal)arg).toPlainString();
                            return p.length() < 40 ? p : arg.toString();
                        }
                        case TIME:
                            // TODO: 31.01.19
                            break;
                        case DATE:
                            // TODO: 31.01.19
                            break;
                        case TIMESTAMP:
                            // TODO: 31.01.19
                            break;
                        case UUID:
                            // TODO: 31.01.19 convertion rules differs from H2, check that it's valid
                            break;
                    }
                    break;
                }
                case DATE: {
                    switch (argType) {
//                        case STRING:
//                            return ValueDate.parse(s.trim());
//                        case TIME:
//                            // because the time has set the date to 1970-01-01,
//                            // this will be the result
//                            return ValueDate.fromDateValue(DateTimeUtils.EPOCH_DATE_VALUE);
//                        case TIMESTAMP:
//                            return ValueDate.fromDateValue(
//                                ((ValueTimestamp) this).getDateValue());
                    }
                    break;
                }
                case TIME: {
                    switch (argType) {
//                        case STRING:
//                            return ValueTime.parse(s.trim());
//                        case DATE:
//                            // need to normalize the year, month and day because a date
//                            // has the time set to 0, the result will be 0
//                            return ValueTime.fromNanos(0);
//                        case TIMESTAMP:
//                            return ValueTime.fromNanos(
//                                ((ValueTimestamp) this).getTimeNanos());
                    }
                    break;
                }
                case TIMESTAMP: {
                    switch (argType) {
//                        case STRING:
//                            return ValueTimestamp.parse(s.trim(), mode);
//                        case TIME:
//                            return DateTimeUtils.normalizeTimestamp(
//                                0, ((ValueTime) this).getNanos());
//                        case DATE:
//                            return ValueTimestamp.fromDateValueAndNanos(
//                                ((ValueDate) this).getDateValue(), 0);
                    }
                    break;
                }
                case UUID: {
                    switch (argType) {
                        case BYTE:
                            // TODO: 31.01.19
                        case STRING:
                            return UUID.fromString((String)arg);
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unable to convert arg.");
            }
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unable to convert arg [" + arg + "] " +
                "with derived type [" + argType + "] to [" + targetType + "].", e);
        }

        // TODO: 30.01.19
        throw new IllegalArgumentException("Unable to convert arg.");
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

    private static byte convertToByte(long val) {
        if (val > Byte.MAX_VALUE || val < Byte.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with derived type long to byte: arg is out of bounds.");
        }
        return (byte)val;
    }

    private static short convertToShort(long val) {
        if (val > Short.MAX_VALUE || val < Short.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with derived type long to short: arg is out of bounds.");
        }
        return (short)val;
    }

    private static int convertToInt(long val) {
        if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with derived type long to int: arg is out of bounds.");
        }
        return (int)val;
    }

    private static long convertToLong(double val) {
        if (val > Long.MAX_VALUE || val < Long.MIN_VALUE) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with derived type double to long: arg is out of bounds.");
        }
        return Math.round(val);
    }

    private static long convertToLong(BigDecimal val) {
        if (val.compareTo(MAX_LONG_DECIMAL) > 0 ||
            val.compareTo(MIN_LONG_DECIMAL) < 0) {
            throw new IllegalArgumentException("Unable to convert arg [" + val + "] " +
                "with derived type BigDecimal to long: arg is out of bounds.");
        }
        return val.setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
    }
}
