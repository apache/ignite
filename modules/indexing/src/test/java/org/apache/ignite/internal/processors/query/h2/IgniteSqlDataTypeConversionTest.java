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

package org.apache.ignite.internal.processors.query.h2;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionParameterType;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.value.Value;
import org.junit.Test;

public class IgniteSqlDataTypeConversionTest extends GridCommonAbstractTest {
    private static final Map<PartitionParameterType, Class<?>> PARAMETER_TYPE_TO_JAVA_CLASS;

    private static final Map<PartitionParameterType, Integer> IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE;

    /** Default node. */
    private static IgniteEx ignite;

    static {
        Map<PartitionParameterType, Class<?>> paramTypeToJavaClass = new HashMap<>();

        paramTypeToJavaClass.put(PartitionParameterType.BOOLEAN, Boolean.class);
        paramTypeToJavaClass.put(PartitionParameterType.BYTE, Byte.class);
        paramTypeToJavaClass.put(PartitionParameterType.SHORT, Short.class);
        paramTypeToJavaClass.put(PartitionParameterType.INT, Integer.class);
        paramTypeToJavaClass.put(PartitionParameterType.LONG, Long.class);
        paramTypeToJavaClass.put(PartitionParameterType.FLOAT, Float.class);
        paramTypeToJavaClass.put(PartitionParameterType.DOUBLE, Double.class);
        paramTypeToJavaClass.put(PartitionParameterType.STRING, String.class);
        paramTypeToJavaClass.put(PartitionParameterType.DECIMAL, BigDecimal.class);
        paramTypeToJavaClass.put(PartitionParameterType.DATE, Date.class);
        paramTypeToJavaClass.put(PartitionParameterType.TIME, Time.class);
        paramTypeToJavaClass.put(PartitionParameterType.TIMESTAMP, Timestamp.class);
        paramTypeToJavaClass.put(PartitionParameterType.UUID, UUID.class);

        PARAMETER_TYPE_TO_JAVA_CLASS = Collections.unmodifiableMap(paramTypeToJavaClass);

        Map<PartitionParameterType, Integer> igniteParamTypeToH2ParamType = new HashMap<>();
        igniteParamTypeToH2ParamType.put(PartitionParameterType.BOOLEAN, Value.BOOLEAN);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.BYTE, Value.BYTE);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.SHORT, Value.SHORT);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.INT, Value.INT);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.LONG, Value.LONG);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.FLOAT, Value.FLOAT);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.DOUBLE, Value.DOUBLE);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.STRING, Value.STRING);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.DECIMAL, Value.DECIMAL);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.DATE, Value.DATE);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.TIME, Value.TIME);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.TIMESTAMP, Value.TIMESTAMP);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.UUID, Value.UUID);

        IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE = Collections.unmodifiableMap(igniteParamTypeToH2ParamType);
    }

    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(0);
    }

    @Test
    public void convertNull() throws Exception {
        checkConvertation(null);
    }

    @Test
    public void convertBoolean() throws Exception {
        checkConvertation(Boolean.TRUE);
        checkConvertation(Boolean.FALSE);
    }

    @Test
    public void convertByte() throws Exception {
        checkConvertation((byte)42);
        checkConvertation((byte)0);
        checkConvertation(Byte.MIN_VALUE);
        checkConvertation(Byte.MAX_VALUE);
    }

    @Test
    public void convertShort() throws Exception {
        checkConvertation((short)42);
        checkConvertation((short)0);
        checkConvertation(Short.MIN_VALUE);
        checkConvertation(Short.MAX_VALUE);
    }

    @Test
    public void convertInteger() throws Exception {
        checkConvertation(42);
        checkConvertation(0);
        checkConvertation(Integer.MIN_VALUE);
        checkConvertation(Integer.MAX_VALUE);
    }

    @Test
    public void convertLong() throws Exception {
        checkConvertation(42L);
        checkConvertation(0L);
        checkConvertation(Long.MIN_VALUE);
        checkConvertation(Long.MAX_VALUE);
    }

    @Test
    public void convertFloat() throws Exception {
        checkConvertation(42.1f);
        checkConvertation(0.1f);
        checkConvertation(0f);
        checkConvertation(1.2345678E7f);

        checkConvertation(Float.POSITIVE_INFINITY);
        checkConvertation(Float.NEGATIVE_INFINITY);
        checkConvertation(Float.NaN);

        checkConvertation(Float.MIN_VALUE);
        checkConvertation(Float.MAX_VALUE);
    }

    @Test
    public void convertDouble() throws Exception {
        checkConvertation(42.2d);
        checkConvertation(0.2d);
        checkConvertation(0d);
        checkConvertation(1.2345678E7d);

        checkConvertation(Double.POSITIVE_INFINITY);
        checkConvertation(Double.NEGATIVE_INFINITY);
        checkConvertation(Double.NaN);

        checkConvertation(Double.MIN_VALUE);
        checkConvertation(Double.MAX_VALUE);
    }

    /** {@inheritDoc} */
    public boolean getBoolean(Object val) throws SQLException {

        if (val == null)
            return false;

        Class<?> cls = val.getClass();

        if (cls == Boolean.class)
            return ((Boolean)val);
        else if (val instanceof Number)
            return ((Number)val).intValue() != 0;
        else if (cls == String.class || cls == Character.class) {
            try {
                return Integer.parseInt(val.toString()) != 0;
            }
            catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to boolean: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        }
        else
            throw new SQLException("Cannot convert to boolean: " + val, SqlStateCode.CONVERSION_FAILED);
    }

    @SuppressWarnings({"ThrowableNotThrown", "AssertWithSideEffects"})
    private void checkConvertation(Object arg) throws Exception {
        // TODO: 31.01.19 instead of tmpTypes PartitionParameterType.values() should be used when Time, Date and Timestamp are supported.
        Collection<PartitionParameterType> tmpTypes = Arrays.asList(
            PartitionParameterType.BOOLEAN,
            PartitionParameterType.BYTE,
            PartitionParameterType.SHORT,
            PartitionParameterType.INT,
            PartitionParameterType.LONG,
            PartitionParameterType.DECIMAL,
            PartitionParameterType.DOUBLE,
            PartitionParameterType.FLOAT,
            PartitionParameterType.STRING);

        IgniteH2Indexing idx = (IgniteH2Indexing)ignite.context().query().getIndexing();

        for (PartitionParameterType targetType : tmpTypes) {
            // TODO: 01.02.19 collapse
            if (arg != null && (
                (arg.getClass().equals(Short.class) && targetType == PartitionParameterType.BYTE && (((short)arg) > Byte.MAX_VALUE || ((short)arg) < Byte.MIN_VALUE)) ||
                    (arg.getClass().equals(Integer.class) && targetType == PartitionParameterType.BYTE && (((int)arg) > Byte.MAX_VALUE || ((int)arg) < Byte.MIN_VALUE)) ||
                    (arg.getClass().equals(Integer.class) && targetType == PartitionParameterType.SHORT && (((int)arg) > Short.MAX_VALUE || ((int)arg) < Short.MIN_VALUE)) ||
                    (arg.getClass().equals(Long.class) && targetType == PartitionParameterType.BYTE && (((long)arg) > Byte.MAX_VALUE || ((long)arg) < Byte.MIN_VALUE)) ||
                    (arg.getClass().equals(Long.class) && targetType == PartitionParameterType.SHORT && (((long)arg) > Short.MAX_VALUE || ((long)arg) < Short.MIN_VALUE)) ||
                    (arg.getClass().equals(Long.class) && targetType == PartitionParameterType.INT && (((long)arg) > Integer.MAX_VALUE || ((long)arg) < Integer.MIN_VALUE)) ||
                    (arg.getClass().equals(Float.class) && targetType == PartitionParameterType.BYTE && (((float)arg) > Byte.MAX_VALUE || ((float)arg) < Byte.MIN_VALUE)) ||
                    (arg.getClass().equals(Float.class) && targetType == PartitionParameterType.SHORT && (((float)arg) > Short.MAX_VALUE || ((float)arg) < Short.MIN_VALUE)) ||
                    (arg.getClass().equals(Float.class) && targetType == PartitionParameterType.INT && (((float)arg) > Integer.MAX_VALUE || ((float)arg) < Integer.MIN_VALUE)) ||
                    (arg.getClass().equals(Float.class) && targetType == PartitionParameterType.LONG && (((float)arg) > Long.MAX_VALUE || ((float)arg) < Long.MIN_VALUE)) ||
                    (arg.getClass().equals(Double.class) && targetType == PartitionParameterType.BYTE && (((double)arg) > Byte.MAX_VALUE || ((double)arg) < Byte.MIN_VALUE)) ||
                    (arg.getClass().equals(Double.class) && targetType == PartitionParameterType.SHORT && (((double)arg) > Short.MAX_VALUE || ((double)arg) < Short.MIN_VALUE)) ||
                    (arg.getClass().equals(Double.class) && targetType == PartitionParameterType.INT && (((double)arg) > Integer.MAX_VALUE || ((double)arg) < Integer.MIN_VALUE)) ||
                    (arg.getClass().equals(Double.class) && targetType == PartitionParameterType.LONG && (((double)arg) > Long.MAX_VALUE || ((double)arg) < Long.MIN_VALUE)))) {
                GridTestUtils.assertThrows(log, () -> {
                    PartitionUtils.convert(arg, targetType);

                    return null;
                }, IllegalArgumentException.class, "Unable to convert arg");

                GridTestUtils.assertThrows(log, () -> {
                    H2Utils.convert(arg, idx, IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE.get(targetType));

                    return null;
                }, org.h2.message.DbException.class, "Numeric value out of range");
            }
            else if (arg instanceof Boolean && targetType == PartitionParameterType.UUID) {
                GridTestUtils.assertThrows(log, () -> {
                    PartitionUtils.convert(arg, targetType);

                    return null;
                }, IllegalArgumentException.class, "Unable to convert arg");

                GridTestUtils.assertThrows(log, () -> {
                    H2Utils.convert(arg, idx, IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE.get(targetType));

                    return null;
                }, org.h2.message.DbException.class, "Data conversion error");
            }
            else if (arg != null &&
                ((arg.getClass().equals(Double.class) && (arg.equals(Double.POSITIVE_INFINITY) || arg.equals(Double.NEGATIVE_INFINITY) || arg.equals(Double.NaN)) && targetType == PartitionParameterType.DECIMAL) ||
                    (arg.getClass().equals(Float.class) && (arg.equals(Float.POSITIVE_INFINITY) || arg.equals(Float.NEGATIVE_INFINITY) || arg.equals(Float.NaN)) && targetType == PartitionParameterType.DECIMAL))) {
                GridTestUtils.assertThrows(log, () -> {
                    PartitionUtils.convert(arg, targetType);

                    return null;
                }, IllegalArgumentException.class, "Unable to convert arg");

                GridTestUtils.assertThrows(log, () -> {
                    H2Utils.convert(arg, idx, IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE.get(targetType));

                    return null;
                }, org.h2.message.DbException.class, "Data conversion error");
            }
            else {
                Object convertationH2Res = H2Utils.convert(arg, idx,
                    IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE.get(targetType));

                Object convertationRes = PartitionUtils.convert(arg, targetType);

                if (convertationRes == null)
                    assertNull(convertationH2Res);
                else {
                    assertEquals(PARAMETER_TYPE_TO_JAVA_CLASS.get(targetType), convertationRes.getClass());
                    assertEquals(convertationH2Res.getClass(), convertationRes.getClass());
                    assertEquals(convertationH2Res, convertationRes);
                }
            }
        }
    }

}
