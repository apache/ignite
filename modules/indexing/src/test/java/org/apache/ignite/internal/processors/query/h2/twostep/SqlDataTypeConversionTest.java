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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionDataTypeUtils;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionParameterType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.value.Value;
import org.junit.Test;

/**
 * Data conversion tests.
 */
public class SqlDataTypeConversionTest extends GridCommonAbstractTest {
    /** Map to convert <code>PartitionParameterType</code> instances to correspondig java classes. */
    private static final Map<PartitionParameterType, Class<?>> PARAMETER_TYPE_TO_JAVA_CLASS;

    /** Map to convert <code>PartitionParameterType</code> instances to correspondig H2 data types. */
    private static final Map<PartitionParameterType, Integer> IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE;

    /** Ignite H2 Indexing. */
    private static IgniteH2Indexing idx;

    static {
        Map<PartitionParameterType, Class<?>> paramTypeToJavaCls = new EnumMap<>(PartitionParameterType.class);

        paramTypeToJavaCls.put(PartitionParameterType.BOOLEAN, Boolean.class);
        paramTypeToJavaCls.put(PartitionParameterType.BYTE, Byte.class);
        paramTypeToJavaCls.put(PartitionParameterType.SHORT, Short.class);
        paramTypeToJavaCls.put(PartitionParameterType.INT, Integer.class);
        paramTypeToJavaCls.put(PartitionParameterType.LONG, Long.class);
        paramTypeToJavaCls.put(PartitionParameterType.FLOAT, Float.class);
        paramTypeToJavaCls.put(PartitionParameterType.DOUBLE, Double.class);
        paramTypeToJavaCls.put(PartitionParameterType.STRING, String.class);
        paramTypeToJavaCls.put(PartitionParameterType.DECIMAL, BigDecimal.class);
        paramTypeToJavaCls.put(PartitionParameterType.UUID, UUID.class);

        PARAMETER_TYPE_TO_JAVA_CLASS = Collections.unmodifiableMap(paramTypeToJavaCls);

        Map<PartitionParameterType, Integer> igniteParamTypeToH2ParamType = new EnumMap<>(PartitionParameterType.class);

        igniteParamTypeToH2ParamType.put(PartitionParameterType.BOOLEAN, Value.BOOLEAN);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.BYTE, Value.BYTE);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.SHORT, Value.SHORT);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.INT, Value.INT);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.LONG, Value.LONG);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.FLOAT, Value.FLOAT);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.DOUBLE, Value.DOUBLE);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.STRING, Value.STRING);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.DECIMAL, Value.DECIMAL);
        igniteParamTypeToH2ParamType.put(PartitionParameterType.UUID, Value.UUID);

        IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE = Collections.unmodifiableMap(igniteParamTypeToH2ParamType);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        idx = (IgniteH2Indexing)startGrid(0).context().query().getIndexing();
    }

    /**
     * Test null value conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertNull() throws Exception {
        checkConvertation(null);
    }

    /**
     * Test boolean conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertBoolean() throws Exception {
        checkConvertation(Boolean.TRUE);
        checkConvertation(Boolean.FALSE);
    }

    /**
     * Test byte conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertByte() throws Exception {
        checkConvertation((byte)42);
        checkConvertation((byte)0);
        checkConvertation(Byte.MIN_VALUE);
        checkConvertation(Byte.MAX_VALUE);
    }

    /**
     * Test short conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertShort() throws Exception {
        checkConvertation((short)42);
        checkConvertation((short)0);
        checkConvertation(Short.MIN_VALUE);
        checkConvertation(Short.MAX_VALUE);
    }

    /**
     * Test int conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertInteger() throws Exception {
        checkConvertation(42);
        checkConvertation(0);
        checkConvertation(Integer.MIN_VALUE);
        checkConvertation(Integer.MAX_VALUE);
    }

    /**
     * Test long conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertLong() throws Exception {
        checkConvertation(42L);
        checkConvertation(0L);
        checkConvertation(Long.MIN_VALUE);
        checkConvertation(Long.MAX_VALUE);
    }

    /**
     * Test float conversion.
     *
     * @throws Exception If failed.
     */
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

    /**
     * Test double conversion.
     *
     * @throws Exception If failed.
     */
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

    /**
     * Test string conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertString() throws Exception {
        checkConvertation("42");
        checkConvertation("0");
        checkConvertation("1");

        checkConvertation("42.3");
        checkConvertation("0.3");

        checkConvertation("42.4f");
        checkConvertation("0.4d");
        checkConvertation("12345678901234567890.123456789012345678901d");

        checkConvertation("04d17cf3-bc20-4e3d-9ff7-72437cdae227");
        checkConvertation("04d17cf3bc204e3d9ff772437cdae227");

        checkConvertation("a");

        checkConvertation("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        checkConvertation("aaa");
        checkConvertation(" aaa ");

        checkConvertation("true");
        checkConvertation("t");
        checkConvertation("yes");
        checkConvertation("y");
        checkConvertation("false");
        checkConvertation("f");
        checkConvertation("no");
        checkConvertation("n");

        checkConvertation(" true ");

        checkConvertation("null");
        checkConvertation("NULL");

        checkConvertation("2000-01-02");

        checkConvertation("10:00:00");

        checkConvertation("2001-01-01 23:59:59.123456");
    }

    /**
     * Test decimal conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertDecimal() throws Exception {
        checkConvertation(new BigDecimal(42.5d));
        checkConvertation(new BigDecimal(0.5d));
        checkConvertation(new BigDecimal(0));
        checkConvertation(new BigDecimal(1.2345678E7));
        checkConvertation(BigDecimal.valueOf(12334535345456700.12345634534534578901));

        checkConvertation(new BigDecimal(Double.MIN_VALUE));
        checkConvertation(new BigDecimal(Double.MAX_VALUE));
    }

    /**
     * Test uuid conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertUUID() throws Exception {
        checkConvertation(UUID.randomUUID());
        checkConvertation(UUID.fromString("00000000-0000-0000-0000-00000000000a"));
        checkConvertation(new UUID(0L, 1L));
    }

    /**
     * Test string to uuid conversion with different combinations of upper and lower case letters and with/without
     * hyphens.
     */
    @Test
    public void stringToUUIDConvertation() {
        UUID expUuid = UUID.fromString("273ded0d-86de-432e-b252-54c06ec22927");

        // Lower case and hyphens.
        assertEquals(expUuid, PartitionDataTypeUtils.stringToUUID("273ded0d-86de-432e-b252-54c06ec22927"));

        // Lower case without hyphens.
        assertEquals(expUuid, PartitionDataTypeUtils.stringToUUID("273ded0d86de432eb25254c06ec22927"));

        // Lower case without few hyphens.
        assertEquals(expUuid, PartitionDataTypeUtils.stringToUUID("273ded0d86de432e-b25254c06ec22927"));

        // Upper case and hyphens.
        assertEquals(expUuid, PartitionDataTypeUtils.stringToUUID("273dED0D-86DE-432E-B25254C06EC22927"));

        // Upper case without few hyphens.
        assertEquals(expUuid, PartitionDataTypeUtils.stringToUUID("273dED0D86DE432Eb252-54c06ec22927"));
    }

    /**
     * Actual conversial check logic.
     *
     * @param arg Argument to convert.
     * @throws Exception If failed.
     */
    private void checkConvertation(Object arg) throws Exception {
        for (PartitionParameterType targetType : PartitionParameterType.values()) {
            Object convertationRes = PartitionDataTypeUtils.convert(arg, targetType);

            if (PartitionDataTypeUtils.CONVERTATION_FAILURE == convertationRes) {
                // Conversion rules for string-to-boolean and every-type-except-string-to-uuid differs in Ignite and H2,
                // so that we might return CONVERTATION_FAILURE
                // whereas H2 will convert types without exception (but not vice versa!).
                // For example in context of string-to-boolean conversion
                // Ignite "2" -> CONVERTATION_FAILURE whereas in H2 "2" -> true.
                // Besides that H2 might convert byte, int, etc to UUID, which is not support
                // sby Ignite client side conversion.
                if ((arg instanceof String && targetType == PartitionParameterType.BOOLEAN) ||
                    (!(arg instanceof String) && targetType == PartitionParameterType.UUID))
                    continue;

                try {
                    H2Utils.convert(arg, idx, IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE.get(targetType));

                    fail("Data conversion failed in Ignite but not in H2.");
                }
                catch (org.h2.message.DbException h2Exception) {
                    assertTrue(h2Exception.getMessage().contains("Numeric value out of range") ||
                        h2Exception.getMessage().contains("Data conversion error"));
                }
            }
            else {
                Object convertationH2Res = H2Utils.convert(arg, idx,
                    IGNITE_PARAMETER_TYPE_TO_H2_PARAMETER_TYPE.get(targetType));

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
