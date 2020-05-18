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
import java.util.EnumSet;
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
        makeSureThatConvertationResultsExactTheSameAsWithinH2(null);
    }

    /**
     * Test boolean conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertBoolean() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Boolean.TRUE);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Boolean.FALSE);
    }

    /**
     * Test byte conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertByte() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2((byte)42, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2((byte)0, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Byte.MIN_VALUE, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Byte.MAX_VALUE, PartitionParameterType.UUID);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert((byte)42, PartitionParameterType.UUID));
    }

    /**
     * Test short conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertShort() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2((short)42, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2((short)0, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Short.MIN_VALUE, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Short.MAX_VALUE, PartitionParameterType.UUID);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert((short)42, PartitionParameterType.UUID));
    }

    /**
     * Test int conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertInteger() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(42, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(0, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Integer.MIN_VALUE, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Integer.MAX_VALUE, PartitionParameterType.UUID);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert(42, PartitionParameterType.UUID));
    }

    /**
     * Test long conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertLong() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(42L, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(0L, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Long.MIN_VALUE, PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Long.MAX_VALUE, PartitionParameterType.UUID);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert(42L, PartitionParameterType.UUID));
    }

    /**
     * Test float conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertFloat() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(42.1f);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(0.1f);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(0f);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(1.2345678E7f);

        makeSureThatConvertationResultsExactTheSameAsWithinH2(Float.POSITIVE_INFINITY);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Float.NEGATIVE_INFINITY);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Float.NaN);

        makeSureThatConvertationResultsExactTheSameAsWithinH2(Float.MIN_VALUE);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Float.MAX_VALUE);
    }

    /**
     * Test double conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertDouble() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(42.2d);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(0.2d);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(0d);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(1.2345678E7d);

        makeSureThatConvertationResultsExactTheSameAsWithinH2(Double.POSITIVE_INFINITY);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Double.NEGATIVE_INFINITY);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Double.NaN);

        makeSureThatConvertationResultsExactTheSameAsWithinH2(Double.MIN_VALUE);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(Double.MAX_VALUE);
    }

    /**
     * Test string conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertString() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2("42", PartitionParameterType.BOOLEAN);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert("42", PartitionParameterType.BOOLEAN));

        makeSureThatConvertationResultsExactTheSameAsWithinH2("0");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("1");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("42.3", PartitionParameterType.BOOLEAN);
        makeSureThatConvertationResultsExactTheSameAsWithinH2("0.3", PartitionParameterType.BOOLEAN);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert("0.3", PartitionParameterType.BOOLEAN));

        makeSureThatConvertationResultsExactTheSameAsWithinH2("42.4f");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("0.4d");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("12345678901234567890.123456789012345678901d");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("04d17cf3-bc20-4e3d-9ff7-72437cdae227");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("04d17cf3bc204e3d9ff772437cdae227");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("a");

        makeSureThatConvertationResultsExactTheSameAsWithinH2(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("aaa",
            PartitionParameterType.BOOLEAN);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(" aaa ");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("true");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("t");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("yes");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("y");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("false");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("f");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("no");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("n");

        makeSureThatConvertationResultsExactTheSameAsWithinH2(" true ");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("null");
        makeSureThatConvertationResultsExactTheSameAsWithinH2("NULL");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("2000-01-02");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("10:00:00");

        makeSureThatConvertationResultsExactTheSameAsWithinH2("2001-01-01 23:59:59.123456");
    }

    /**
     * Test decimal conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertDecimal() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(new BigDecimal(42.5d), PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(new BigDecimal(0.5d), PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(new BigDecimal(0), PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(new BigDecimal(1.2345678E7),
            PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(BigDecimal.valueOf(12334535345456700.12345634534534578901),
            PartitionParameterType.UUID);

        makeSureThatConvertationResultsExactTheSameAsWithinH2(new BigDecimal(Double.MIN_VALUE),
            PartitionParameterType.UUID);
        makeSureThatConvertationResultsExactTheSameAsWithinH2(new BigDecimal(Double.MAX_VALUE),
            PartitionParameterType.UUID);

        assertEquals(PartitionDataTypeUtils.CONVERTATION_FAILURE,
            PartitionDataTypeUtils.convert(new BigDecimal(42.5d), PartitionParameterType.UUID));
    }

    /**
     * Test uuid conversion.
     *
     * @throws Exception If failed.
     */
    @Test
    public void convertUUID() throws Exception {
        makeSureThatConvertationResultsExactTheSameAsWithinH2(UUID.randomUUID());
        makeSureThatConvertationResultsExactTheSameAsWithinH2(UUID.fromString("00000000-0000-0000-0000-00000000000a"));
        makeSureThatConvertationResultsExactTheSameAsWithinH2(new UUID(0L, 1L));
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
     * @param excludedTargetTypesFromCheck <@code>PartitionParameterType</code> instances to exclude from check.
     * @throws Exception If failed.
     */
    private void makeSureThatConvertationResultsExactTheSameAsWithinH2(Object arg,
        PartitionParameterType... excludedTargetTypesFromCheck) throws Exception {

        Iterable<PartitionParameterType> targetTypes = excludedTargetTypesFromCheck.length > 0 ?
            EnumSet.complementOf(EnumSet.of(excludedTargetTypesFromCheck[0], excludedTargetTypesFromCheck)) :
            EnumSet.allOf(PartitionParameterType.class);

        for (PartitionParameterType targetType : targetTypes) {
            Object convertationRes = PartitionDataTypeUtils.convert(arg, targetType);

            if (PartitionDataTypeUtils.CONVERTATION_FAILURE == convertationRes) {
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
