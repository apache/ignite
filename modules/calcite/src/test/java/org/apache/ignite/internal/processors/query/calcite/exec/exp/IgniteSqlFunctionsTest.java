/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class IgniteSqlFunctionsTest {
    /** CYCLE path membership follows SQL row equality, including nested ROW and collection fields. */
    @Test
    public void testCyclePathContains() {
        assertTrue(IgniteSqlFunctions.cyclePathContains(Collections.singletonList(new Object[] {1, "x"}),
            new Object[] {1, "x"}));
        assertFalse(IgniteSqlFunctions.cyclePathContains(Collections.singletonList(new Object[] {1, "x"}),
            new Object[] {1, "y"}));
        assertFalse(IgniteSqlFunctions.cyclePathContains(Collections.singletonList(new Object[] {null, 1}),
            new Object[] {null, 1}));
        assertFalse(IgniteSqlFunctions.cyclePathContains(Collections.singletonList(new Object[] {new Object[] {null}}),
            new Object[] {new Object[] {null}}));
        assertTrue(IgniteSqlFunctions.cyclePathContains(Collections.singletonList(new Object[] {Arrays.asList(1, null)}),
            new Object[] {Arrays.asList(1, null)}));
        assertTrue(IgniteSqlFunctions.cyclePathContains(Arrays.asList(new Object[] {null}, new Object[] {1}),
            new Object[] {1}));
        assertFalse(IgniteSqlFunctions.cyclePathContains(Collections.emptyList(), new Object[] {1}));
    }

    /** */
    @Test
    public void testBigDecimalToString() {
        assertNull(IgniteSqlFunctions.toString((BigDecimal)null));

        assertEquals(
            "10",
            IgniteSqlFunctions.toString(BigDecimal.valueOf(10))
        );

        assertEquals(
            "9223372036854775807",
            IgniteSqlFunctions.toString(BigDecimal.valueOf(Long.MAX_VALUE))
        );

        assertEquals(
            "340282350000000000000000000000000000000",
            IgniteSqlFunctions.toString(new BigDecimal(String.valueOf(Float.MAX_VALUE)))
        );

        assertEquals(
            "-340282346638528860000000000000000000000",
            IgniteSqlFunctions.toString(BigDecimal.valueOf(-Float.MAX_VALUE))
        );
    }

    /** */
    @Test
    public void testBooleanPrimitiveToBigDecimal() {
        assertThrows(UnsupportedOperationException.class, () -> IgniteSqlFunctions.toBigDecimal(true, 10, 10));
    }

    /** */
    @Test
    public void testBooleanObjectToBigDecimal() {
        assertThrows(UnsupportedOperationException.class, () -> IgniteSqlFunctions.toBigDecimal(Boolean.valueOf(true), 10, 10));
    }

    /** */
    @Test
    public void testPrimitiveToDecimal() {
        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal((byte)10, 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal((short)10, 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(10, 10, 0)
        );

        assertEquals(
            new BigDecimal("10.0"),
            IgniteSqlFunctions.toBigDecimal(10L, 10, 1)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(10.101f, 10, 3)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(10.101d, 10, 3)
        );
    }

    /** */
    @Test
    public void testObjectToDecimal() {
        assertNull(IgniteSqlFunctions.toBigDecimal((Object)null, 10, 0));

        assertNull(IgniteSqlFunctions.toBigDecimal((Double)null, 10, 0));

        assertNull(IgniteSqlFunctions.toBigDecimal((String)null, 10, 0));

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(Byte.valueOf("10"), 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(Short.valueOf("10"), 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(Integer.valueOf(10), 10, 0)
        );

        assertEquals(
            new BigDecimal("10.0"),
            IgniteSqlFunctions.toBigDecimal(Long.valueOf(10L), 10, 1)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(Float.valueOf(10.101f), 10, 3)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(Double.valueOf(10.101d), 10, 3)
        );
    }
}
