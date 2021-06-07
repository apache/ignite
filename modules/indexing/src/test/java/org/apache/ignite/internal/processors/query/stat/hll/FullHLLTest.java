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
package org.apache.ignite.internal.processors.query.stat.hll;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.processors.query.stat.hll.serialization.ISchemaVersion;
import org.apache.ignite.internal.processors.query.stat.hll.serialization.SerializationUtil;
import org.apache.ignite.internal.processors.query.stat.hll.util.BitVector;
import org.apache.ignite.internal.processors.query.stat.hll.util.HLLUtil;
import org.apache.ignite.internal.processors.query.stat.hll.util.LongIterator;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link HLL} of type {@link HLLType#FULL}.
 *
 * @author rgrzywinski
 * @author timon
 */
public class FullHLLTest {
    /**
     * Smoke test for {@link HLL#cardinality()} and the proper use of the
     * small range correction.
     */
    @Test
    public void smallRangeSmokeTest() {
        final int log2m = 11;
        final int m = (1 << log2m);
        final int regwidth = 5;

        // only one register set
        {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);
            hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, 0/*ix*/, 1/*val*/));

            final long cardinality = hll.cardinality();

            // Trivially true that small correction conditions hold: one register
            // set implies zeroes exist, and estimator trivially smaller than 5m/2.
            // Small range correction: m * log(m/V)
            final long expected = (long)Math.ceil(m * Math.log((double)m / (m - 1)/*# of zeroes*/));
            assertEquals(cardinality, expected);
        }

        // all but one register set
        {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);
            for (int i = 0; i < (m - 1); i++)
                hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i/*ix*/, 1/*val*/));

            // Trivially true that small correction conditions hold: all but
            // one register set implies a zero exists, and estimator trivially
            // smaller than 5m/2 since it's alpha / ((m-1)/2)
            final long cardinality = hll.cardinality();

            // Small range correction: m * log(m/V)
            final long expected = (long)Math.ceil(m * Math.log((double)m / 1/*# of zeroes*/));
            assertEquals(cardinality, expected);
        }
    }

    /**
     * Smoke test for {@link HLL#cardinality()} and the proper use of the
     * uncorrected estimator
     */
    @Test
    public void normalRangeSmokeTest() {
        final int log2m = 11;
        final int regwidth = 5;
        // regwidth = 5, so hash space is
        // log2m + (2^5 - 1 - 1), so L = log2m + 30
        final int l = log2m + 30;
        final int m = (1 << log2m);
        final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
            256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);

        // all registers at 'medium' value
        {
            final int registerValue = 7/*chosen to ensure neither correction kicks in*/;
            for (int i = 0; i < m; i++)
                hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i, registerValue));

            final long cardinality = hll.cardinality();

            // Simplified estimator when all registers take same value: alpha / (m/2^val)
            final double estimator = HLLUtil.alphaMSquared(m) / ((double)m / Math.pow(2, registerValue));

            // Assert conditions for uncorrected range
            assertTrue(estimator <= Math.pow(2, l) / 30);
            assertTrue(estimator > (5 * m / (double)2));

            final long expected = (long)Math.ceil(estimator);
            assertEquals(cardinality, expected);
        }
    }

    /**
     * Smoke test for {@link HLL#cardinality()} and the proper use of the large
     * range correction.
     */
    @Test
    public void largeRangeSmokeTest() {
        final int log2m = 12;
        final int regwidth = 5;
        // regwidth = 5, so hash space is
        // log2m + (2^5 - 1 - 1), so L = log2m + 30
        final int l = log2m + 30;
        final int m = (1 << log2m);
        final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
            256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);

        {
            final int registerValue = 31/*chosen to ensure large correction kicks in*/;
            for (int i = 0; i < m; i++)
                hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i, registerValue));

            final long cardinality = hll.cardinality();

            // Simplified estimator when all registers take same value: alpha / (m/2^val)
            final double estimator = HLLUtil.alphaMSquared(m) / ((double)m / Math.pow(2, registerValue));

            // Assert conditions for large range

            assertTrue(estimator > Math.pow(2, l) / 30);

            // Large range correction: -2^L * log(1 - E/2^L)
            final long expected = (long)Math.ceil(-1.0 * Math.pow(2, l) * Math.log(1.0 - estimator / Math.pow(2, l)));
            assertEquals(cardinality, expected);
        }
    }

    // ========================================================================
    /**
     * Tests the bounds on a register's value for a given raw input value.
     */
    @Test
    public void registerValueTest() {
        final int log2m = 4/*small enough to make testing easy (addRaw() shifts by one byte)*/;

        // register width 4 (the minimum size)
        { // scoped locally for sanity
            final int regwidth = 4;
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);
            final BitVector bitVector = GridTestUtils.getFieldValue(hll, "probabilisticStorage");
            //(BitVector)getInternalState(hll, "probabilisticStorage");/*for testing convenience*/

            // lower-bounds of the register
            hll.addRaw(0x000000000000001L/*'j'=1*/);
            assertEquals(bitVector.getRegister(1/*'j'*/), 0);

            hll.addRaw(0x0000000000000012L/*'j'=2*/);
            assertEquals(bitVector.getRegister(2/*'j'*/), 1);

            hll.addRaw(0x0000000000000023L/*'j'=3*/);
            assertEquals(bitVector.getRegister(3/*'j'*/), 2);

            hll.addRaw(0x0000000000000044L/*'j'=4*/);
            assertEquals(bitVector.getRegister(4/*'j'*/), 3);

            hll.addRaw(0x0000000000000085L/*'j'=5*/);
            assertEquals(bitVector.getRegister(5/*'j'*/), 4);

            // upper-bounds of the register
            // NOTE:  bear in mind that BitVector itself does ensure that
            //        overflow of a register is prevented
            hll.addRaw(0x0000000000010006L/*'j'=6*/);
            assertEquals(bitVector.getRegister(6/*'j'*/), 13);

            hll.addRaw(0x0000000000020007L/*'j'=7*/);
            assertEquals(bitVector.getRegister(7/*'j'*/), 14);

            hll.addRaw(0x0000000000040008L/*'j'=8*/);
            assertEquals(bitVector.getRegister(8/*'j'*/), 15);

            hll.addRaw(0x0000000000080009L/*'j'=9*/);
            assertEquals(bitVector.getRegister(9/*'j'*/), 15/*overflow*/);

            // sanity checks to ensure that no other bits above the lowest-set
            // bit matters
            // NOTE:  same as case 'j = 6' above
            hll.addRaw(0x000000000003000AL/*'j'=10*/);
            assertEquals(bitVector.getRegister(10/*'j'*/), 13);

            hll.addRaw(0x000000000011000BL/*'j'=11*/);
            assertEquals(bitVector.getRegister(11/*'j'*/), 13);
        }

        // register width 5
        { // scoped locally for sanity
            final int regwidth = 5;
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);
            final BitVector bitVector = GridTestUtils.getFieldValue(hll, "probabilisticStorage");

            // lower-bounds of the register
            hll.addRaw(0x0000000000000001L/*'j'=1*/);
            assertEquals(bitVector.getRegister(1/*'j'*/), 0);

            hll.addRaw(0x0000000000000012L/*'j'=2*/);
            assertEquals(bitVector.getRegister(2/*'j'*/), 1);

            hll.addRaw(0x0000000000000023L/*'j'=3*/);
            assertEquals(bitVector.getRegister(3/*'j'*/), 2);

            hll.addRaw(0x0000000000000044L/*'j'=4*/);
            assertEquals(bitVector.getRegister(4/*'j'*/), 3);

            hll.addRaw(0x0000000000000085L/*'j'=5*/);
            assertEquals(bitVector.getRegister(5/*'j'*/), 4);

            // upper-bounds of the register
            // NOTE:  bear in mind that BitVector itself does ensure that
            //        overflow of a register is prevented
            hll.addRaw(0x0000000100000006L/*'j'=6*/);
            assertEquals(bitVector.getRegister(6/*'j'*/), 29);

            hll.addRaw(0x0000000200000007L/*'j'=7*/);
            assertEquals(bitVector.getRegister(7/*'j'*/), 30);

            hll.addRaw(0x0000000400000008L/*'j'=8*/);
            assertEquals(bitVector.getRegister(8/*'j'*/), 31);

            hll.addRaw(0x0000000800000009L/*'j'=9*/);
            assertEquals(bitVector.getRegister(9/*'j'*/), 31/*overflow*/);
        }
    }

    // ========================================================================
    /**
     * Tests {@link HLL#clear()}.
     */
    @Test
    public void clearTest() {
        final int regwidth = 5;
        final int log2m = 4/*16 registers per counter*/;
        final int m = 1 << log2m;

        final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
            256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);
        final BitVector bitVector = GridTestUtils.getFieldValue(hll, "probabilisticStorage")
            /*for testing convenience*/;
        for (int i = 0; i < m; i++)
            bitVector.setRegister(i, i);

        hll.clear();
        for (int i = 0; i < m; i++)
            assertEquals(bitVector.getRegister(i), 0L/*default value of register*/);
    }

    // ========================================================================
    // Serialization
    /**
     * Tests {@link HLL#toBytes(ISchemaVersion)} and {@link HLL#fromBytes(byte[])}.
     */
    @Test
    public void toFromBytesTest() {
        final int log2m = 11/*arbitrary*/;
        final int regwidth = 5;

        final ISchemaVersion schemaVersion = SerializationUtil.DEFAULT_SCHEMA_VERSION;
        final HLLType type = HLLType.FULL;
        final int padding = schemaVersion.paddingBytes(type);
        final int dataByteCount = ProbabilisticTestUtil.getRequiredBytes(regwidth, (1 << log2m)/*aka 2^log2m = m*/);
        final int expectedByteCount = padding + dataByteCount;

        {
            // Should work on an empty element
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);
            final byte[] bytes = hll.toBytes(schemaVersion);

            // assert output length is correct
            assertEquals(bytes.length, expectedByteCount);

            final HLL inHLL = HLL.fromBytes(bytes);

            // assert register values correct
            assertElementsEqual(hll, inHLL);
        }
        {
            // Should work on a partially filled element
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);

            for (int i = 0; i < 3; i++) {
                final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, i, (i + 9));
                hll.addRaw(rawValue);
            }

            final byte[] bytes = hll.toBytes(schemaVersion);

            // assert output length is correct
            assertEquals(bytes.length, expectedByteCount);

            final HLL inHLL = HLL.fromBytes(bytes);

            // assert register values correct
            assertElementsEqual(hll, inHLL);
        }
        {
            // Should work on a full set
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/,
                256/*sparseThreshold, arbitrary, unused*/, HLLType.FULL);

            for (int i = 0; i < (1 << log2m)/*aka 2^log2m*/; i++) {
                final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, i, (i % 9) + 1);
                hll.addRaw(rawValue);
            }

            final byte[] bytes = hll.toBytes(schemaVersion);

            // assert output length is correct
            assertEquals(bytes.length, expectedByteCount);

            final HLL inHLL = HLL.fromBytes(bytes);

            // assert register values correct
            assertElementsEqual(hll, inHLL);
        }
    }

    /**
     *
     */
    @Test
    public void unionTest() {
        Random r = ThreadLocalRandom.current();
        HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);

        hll.addRaw(r.nextLong());
    }

    /**
     *
     */
    @Test
    public void homogeneousUnionTest() {
        getHll(1).union((getHll(1)));

        getHll(1000).union((getHll(1000)));

        getHll(100000).union((getHll(100000)));
    }


    /**
     * Test empty vs full in different combination unions.
     */
    @Test
    public void emptyUnionTest() {
        getHll(0).union(getHll(0));

        getHll(1000).union(getHll(0));

        getHll(0).union(getHll(1000));
    }

    /**
     * Test HLL with different number of values serialized/deserialized properly.
     */
    @Test
    public void serializationTest() {
        testSerialization(getHll(0));

        testSerialization(getHll(1));

        testSerialization(getHll(1000));

        testSerialization(getHll(100000));
    }

    /**
     *
     * @param originHll
     */
    private static void testSerialization(HLL originHll) {
        byte[] hllBytes = originHll.toBytes();
        HLL restoredHll = HLL.fromBytes(hllBytes);

        assertElementsEqual(originHll, restoredHll);
        assertEquals(originHll.cardinality(), restoredHll.cardinality());
        assertEquals(originHll.getType(), restoredHll.getType());
    }

    /**
     * Get HLL with specified number of random values added.
     *
     * @param rows
     * @return
     */
    private static HLL getHll(int rows) {
        Random r = ThreadLocalRandom.current();
        HLL result = new HLL(13, 5);
        for (int i = 0; i < rows; i++)
            result.addRaw(r.nextLong());
        return result;
    }

    // ************************************************************************
    // Assertion Helpers
    /**
     * Asserts that the two HLLs are register-wise equal.
     */
    private static void assertElementsEqual(final HLL hllA, final HLL hllB) {
        /*for testing convenience*/
        final BitVector bitVectorA = GridTestUtils.getFieldValue(hllA, "probabilisticStorage");
        /*for testing convenience*/
        final BitVector bitVectorB = GridTestUtils.getFieldValue(hllB, "probabilisticStorage");

        if (bitVectorA == null && bitVectorB == null)
            return;

        final LongIterator iterA = bitVectorA.registerIterator();
        final LongIterator iterB = bitVectorB.registerIterator();

        while (iterA.hasNext() && iterB.hasNext())
            assertEquals(iterA.next(), iterB.next());

        assertFalse(iterA.hasNext());
        assertFalse(iterB.hasNext());
    }
}
