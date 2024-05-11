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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/** */
@RunWith(Parameterized.class)
public class VectorNormCasesTest {
    /**
     * Precision.
     */
    private static final double PRECISION = 0.01;

    /** */
    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestData> data() {
        return Arrays.asList(
            new TestData(
                new double[] {1.0, -1.0, 0.0},
                1,
                2.0
            ),
            new TestData(
                new double[] {1.0, -1.0, 0.0},
                2,
                1.41
            ),
            new TestData(
                new double[] {1.0, -1.0, 0.0},
                3,
                1.25
            ),
            new TestData(
                new double[] {1.0, -1.0, 0.0},
                4,
                1.18
            ),
            new TestData(
                new double[] {1.0, -1.0, 0.0},
                5,
                1.14
            )
        );
    }

    /** */
    private final TestData testData;

    /** */
    public VectorNormCasesTest(TestData testData) {
        this.testData = testData;
    }

    /** */
    @Test
    public void test() {
        assertEquals(
            testData.vector.kNorm(testData.p),
            testData.expRes,
            PRECISION
        );
    }

    /** */
    private static class TestData {
        /** */
        public final Vector vector;

        /** */
        public final Double p;

        /** */
        public final Double expRes;

        /** */
        private TestData(double[] vector, double p, double expRes) {
            this.vector = new DenseVector(vector);
            this.p = p;
            this.expRes = expRes;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("norm(%s, %s) = %s",
                Arrays.toString(vector.asArray()),
                p,
                expRes
            );
        }
    }
}
