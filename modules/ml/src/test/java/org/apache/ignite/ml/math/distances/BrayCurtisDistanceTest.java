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
package org.apache.ignite.ml.math.distances;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * Evaluate BrayCurtisDistance in multiple test datasets
 */
@RunWith(Parameterized.class)
public class BrayCurtisDistanceTest {
    /** Precision. */
    private static final double PRECISION = 0.01;

    /** */
    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestData> data() {
        return Arrays.asList(
            new TestData(
                new double[] {0, 0, 0},
                new double[] {2, 1, 0},
                1.0
            ),
            new TestData(
                new double[] {1, 2, 3},
                new double[] {2, 1, 0},
                0.55
            ),
            new TestData(
                new double[] {1, 2, 3},
                new double[] {2, 1, 50},
                0.83
            ),
            new TestData(
                new double[] {1, -100, 3},
                new double[] {2, 1, -50},
                1.04
            )
        );
    }

    /** */
    private final TestData testData;

    /** */
    public BrayCurtisDistanceTest(TestData testData) {
        this.testData = testData;
    }

    /** */
    @Test
    public void testBrayCurtisDistance() {
        DistanceMeasure distanceMeasure = new BrayCurtisDistance();

        assertEquals(testData.expRes,
            distanceMeasure.compute(testData.vectorA, testData.vectorB), PRECISION);
        assertEquals(testData.expRes,
            distanceMeasure.compute(testData.vectorA, testData.vectorB), PRECISION);
    }

    /** */
    private static class TestData {
        /** */
        public final Vector vectorA;

        /** */
        public final Vector vectorB;

        /** */
        public final double expRes;

        /** */
        private TestData(double[] vectorA, double[] vectorB, double expRes) {
            this.vectorA = new DenseVector(vectorA);
            this.vectorB = new DenseVector(vectorB);
            this.expRes = expRes;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("d(%s,%s) = %s",
                Arrays.toString(vectorA.asArray()),
                Arrays.toString(vectorB.asArray()),
                expRes
            );
        }
    }
}
