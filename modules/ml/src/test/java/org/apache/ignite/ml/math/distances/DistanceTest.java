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
import java.util.List;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class DistanceTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

    /** All distace measures for distace properties tests. */
    private static final List<DistanceMeasure> DISTANCE_MEASURES = asList(
        new ChebyshevDistance(),
        new EuclideanDistance(),
        new HammingDistance(),
        new ManhattanDistance(),
        new BrayCurtisDistance(),
        new CanberraDistance(),
        new JensenShannonDistance(),
        new WeightedMinkowskiDistance(4, new DenseVector(new double[]{1, 1, 1})),
        new MinkowskiDistance(Math.random()));

    /** */
    private Vector v1;

    /** */
    private Vector v2;

    /** */
    private double[] data2;

    /** */
    @Before
    public void setup() {
        data2 = new double[] {2.0, 1.0, 0.0};
        v1 = new DenseVector(new double[] {0.0, 0.0, 0.0});
        v2 = new DenseVector(data2);
    }

    /** */
    @Test
    public void distanceFromPointToItselfIsZero() {
        DISTANCE_MEASURES.forEach(distance -> {
            Vector vector = randomVector(3);
            String errorMessage = errorMessage(distance, vector, vector);

            assertEquals(errorMessage, 0d, distance.compute(vector, vector), PRECISION);
        });
    }

    /** */
    @Test
    public void distanceFromAToBIsTheSameAsDistanceFromBToA() {
        DISTANCE_MEASURES.forEach(distance -> {
            Vector vector1 = randomVector(3);
            Vector vector2 = randomVector(3);
            String errorMessage = errorMessage(distance, vector1, vector2);

            assertEquals(errorMessage,
                distance.compute(vector1, vector2), distance.compute(vector2, vector1), PRECISION);
        });
    }

    /** */
    @Test
    public void distanceBetweenTwoDistinctPointsIsPositive() {
        DISTANCE_MEASURES.forEach(distance -> {
            Vector vector1 = randomVector(3);
            Vector vector2 = randomVector(3);
            String errorMessage = errorMessage(distance, vector1, vector2);

            assertTrue(errorMessage, distance.compute(vector1, vector2) > 0);
        });
    }

    /** */
    @Test
    public void euclideanDistance() {
        double expRes = Math.pow(5, 0.5);

        DistanceMeasure distanceMeasure = new EuclideanDistance();

        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
        assertEquals(expRes, new EuclideanDistance().compute(v1, data2), PRECISION);
    }

    /** */
    @Test
    public void manhattanDistance() {
        double expRes = 3;

        DistanceMeasure distanceMeasure = new ManhattanDistance();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void hammingDistance() {
        double expRes = 2;

        DistanceMeasure distanceMeasure = new HammingDistance();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void chebyshevDistance() {
        double expRes = 2d;

        DistanceMeasure distanceMeasure = new ChebyshevDistance();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void minkowskiDistance() {
        double expRes = Math.pow(5, 0.5);

        DistanceMeasure distanceMeasure = new MinkowskiDistance(2d);

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void brayCurtisDistance() {
        double expRes = 1.0;

        DistanceMeasure distanceMeasure = new BrayCurtisDistance();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void canberraDistance() {
        double expRes = 2.0;

        DistanceMeasure distanceMeasure = new CanberraDistance();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void jensenShannonDistance() {
        double precistion = 0.01;
        double expRes = 0.83;
        double[] pData = new double[] {1.0, 0.0, 0.0};
        Vector pV1 = new DenseVector(new double[] {0.0, 1.0, 0.0});
        Vector pV2 = new DenseVector(pData);

        DistanceMeasure distanceMeasure = new JensenShannonDistance();

        assertEquals(expRes, distanceMeasure.compute(pV1, pData), precistion);
        assertEquals(expRes, distanceMeasure.compute(pV1, pV2), precistion);
    }

    /** */
    @Test
    public void weightedMinkowskiDistance() {
        double precistion = 0.01;
        int p = 2;
        double expRes = 5.0;
        Vector v = new DenseVector(new double[]{2, 3, 4});

        DistanceMeasure distanceMeasure = new WeightedMinkowskiDistance(p, v);

        assertEquals(expRes, distanceMeasure.compute(v1, data2), precistion);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), precistion);
    }

    /** Returns a random vector */
    private static Vector randomVector(int length) {
        double[] vec = new double[length];

        for (int i = 0; i < vec.length; i++) {
            vec[i] = Math.random();
        }
        return new DenseVector(vec);
    }

    /** Creates an assertion error message from a distsnce measure and params. */
    private static String errorMessage(DistanceMeasure measure, Vector param1, Vector param2) {
        return String.format("%s(%s, %s)", measure.getClass().getSimpleName(),
            vectorToString(param1), vectorToString(param2));
    }

    /** Converts vector to string. */
    private static String vectorToString(Vector vector) {
        return "[" + Arrays.stream(vector.asArray()).boxed()
            .map(Object::toString)
            .collect(joining(",")) + "]";
    }
}
