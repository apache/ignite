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

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** */
public class DistanceTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

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
    public void euclideanDistance() {
        double expRes = Math.pow(5, 0.5);

        DistanceMeasure distanceMeasure = new EuclideanDistance();

        Assert.assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);

        Assert.assertEquals(expRes, new EuclideanDistance().compute(v1, data2), PRECISION);
    }

    /** */
    @Test
    public void manhattanDistance() {
        double expRes = 3;

        DistanceMeasure distanceMeasure = new ManhattanDistance();

        Assert.assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test
    public void hammingDistance() {
        double expRes = 2;

        DistanceMeasure distanceMeasure = new HammingDistance();

        Assert.assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void manhattanDistance2() {
        new ManhattanDistance().compute(v1, data2);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void hammingDistance2() {
        new HammingDistance().compute(v1, data2);
    }
}
