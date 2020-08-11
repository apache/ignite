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

import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@code CosineSimilarity}. */
public class CosineSimilarityTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

    /** */
    @Test
    public void cosineSimilarityDistance() {
        double expRes = 0.9449111825230682d;
        DenseVector a = new DenseVector(new double[] {1, 2, 3});
        double[] b = {1, 1, 4};

        DistanceMeasure distanceMeasure = new CosineSimilarity();

        assertEquals(expRes, distanceMeasure.compute(a, b), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(a, new DenseVector(b)), PRECISION);
    }
}
