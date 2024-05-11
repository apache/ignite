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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@code JaccardIndex}. */
public class JaccardIndexTest {
    /** Precision. */
    private static final double PRECISION = 0.0;

    /** */
    @Test
    public void jaccardIndex() {
        double expRes = 0.2;
        double[] data2 = new double[] {2.0, 1.0, 0.0};
        Vector v1 = new DenseVector(new double[] {0.0, 0.0, 0.0});
        Vector v2 = new DenseVector(data2);

        DistanceMeasure distanceMeasure = new JaccardIndex();

        assertEquals(expRes, distanceMeasure.compute(v1, data2), PRECISION);
        assertEquals(expRes, distanceMeasure.compute(v1, v2), PRECISION);
    }
}
