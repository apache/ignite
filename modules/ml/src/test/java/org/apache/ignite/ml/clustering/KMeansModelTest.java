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

package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link KMeansModel}.
 */
public class KMeansModelTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-6;

    /** */
    @Test
    public void predictClusters() {
        DistanceMeasure distanceMeasure = new EuclideanDistance();

        Vector[] centers = new DenseVector[4];

        centers[0] = new DenseVector(new double[]{1.0, 1.0});
        centers[1] = new DenseVector(new double[]{-1.0, 1.0});
        centers[2] = new DenseVector(new double[]{1.0, -1.0});
        centers[3] = new DenseVector(new double[]{-1.0, -1.0});

        KMeansModel mdl = new KMeansModel(centers, distanceMeasure);

        Assert.assertEquals(mdl.apply(new DenseVector(new double[]{1.1, 1.1})), 0.0, PRECISION);
        Assert.assertEquals(mdl.apply(new DenseVector(new double[]{-1.1, 1.1})), 1.0, PRECISION);
        Assert.assertEquals(mdl.apply(new DenseVector(new double[]{1.1, -1.1})), 2.0, PRECISION);
        Assert.assertEquals(mdl.apply(new DenseVector(new double[]{-1.1, -1.1})), 3.0, PRECISION);

        Assert.assertEquals(mdl.distanceMeasure(), distanceMeasure);
        Assert.assertEquals(mdl.amountOfClusters(), 4);
        Assert.assertArrayEquals(mdl.centers(), centers);
    }
}
