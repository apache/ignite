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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link KMeansTrainer}.
 */
public class KMeansTrainerTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /**
     * A few points, one cluster, one iteration
     */
    @Test
    public void findOneClusters() {

        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {1.0, 1.0, 1.0});
        data.put(1, new double[] {1.0, 2.0, 1.0});
        data.put(2, new double[] {2.0, 1.0, 1.0});
        data.put(3, new double[] {-1.0, -1.0, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});

        KMeansTrainer trainer = new KMeansTrainer()
            .withDistance(new EuclideanDistance())
            .withK(1)
            .withMaxIterations(1)
            .withEpsilon(PRECISION);

        KMeansModel knnMdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Vector firstVector = new DenseVector(new double[] {2.0, 2.0});
        assertEquals(knnMdl.apply(firstVector), 0.0, PRECISION);
        Vector secondVector = new DenseVector(new double[] {-2.0, -2.0});
        assertEquals(knnMdl.apply(secondVector), 0.0, PRECISION);
        assertEquals(trainer.getMaxIterations(), 1);
        assertEquals(trainer.getEpsilon(), PRECISION, PRECISION);
    }
}
