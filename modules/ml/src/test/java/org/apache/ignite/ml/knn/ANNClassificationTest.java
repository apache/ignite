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

package org.apache.ignite.ml.knn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.knn.ann.ANNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests behaviour of ANNClassificationTest. */
@RunWith(Parameterized.class)
public class ANNClassificationTest {
    /** Number of parts to be tested. */
    private static final int[] partsToBeTested = new int[]{1, 2, 3, 4, 5, 7, 100};

    /** Fixed size of Dataset. */
    private static final int AMOUNT_OF_OBSERVATIONS = 1000;

    /** Fixed size of columns in Dataset. */
    private static final int AMOUNT_OF_FEATURES = 2;

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Number of partitions. */
    @Parameterized.Parameter
    public int parts;

    /** Parameters. */
    @Parameterized.Parameters(name = "Data divided on {0} partitions, training with batch size {1}")
    public static Iterable<Integer[]> data() {
        List<Integer[]> res = new ArrayList<>();

        for (int part : partsToBeTested)
            res.add(new Integer[]{part});

        return res;
    }

    /** */
    @Test
    public void testBinaryClassificationTest() {
        Map<Integer, double[]> data = new HashMap<>();

        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();

        for (int i = 0; i < AMOUNT_OF_OBSERVATIONS; i++) {
            double x = rndX.nextDouble(500, 600);
            double y = rndY.nextDouble(500, 600);
            double[] vec = new double[AMOUNT_OF_FEATURES + 1];
            vec[0] = 0; // assign label.
            vec[1] = x;
            vec[2] = y;
            data.put(i, vec);
        }

        for (int i = AMOUNT_OF_OBSERVATIONS; i < AMOUNT_OF_OBSERVATIONS * 2; i++) {
            double x = rndX.nextDouble(-600, -500);
            double y = rndY.nextDouble(-600, -500);
            double[] vec = new double[AMOUNT_OF_FEATURES + 1];
            vec[0] = 1; // assign label.
            vec[1] = x;
            vec[2] = y;
            data.put(i, vec);
        }

        ANNClassificationTrainer trainer = new ANNClassificationTrainer()
            .withK(10);

        NNClassificationModel mdl = trainer.fit(
            data,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        ).withK(3)
            .withDistanceMeasure(new EuclideanDistance())
            .withStrategy(NNStrategy.SIMPLE);

        TestUtils.assertEquals(0, mdl.apply(VectorUtils.of(550, 550)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(-550, -550)), PRECISION);
    }
}