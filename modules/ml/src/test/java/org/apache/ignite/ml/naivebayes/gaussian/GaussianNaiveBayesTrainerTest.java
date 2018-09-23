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

package org.apache.ignite.ml.naivebayes.gaussian;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

/**
 * Tests for {@link GaussianNaiveBayesTrainer}.
 */
public class GaussianNaiveBayesTrainerTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    private static final double LABEL_1 = 1.;
    private static final double LABEL_2 = 2.;

    /** Data. */
    private static final Map<Integer, double[]> data = new HashMap<>();

    static {
        data.put(0, new double[] {1.0, 1.0, LABEL_1});
        data.put(1, new double[] {1.0, 2.0, LABEL_1});
        data.put(2, new double[] {2.0, 1.0, LABEL_1});
        data.put(3, new double[] {-1.0, -1.0, LABEL_2});
        data.put(4, new double[] {-1.0, -2.0, LABEL_2});
        data.put(5, new double[] {-2.0, -1.0, LABEL_2});
    }

    @Test
    public void fit_returnsCorrectModel() {
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer();
        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        System.out.println(model);
    }
}
