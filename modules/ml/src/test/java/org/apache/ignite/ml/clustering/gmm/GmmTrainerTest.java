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

package org.apache.ignite.ml.clustering.gmm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for GMM trainer.
 */
public class GmmTrainerTest extends TrainerTest {
    /** Data. */
    private static final Map<Integer, double[]> data = new HashMap<>();

    static {
        data.put(0, new double[] {1.0, 1.0, 1.0});
        data.put(1, new double[] {1.0, 2.0, 1.0});
        data.put(2, new double[] {2.0, 1.0, 1.0});
        data.put(3, new double[] {-1.0, -1.0, 2.0});
        data.put(4, new double[] {-1.0, -2.0, 2.0});
        data.put(5, new double[] {-2.0, -1.0, 2.0});
    }

    /** */
    @Test
    public void testFit() {
        GmmTrainer trainer = new GmmTrainer(2)
            .withInitialMeans(Arrays.asList(
                VectorUtils.of(1.0, 2.0),
                VectorUtils.of(-1.0, -2.0)));
        GmmModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        Assert.assertEquals(2, model.countOfComponents());
        Assert.assertEquals(2, model.dimension());
        Assert.assertArrayEquals(new double[] {1.33, 1.33}, model.distributions().get(0).mean().asArray(), 1e-2);
        Assert.assertArrayEquals(new double[] {-1.33, -1.33}, model.distributions().get(1).mean().asArray(), 1e-2);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testOnEmptyPartition() throws Throwable {
        GmmTrainer trainer = new GmmTrainer(2)
            .withInitialMeans(Arrays.asList(VectorUtils.of(1.0, 2.0), VectorUtils.of(-1.0, -2.0)));

        try {
            trainer.fit(
                new LocalDatasetBuilder<>(new HashMap<>(), parts),
                (k, v) -> new DenseVector(2),
                (k, v) -> 1.0
            );
        }
        catch (RuntimeException e) {
            throw e.getCause();
        }
    }

    /** */
    @Test
    public void testUpdateOnEmptyDataset() {
        GmmTrainer trainer = new GmmTrainer(2)
            .withInitialMeans(Arrays.asList(
                VectorUtils.of(1.0, 2.0),
                VectorUtils.of(-1.0, -2.0)));
        GmmModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, parts),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );

        model = trainer.updateModel(model,
            new LocalDatasetBuilder<>(new HashMap<>(), parts),
            new FeatureLabelExtractor<Double, Vector, Double>() {
                private static final long serialVersionUID = -7245682432641745217L;

                @Override public LabeledVector<Double> extract(Double aDouble, Vector vector) {
                    return new LabeledVector<>(new DenseVector(2), 1.0);
                }
            }
        );

        Assert.assertEquals(2, model.countOfComponents());
        Assert.assertEquals(2, model.dimension());
        Assert.assertArrayEquals(new double[] {1.33, 1.33}, model.distributions().get(0).mean().asArray(), 1e-2);
        Assert.assertArrayEquals(new double[] {-1.33, -1.33}, model.distributions().get(1).mean().asArray(), 1e-2);
    }
}
