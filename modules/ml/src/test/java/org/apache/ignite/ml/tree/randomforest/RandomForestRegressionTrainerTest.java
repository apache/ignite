/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.tree.randomforest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link RandomForestRegressionTrainer}.
 */
public class RandomForestRegressionTrainerTest extends TrainerTest {
    /** */
    @Test
    public void testFit() {
        int sampleSize = 1000;
        Map<Double, double[]> sample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++) {
            double x1 = i;
            double x2 = x1 / 10.0;
            double x3 = x2 / 10.0;
            double x4 = x3 / 10.0;

            sample.put(x1 * x2 + x3 * x4, new double[] {x1, x2, x3, x4});
        }

        ArrayList<FeatureMeta> meta = new ArrayList<>();
        for(int i = 0; i < 4; i++)
            meta.add(new FeatureMeta("", i, false));
        RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(meta)
            .withAmountOfTrees(5)
            .withFeaturesCountSelectionStrgy(x -> 2);

        ModelsComposition mdl = trainer.fit(sample, parts, (k, v) -> VectorUtils.of(v), (k, v) -> k);
        assertTrue(mdl.getPredictionsAggregator() instanceof MeanValuePredictionsAggregator);
        assertEquals(5, mdl.getModels().size());
    }

    /** */
    @Test
    public void testUpdate() {
        int sampleSize = 1000;
        Map<double[], Double> sample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++) {
            double x1 = i;
            double x2 = x1 / 10.0;
            double x3 = x2 / 10.0;
            double x4 = x3 / 10.0;

            sample.put(new double[] {x1, x2, x3, x4}, (double)(i % 2));
        }

        ArrayList<FeatureMeta> meta = new ArrayList<>();
        for (int i = 0; i < 4; i++)
            meta.add(new FeatureMeta("", i, false));
        RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(meta)
            .withAmountOfTrees(100)
            .withFeaturesCountSelectionStrgy(x -> 2);

        ModelsComposition originalMdl = trainer.fit(sample, parts, (k, v) -> VectorUtils.of(k), (k, v) -> v);
        ModelsComposition updatedOnSameDS = trainer.update(originalMdl, sample, parts, (k, v) -> VectorUtils.of(k), (k, v) -> v);
        ModelsComposition updatedOnEmptyDS = trainer.update(originalMdl, new HashMap<double[], Double>(), parts, (k, v) -> VectorUtils.of(k), (k, v) -> v);

        Vector v = VectorUtils.of(5, 0.5, 0.05, 0.005);
        assertEquals(originalMdl.predict(v), updatedOnSameDS.predict(v), 0.1);
        assertEquals(originalMdl.predict(v), updatedOnEmptyDS.predict(v), 0.1);
    }
}
