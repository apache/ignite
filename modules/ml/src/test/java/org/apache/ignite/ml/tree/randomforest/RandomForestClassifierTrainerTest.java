/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.tree.randomforest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link RandomForestClassifierTrainer}.
 */
public class RandomForestClassifierTrainerTest extends TrainerTest {
    /** */
    @Test
    public void testFit() {
        int sampleSize = 500;
        Map<Integer, LabeledVector<Double>> sample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++) {
            double x1 = i;
            double x2 = x1 / 10.0;
            double x3 = x2 / 10.0;
            double x4 = x3 / 10.0;

            sample.put(i, VectorUtils.of(x1, x2, x3, x4).labeled((double) i % 2));
        }

        ArrayList<FeatureMeta> meta = new ArrayList<>();
        for (int i = 0; i < 4; i++)
            meta.add(new FeatureMeta("", i, false));
        DatasetTrainer<ModelsComposition, Double> trainer = new RandomForestClassifierTrainer(meta)
            .withAmountOfTrees(5)
            .withFeaturesCountSelectionStrgy(x -> 2)
            .withEnvironmentBuilder(TestUtils.testEnvBuilder());

        ModelsComposition mdl = trainer.fit(sample, parts, new LabeledDummyVectorizer<>());

        assertTrue(mdl.getPredictionsAggregator() instanceof OnMajorityPredictionsAggregator);
        assertEquals(5, mdl.getModels().size());
    }

    /** */
    @Test
    public void testUpdate() {
        int sampleSize = 500;
        Map<Integer, LabeledVector<Double>> sample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++) {
            double x1 = i;
            double x2 = x1 / 10.0;
            double x3 = x2 / 10.0;
            double x4 = x3 / 10.0;

            sample.put(i, VectorUtils.of(x1, x2, x3, x4).labeled((double) i % 2));
        }

        ArrayList<FeatureMeta> meta = new ArrayList<>();
        for (int i = 0; i < 4; i++)
            meta.add(new FeatureMeta("", i, false));
        DatasetTrainer<ModelsComposition, Double> trainer = new RandomForestClassifierTrainer(meta)
            .withAmountOfTrees(32)
            .withFeaturesCountSelectionStrgy(x -> 2)
            .withEnvironmentBuilder(TestUtils.testEnvBuilder());

        ModelsComposition originalMdl = trainer.fit(sample, parts, new LabeledDummyVectorizer<>());
        ModelsComposition updatedOnSameDS = trainer.update(originalMdl, sample, parts, new LabeledDummyVectorizer<>());
        ModelsComposition updatedOnEmptyDS = trainer.update(originalMdl, new HashMap<Integer, LabeledVector<Double>>(), parts, new LabeledDummyVectorizer<>());

        Vector v = VectorUtils.of(5, 0.5, 0.05, 0.005);
        assertEquals(originalMdl.predict(v), updatedOnSameDS.predict(v), 0.01);
        assertEquals(originalMdl.predict(v), updatedOnEmptyDS.predict(v), 0.01);
    }
}
