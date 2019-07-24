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

package org.apache.ignite.ml.selection.scoring.evaluator;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Evaluator}.
 */
public class BinaryClassificationEvaluatorTest extends TrainerTest {
    /**
     * Test evalutor and trainer on classification model y = x.
     */
    @Test
    public void testEvaluatorWithoutFilter() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        KNNClassificationTrainer trainer = new KNNClassificationTrainer().withK(3);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);

        KNNClassificationModel mdl = trainer.fit(
            cacheMock, parts,
            vectorizer
        );

        double score = Evaluator.evaluate(cacheMock, mdl, vectorizer, new Accuracy<>());

        assertEquals(0.9839357429718876, score, 1e-12);
    }

    /**
     * Test evalutor and trainer on classification model y = x.
     */
    @Test
    public void testEvaluatorWithFilter() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        KNNClassificationTrainer trainer = new KNNClassificationTrainer().withK(3);


        TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>()
            .split(0.75);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);

        KNNClassificationModel mdl = trainer.fit(
            cacheMock,
            split.getTrainFilter(),
            parts,
            vectorizer
        );

        double score = Evaluator.evaluate(cacheMock, mdl, vectorizer, new Accuracy<>());

        assertEquals(0.9, score, 1);
    }
}
