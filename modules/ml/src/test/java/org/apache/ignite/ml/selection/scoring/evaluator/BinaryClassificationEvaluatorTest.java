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

package org.apache.ignite.ml.selection.scoring.evaluator;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Evaluator}.
 */
public class BinaryClassificationEvaluatorTest extends TrainerTest {
    /**
     * Test evaluator and trainer on classification model y = x.
     */
    @Test
    public void testEvaluatorWithoutFilter() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        KNNClassificationTrainer trainer = new KNNClassificationTrainer().withK(3);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        KNNClassificationModel mdl = trainer.fit(
            cacheMock, parts,
            vectorizer
        );

        double score = Evaluator.evaluate(cacheMock, mdl, vectorizer, MetricName.ACCURACY);

        assertEquals(0.9919839679358717, score, 1e-12);
    }

    /**
     * Test evaluator and trainer on classification model y = x.
     */
    @Test
    public void testEvaluatorWithFilter() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        KNNClassificationTrainer trainer = new KNNClassificationTrainer().withK(3);

        TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>(new SHA256UniformMapper<>(new Random(100)))
            .split(0.75);

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        KNNClassificationModel mdl = trainer.fit(
            cacheMock,
            split.getTrainFilter(),
            parts,
            vectorizer
        );

        double score = Evaluator.evaluate(cacheMock, split.getTestFilter(), mdl, vectorizer, MetricName.ACCURACY);
        assertEquals(0.9769230769230769, score, 1e-12);
    }
}
