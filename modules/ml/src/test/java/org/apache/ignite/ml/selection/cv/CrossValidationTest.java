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

package org.apache.ignite.ml.selection.cv;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetrics;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CrossValidation}.
 */
public class CrossValidationTest {
    /** */
    @Test
    public void testScoreWithGoodDataset() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, new double[] {i > 500 ? 1.0 : 0.0, i});

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        CrossValidation<DecisionTreeNode, Double, Integer, double[]> scoreCalculator =
            new CrossValidation<>();

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        int folds = 4;

        verifyScores(folds, scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            1,
            vectorizer,
            folds
        ));

        verifyScores(folds, scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            (e1, e2) -> true,
            1,
            vectorizer,
            folds
        ));
    }

    /** */
    @Test
    public void testScoreWithGoodDatasetAndBinaryMetrics() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, new double[] {i > 500 ? 1.0 : 0.0, i});

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        CrossValidation<DecisionTreeNode, Double, Integer, double[]> scoreCalculator =
            new CrossValidation<>();

        int folds = 4;

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        verifyScores(folds, scoreCalculator.score(
            trainer,
            metrics,
            data,
            1,
            vectorizer,
            folds
        ));

        verifyScores(folds, scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            (e1, e2) -> true,
            1,
            vectorizer,
            folds
        ));
    }

    /** */
    @Test
    public void testScoreWithBadDataset() {
        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, new double[] { i, i % 2 == 0 ? 1.0 : 0.0});

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        CrossValidation<DecisionTreeNode, Double, Integer, double[]> scoreCalculator =
            new CrossValidation<>();

        int folds = 4;

        double[] scores = scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            1,
            vectorizer,
            folds
        );

        assertEquals(folds, scores.length);

        for (int i = 0; i < folds; i++)
            assertTrue(scores[i] < 0.6);
    }

    /** */
    private void verifyScores(int folds, double[] scores) {
        assertEquals(folds, scores.length);

        for (int i = 0; i < folds; i++)
            assertEquals(1, scores[i], 1e-1);
    }
}
