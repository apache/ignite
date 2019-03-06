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

package org.apache.ignite.ml.selection.cv;

import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetrics;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CrossValidation}.
 */
public class CrossValidationTest {
    /** */
    @Test
    public void testScoreWithGoodDataset() {
        Map<Integer, Double> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, i > 500 ? 1.0 : 0.0);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        CrossValidation<DecisionTreeNode, Double, Integer, Double> scoreCalculator =
            new CrossValidation<>();

        int folds = 4;

        verifyScores(folds, scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            1,
            (k, v) -> VectorUtils.of(k),
            (k, v) -> v,
            folds
        ));

        verifyScores(folds, scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            (e1, e2) -> true,
            1,
            (k, v) -> VectorUtils.of(k),
            (k, v) -> v,
            folds
        ));
    }

    /** */
    @Test
    public void testScoreWithGoodDatasetAndBinaryMetrics() {
        Map<Integer, Double> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, i > 500 ? 1.0 : 0.0);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        CrossValidation<DecisionTreeNode, Double, Integer, Double> scoreCalculator =
            new CrossValidation<>();

        int folds = 4;

        BinaryClassificationMetrics metrics = (BinaryClassificationMetrics) new BinaryClassificationMetrics()
            .withMetric(BinaryClassificationMetricValues::accuracy);

        verifyScores(folds, scoreCalculator.score(
            trainer,
            metrics,
            data,
            1,
            (k, v) -> VectorUtils.of(k),
            (k, v) -> v,
            folds
        ));

        verifyScores(folds, scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            (e1, e2) -> true,
            1,
            (k, v) -> VectorUtils.of(k),
            (k, v) -> v,
            folds
        ));
    }

    /** */
    @Test
    public void testScoreWithBadDataset() {
        Map<Integer, Double> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, i % 2 == 0 ? 1.0 : 0.0);

        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(1, 0);

        CrossValidation<DecisionTreeNode, Double, Integer, Double> scoreCalculator =
            new CrossValidation<>();

        int folds = 4;

        double[] scores = scoreCalculator.score(
            trainer,
            new Accuracy<>(),
            data,
            1,
            (k, v) -> VectorUtils.of(k),
            (k, v) -> v,
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
