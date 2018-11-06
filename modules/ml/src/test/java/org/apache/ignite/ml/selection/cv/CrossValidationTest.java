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

package org.apache.ignite.ml.selection.cv;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

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
            assertEquals(1, scores[i], 1e-1);
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
}
