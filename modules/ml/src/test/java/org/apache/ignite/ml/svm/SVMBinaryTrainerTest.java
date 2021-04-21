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

package org.apache.ignite.ml.svm;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

/**
 * Tests for {@link SVMLinearClassificationTrainer}.
 */
public class SVMBinaryTrainerTest extends TrainerTest {
    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        SVMLinearClassificationTrainer trainer = new SVMLinearClassificationTrainer()
            .withSeed(1234L);

        SVMLinearClassificationModel mdl = trainer.fit(
            cacheMock, parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );

        TestUtils.assertEquals(0, mdl.predict(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.predict(VectorUtils.of(10, 100)), PRECISION);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        SVMLinearClassificationTrainer trainer = new SVMLinearClassificationTrainer()
            .withAmountOfIterations(1000)
            .withSeed(1234L);

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);
        SVMLinearClassificationModel originalMdl = trainer.fit(
            cacheMock, parts,
            vectorizer
        );

        SVMLinearClassificationModel updatedOnSameDS = trainer.update(
            originalMdl,
            cacheMock,
            parts,
            vectorizer
        );

        SVMLinearClassificationModel updatedOnEmptyDS = trainer.update(
            originalMdl,
            new HashMap<>(),
            parts,
            vectorizer
        );

        Vector v = VectorUtils.of(100, 10);
        TestUtils.assertEquals(originalMdl.predict(v), updatedOnSameDS.predict(v), PRECISION);
        TestUtils.assertEquals(originalMdl.predict(v), updatedOnEmptyDS.predict(v), PRECISION);
    }
}
