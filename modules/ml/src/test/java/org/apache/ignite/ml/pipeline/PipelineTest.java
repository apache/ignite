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

package org.apache.ignite.ml.pipeline;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.svm.SVMLinearClassificationTrainer;
import org.junit.Test;

/**
 * Tests for {@link Pipeline}.
 */
public class PipelineTest extends TrainerTest {
    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);

        PipelineMdl<Integer, Vector> mdl = new Pipeline<Integer, Vector, Integer, Double>()
            .addVectorizer(vectorizer)
            .addPreprocessingTrainer(new MinMaxScalerTrainer<Integer, Vector>())
            .addPreprocessingTrainer(new NormalizationTrainer<Integer, Vector>()
                .withP(1))
            .addTrainer(new SVMLinearClassificationTrainer())
            .fit(cacheMock, parts);

        TestUtils.assertEquals(0, mdl.predict(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.predict(VectorUtils.of(10, 100)), PRECISION);
    }

    /**
     * Test the missed final state.
     */
    @Test(expected = IllegalStateException.class)
    public void testTrainWithMissedFinalStage() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
            .labeled(Vectorizer.LabelCoordinate.FIRST);

        PipelineMdl<Integer, Vector> mdl = new Pipeline<Integer, Vector, Integer, Double>()
            .addVectorizer(vectorizer)
            .addPreprocessingTrainer(new MinMaxScalerTrainer<Integer, Vector>())
            .addPreprocessingTrainer(new NormalizationTrainer<Integer, Vector>()
                .withP(1))
            .fit(cacheMock, parts);

        TestUtils.assertEquals(0, mdl.predict(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.predict(VectorUtils.of(10, 100)), PRECISION);
    }
}
