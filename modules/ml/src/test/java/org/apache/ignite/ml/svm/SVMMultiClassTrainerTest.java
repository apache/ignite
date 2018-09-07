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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

/**
 * Tests for {@link SVMLinearBinaryClassificationTrainer}.
 */
public class SVMMultiClassTrainerTest extends TrainerTest {
    /**
     * Test trainer on 4 sets grouped around of square vertices.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        SVMLinearMultiClassClassificationTrainer trainer = new SVMLinearMultiClassClassificationTrainer()
            .withLambda(0.3)
            .withAmountOfLocIterations(10)
            .withAmountOfIterations(20)
            .withSeed(1234L);

        SVMLinearMultiClassClassificationModel mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );
        TestUtils.assertEquals(0, mdl.apply(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(10, 100)), PRECISION);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        SVMLinearMultiClassClassificationTrainer trainer = new SVMLinearMultiClassClassificationTrainer()
            .withLambda(0.3)
            .withAmountOfLocIterations(10)
            .withAmountOfIterations(100)
            .withSeed(1234L);

        SVMLinearMultiClassClassificationModel originalModel = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        SVMLinearMultiClassClassificationModel updatedOnSameDS = trainer.update(
            originalModel,
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        SVMLinearMultiClassClassificationModel updatedOnEmptyDS = trainer.update(
            originalModel,
            new HashMap<Integer, double[]>(),
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        Vector v = VectorUtils.of(100, 10);
        TestUtils.assertEquals(originalModel.apply(v), updatedOnSameDS.apply(v), PRECISION);
        TestUtils.assertEquals(originalModel.apply(v), updatedOnEmptyDS.apply(v), PRECISION);
    }
}
