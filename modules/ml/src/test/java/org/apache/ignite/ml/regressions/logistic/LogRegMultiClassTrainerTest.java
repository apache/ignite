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

package org.apache.ignite.ml.regressions.logistic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.SmoothParametrized;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassModel;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassTrainer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link LogRegressionMultiClassTrainer}.
 */
public class LogRegMultiClassTrainerTest extends TrainerTest {
    /**
     * Test trainer on 4 sets grouped around of square vertices.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < fourSetsInSquareVertices.length; i++)
            cacheMock.put(i, fourSetsInSquareVertices[i]);

        final UpdatesStrategy<SmoothParametrized, SimpleGDParameterUpdate> stgy = new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator(0.2),
            SimpleGDParameterUpdate::sumLocal,
            SimpleGDParameterUpdate::avg
        );

        LogRegressionMultiClassTrainer<?> trainer = new LogRegressionMultiClassTrainer<>()
            .withUpdatesStgy(stgy)
            .withAmountOfIterations(1000)
            .withAmountOfLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        Assert.assertEquals(trainer.getAmountOfIterations(), 1000);
        Assert.assertEquals(trainer.getAmountOfLocIterations(), 10);
        Assert.assertEquals(trainer.getBatchSize(), 100, PRECISION);
        Assert.assertEquals(trainer.seed(), 123L);
        Assert.assertEquals(trainer.getUpdatesStgy(), stgy);

        LogRegressionMultiClassModel mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        Assert.assertTrue(mdl.toString().length() > 0);
        Assert.assertTrue(mdl.toString(true).length() > 0);
        Assert.assertTrue(mdl.toString(false).length() > 0);

        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(10, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(-10, 10)), PRECISION);
        TestUtils.assertEquals(2, mdl.apply(VectorUtils.of(-10, -10)), PRECISION);
        TestUtils.assertEquals(3, mdl.apply(VectorUtils.of(10, -10)), PRECISION);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < fourSetsInSquareVertices.length; i++)
            cacheMock.put(i, fourSetsInSquareVertices[i]);

        LogRegressionMultiClassTrainer<?> trainer = new LogRegressionMultiClassTrainer<>()
            .withUpdatesStgy(new UpdatesStrategy<>(
                new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate::sumLocal,
                SimpleGDParameterUpdate::avg
            ))
            .withAmountOfIterations(1000)
            .withAmountOfLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        LogRegressionMultiClassModel originalMdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        LogRegressionMultiClassModel updatedOnSameDS = trainer.update(
            originalMdl,
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        LogRegressionMultiClassModel updatedOnEmptyDS = trainer.update(
            originalMdl,
            new HashMap<Integer, double[]>(),
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        List<Vector> vectors = Arrays.asList(
            VectorUtils.of(10, 10),
            VectorUtils.of(-10, 10),
            VectorUtils.of(-10, -10),
            VectorUtils.of(10, -10)
        );

        for (Vector vec : vectors) {
            TestUtils.assertEquals(originalMdl.apply(vec), updatedOnSameDS.apply(vec), PRECISION);
            TestUtils.assertEquals(originalMdl.apply(vec), updatedOnEmptyDS.apply(vec), PRECISION);
        }
    }
}
