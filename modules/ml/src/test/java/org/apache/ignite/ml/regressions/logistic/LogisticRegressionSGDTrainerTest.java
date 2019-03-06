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
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.junit.Test;

/**
 * Tests for {@link LogisticRegressionSGDTrainer}.
 */
public class LogisticRegressionSGDTrainerTest extends TrainerTest {
    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void trainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
            .withMaxIterations(100000)
            .withLocIterations(100)
            .withBatchSize(14)
            .withSeed(123L);

        LogisticRegressionModel mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
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

        LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
            .withMaxIterations(100000)
            .withLocIterations(100)
            .withBatchSize(10)
            .withSeed(123L);

        LogisticRegressionModel originalMdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        LogisticRegressionModel updatedOnSameDS = trainer.update(
            originalMdl,
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        LogisticRegressionModel updatedOnEmptyDS = trainer.update(
            originalMdl,
            new HashMap<Integer, double[]>(),
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        Vector v1 = VectorUtils.of(100, 10);
        Vector v2 = VectorUtils.of(10, 100);
        TestUtils.assertEquals(originalMdl.predict(v1), updatedOnSameDS.predict(v1), PRECISION);
        TestUtils.assertEquals(originalMdl.predict(v2), updatedOnSameDS.predict(v2), PRECISION);
        TestUtils.assertEquals(originalMdl.predict(v2), updatedOnEmptyDS.predict(v2), PRECISION);
        TestUtils.assertEquals(originalMdl.predict(v1), updatedOnEmptyDS.predict(v1), PRECISION);
    }
}
