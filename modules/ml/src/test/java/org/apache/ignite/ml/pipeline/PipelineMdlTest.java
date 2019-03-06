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

package org.apache.ignite.ml.pipeline;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.junit.Test;

/**
 * Tests for {@link PipelineMdl}.
 */
public class PipelineMdlTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-6;

    /** */
    @Test
    public void testPredict() {
        Vector weights = new DenseVector(new double[] {2.0, 3.0});

        verifyPredict(getMdl(new LogisticRegressionModel(weights, 1.0).withRawLabels(true)));
    }

    /**
     * Get the empty internal model.
     *
     * @param internalMdl Internal model.
     */
    private PipelineMdl<Integer, double[]> getMdl(LogisticRegressionModel internalMdl) {
        return new PipelineMdl<Integer, double[]>()
            .withFeatureExtractor(null)
            .withLabelExtractor(null)
            .withInternalMdl(internalMdl);
    }

    /** */
    private void verifyPredict(PipelineMdl mdl) {
        Vector observation = new DenseVector(new double[] {1.0, 1.0});
        TestUtils.assertEquals(sigmoid(1.0 + 2.0 * 1.0 + 3.0 * 1.0), mdl.predict(observation), PRECISION);

        observation = new DenseVector(new double[] {2.0, 1.0});
        TestUtils.assertEquals(sigmoid(1.0 + 2.0 * 2.0 + 3.0 * 1.0), mdl.predict(observation), PRECISION);

        observation = new DenseVector(new double[] {1.0, 2.0});
        TestUtils.assertEquals(sigmoid(1.0 + 2.0 * 1.0 + 3.0 * 2.0), mdl.predict(observation), PRECISION);

        observation = new DenseVector(new double[] {-2.0, 1.0});
        TestUtils.assertEquals(sigmoid(1.0 - 2.0 * 2.0 + 3.0 * 1.0), mdl.predict(observation), PRECISION);

        observation = new DenseVector(new double[] {1.0, -2.0});
        TestUtils.assertEquals(sigmoid(1.0 + 2.0 * 1.0 - 3.0 * 2.0), mdl.predict(observation), PRECISION);
    }

    /**
     * Sigmoid function.
     *
     * @param z The regression value.
     * @return The result.
     */
    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }
}
