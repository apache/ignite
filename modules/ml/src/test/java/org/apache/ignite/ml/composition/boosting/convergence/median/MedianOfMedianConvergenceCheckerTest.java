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

package org.apache.ignite.ml.composition.boosting.convergence.median;

import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceChecker;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDataset;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/** */
public class MedianOfMedianConvergenceCheckerTest extends ConvergenceCheckerTest {
    /** */
    @Test
    public void testConvergenceChecking() {
        data.put(new double[]{10, 11}, 100000.0);
        LocalDatasetBuilder<double[], Double> datasetBuilder = new LocalDatasetBuilder<>(data, 1);

        ConvergenceChecker<double[], Double> checker = createChecker(
            new MedianOfMedianConvergenceCheckerFactory(0.1), datasetBuilder);

        double error = checker.computeError(VectorUtils.of(1, 2), 4.0, notConvergedMdl);
        Assert.assertEquals(1.9, error, 0.01);
        Assert.assertFalse(checker.isConverged(datasetBuilder, notConvergedMdl));
        Assert.assertTrue(checker.isConverged(datasetBuilder, convergedMdl));

        try(LocalDataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(), new FeatureMatrixWithLabelsOnHeapDataBuilder<>(fExtr, lbExtr))) {

            double onDSError = checker.computeMeanErrorOnDataset(dataset, notConvergedMdl);
            Assert.assertEquals(1.6, onDSError, 0.01);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
