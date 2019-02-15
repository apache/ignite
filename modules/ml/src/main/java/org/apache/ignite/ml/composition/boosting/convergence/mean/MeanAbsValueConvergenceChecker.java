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

package org.apache.ignite.ml.composition.boosting.convergence.mean;

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceChecker;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Use mean value of errors for estimating error on dataset.
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public class MeanAbsValueConvergenceChecker<K,V> extends ConvergenceChecker<K,V> {
    /** Serial version uid. */
    private static final long serialVersionUID = 8534776439755210864L;

    /**
     * Creates an intance of MeanAbsValueConvergenceChecker.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param loss Loss.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    public MeanAbsValueConvergenceChecker(long sampleSize, IgniteFunction<Double, Double> externalLbToInternalMapping,
        Loss loss, DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor,
        double precision) {

        super(sampleSize, externalLbToInternalMapping, loss, datasetBuilder, featureExtractor, lbExtractor, precision);
    }

    /** {@inheritDoc} */
    @Override public Double computeMeanErrorOnDataset(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition mdl) {

        IgniteBiTuple<Double, Long> sumAndCnt = dataset.compute(
            partition -> computeStatisticOnPartition(mdl, partition),
            this::reduce
        );

        if(sumAndCnt == null || sumAndCnt.getValue() == 0)
            return Double.NaN;
        return sumAndCnt.getKey() / sumAndCnt.getValue();
    }

    /**
     * Compute sum of absolute value of errors and count of rows in partition.
     *
     * @param mdl Model.
     * @param part Partition.
     * @return Tuple (sum of errors, count of rows)
     */
    private IgniteBiTuple<Double, Long> computeStatisticOnPartition(ModelsComposition mdl, FeatureMatrixWithLabelsOnHeapData part) {
        Double sum = 0.0;

        for(int i = 0; i < part.getFeatures().length; i++) {
            double error = computeError(VectorUtils.of(part.getFeatures()[i]), part.getLabels()[i], mdl);
            sum += Math.abs(error);
        }

        return new IgniteBiTuple<>(sum, (long) part.getLabels().length);
    }

    /**
     * Merge left and right statistics from partitions.
     *
     * @param left Left.
     * @param right Right.
     * @return merged value.
     */
    private IgniteBiTuple<Double, Long> reduce(IgniteBiTuple<Double, Long> left, IgniteBiTuple<Double, Long> right) {
        if (left == null) {
            if (right != null)
                return right;
            else
                return new IgniteBiTuple<>(0.0, 0L);
        }

        if (right == null)
            return left;

        return new IgniteBiTuple<>(
            left.getKey() + right.getKey(),
            right.getValue() + left.getValue()
        );
    }
}
