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

import java.util.Arrays;
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
 * Use median of median on partitions value of errors for estimating error on dataset. This algorithm may be less
 * sensitive to
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public class MedianOfMedianConvergenceChecker<K, V> extends ConvergenceChecker<K, V> {
    /** Serial version uid. */
    private static final long serialVersionUID = 4902502002933415287L;

    /**
     * Creates an instance of MedianOfMedianConvergenceChecker.
     *
     * @param sampleSize Sample size.
     * @param lblMapping External label to internal mapping.
     * @param loss Loss function.
     * @param datasetBuilder Dataset builder.
     * @param fExtr Feature extractor.
     * @param lbExtr Label extractor.
     * @param precision Precision.
     */
    public MedianOfMedianConvergenceChecker(long sampleSize, IgniteFunction<Double, Double> lblMapping, Loss loss,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> fExtr,
        IgniteBiFunction<K, V, Double> lbExtr, double precision) {

        super(sampleSize, lblMapping, loss, datasetBuilder, fExtr, lbExtr, precision);
    }

    /** {@inheritDoc} */
    @Override public Double computeMeanErrorOnDataset(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition mdl) {

        double[] medians = dataset.compute(
            data -> computeMedian(mdl, data),
            this::reduce
        );

        if(medians == null)
            return Double.POSITIVE_INFINITY;
        return getMedian(medians);
    }

    /**
     * Compute median value on data partition.
     *
     * @param mdl Model.
     * @param data Data.
     * @return median value.
     */
    private double[] computeMedian(ModelsComposition mdl, FeatureMatrixWithLabelsOnHeapData data) {
        double[] errors = new double[data.getLabels().length];
        for (int i = 0; i < errors.length; i++)
            errors[i] = Math.abs(computeError(VectorUtils.of(data.getFeatures()[i]), data.getLabels()[i], mdl));
        return new double[] {getMedian(errors)};
    }

    /**
     * Compute median value on array of errors.
     *
     * @param errors Error values.
     * @return median value of errors.
     */
    private double getMedian(double[] errors) {
        if(errors.length == 0)
            return Double.POSITIVE_INFINITY;

        Arrays.sort(errors);
        final int middleIdx = (errors.length - 1) / 2;
        if (errors.length % 2 == 1)
            return errors[middleIdx];
        else
            return (errors[middleIdx + 1] + errors[middleIdx]) / 2;
    }

    /**
     * Merge median values among partitions.
     *
     * @param left Left partition.
     * @param right Right partition.
     * @return merged median values.
     */
    private double[] reduce(double[] left, double[] right) {
        if (left == null)
            return right;
        if(right == null)
            return left;

        double[] res = new double[left.length + right.length];
        System.arraycopy(left, 0, res, 0, left.length);
        System.arraycopy(right, 0, res, left.length, right.length);
        return res;
    }
}
