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

package org.apache.ignite.ml.composition.boosting.convergence;

import java.io.Serializable;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Contains logic of error computing and convergence checking for Gradient Boosting algorithms.
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public abstract class ConvergenceChecker<K, V> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 710762134746674105L;

    /** Sample size. */
    private long sampleSize;

    /** External label to internal mapping. */
    private IgniteFunction<Double, Double> externalLbToInternalMapping;

    /** Loss function. */
    private Loss loss;

    /** Feature extractor. */
    private IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Label extractor. */
    private IgniteBiFunction<K, V, Double> lbExtractor;

    /** Precision of convergence check. */
    private double precision;

    /**
     * Constructs an instance of ConvergenceChecker.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param loss Loss gradient.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param precision Precision.
     */
    public ConvergenceChecker(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping, Loss loss,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor,
        double precision) {

        assert precision < 1 && precision >= 0;

        this.sampleSize = sampleSize;
        this.externalLbToInternalMapping = externalLbToInternalMapping;
        this.loss = loss;
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.precision = precision;
    }

    /**
     * Checks convergency on dataset.
     *
     * @param envBuilder Learning environment builder.
     * @param currMdl Current model.
     * @return true if GDB is converged.
     */
    public boolean isConverged(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        ModelsComposition currMdl) {
        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            envBuilder,
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(featureExtractor, lbExtractor)
        )) {
            return isConverged(dataset, currMdl);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks convergency on dataset.
     *
     * @param dataset Dataset.
     * @param currMdl Current model.
     * @return true if GDB is converged.
     */
    public boolean isConverged(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset, ModelsComposition currMdl) {
        Double error = computeMeanErrorOnDataset(dataset, currMdl);
        return error < precision || error.isNaN();
    }

    /**
     * Compute error for given model on learning dataset.
     *
     * @param dataset Learning dataset.
     * @param mdl Model.
     * @return error mean value.
     */
    public abstract Double computeMeanErrorOnDataset(
        Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition mdl);

    /**
     * Compute error for the specific vector of dataset.
     *
     * @param currMdl Current model.
     * @return error.
     */
    public double computeError(Vector features, Double answer, ModelsComposition currMdl) {
        Double realAnswer = externalLbToInternalMapping.apply(answer);
        Double mdlAnswer = currMdl.predict(features);
        return -loss.gradient(sampleSize, realAnswer, mdlAnswer);
    }
}
