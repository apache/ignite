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
     * @param precision
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
     * @param currMdl Current model.
     * @return true if GDB is converged.
     */
    public boolean isConverged(DatasetBuilder<K, V> datasetBuilder, ModelsComposition currMdl) {
        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
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
        Double mdlAnswer = currMdl.apply(features);
        return -loss.gradient(sampleSize, realAnswer, mdlAnswer);
    }
}
