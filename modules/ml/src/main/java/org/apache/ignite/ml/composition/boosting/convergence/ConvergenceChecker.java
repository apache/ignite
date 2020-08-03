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
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;

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

    /** Upstream preprocessor. */
    private Preprocessor<K, V> preprocessor;

    /** Precision of convergence check. */
    private double precision;

    /**
     * Constructs an instance of ConvergenceChecker.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param loss Loss gradient.
     * @param datasetBuilder Dataset builder.
     * @param preprocessor Upstream preprocessor.
     * @param precision Precision.FeatureMatrixWithLabelsOnHeapDataBuilder.java
     */
    public ConvergenceChecker(long sampleSize,
                              IgniteFunction<Double, Double> externalLbToInternalMapping, Loss loss,
                              DatasetBuilder<K, V> datasetBuilder,
                              Preprocessor<K, V> preprocessor, double precision) {
        assert precision < 1 && precision >= 0;

        this.sampleSize = sampleSize;
        this.externalLbToInternalMapping = externalLbToInternalMapping;
        this.loss = loss;
        this.precision = precision;
        this.preprocessor = preprocessor;
    }

    /**
     * Checks convergency on dataset.
     *
     * @param envBuilder Learning environment builder.
     * @param currMdl Current model.
     * @return True if GDB is converged.
     */
    public boolean isConverged(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        ModelsComposition currMdl) {
        LearningEnvironment environment = envBuilder.buildForTrainer();
        environment.initDeployingContext(preprocessor);

        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            envBuilder,
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(preprocessor),
            environment
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
     * @return True if GDB is converged.
     */
    public boolean isConverged(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition currMdl) {
        Double error = computeMeanErrorOnDataset(dataset, currMdl);
        return error < precision || error.isNaN();
    }

    /**
     * Compute error for given model on learning dataset.
     *
     * @param dataset Learning dataset.
     * @param mdl Model.
     * @return Error mean value.
     */
    public abstract Double computeMeanErrorOnDataset(
        Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition mdl);

    /**
     * Compute error for the specific vector of dataset.
     *
     * @param currMdl Current model.
     * @return Error.
     */
    public double computeError(Vector features, Double answer, ModelsComposition currMdl) {
        Double realAnswer = externalLbToInternalMapping.apply(answer);
        Double mdlAnswer = currMdl.predict(features);
        return -loss.gradient(sampleSize, realAnswer, mdlAnswer);
    }
}
