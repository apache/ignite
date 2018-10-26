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

package org.apache.ignite.ml.naivebayes.bernoulli;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer for the Bernoully naive Bayes classification model. The trainer calculates prior probabilities from the input
 * binary dataset. Prior probabilities can be also set by {@code setPriorProbabilities} or {@code
 * withEquiprobableClasses}. If {@code equiprobableClasses} is set, the probalilities of all classes will be {@code
 * 1/k}, where {@code k} is classes count.
 */
public class BernoulliNaiveBayesTrainer extends SingleLabelDatasetTrainer<BernoulliNaiveBayesModel> {

    /* Preset prior probabilities. */
    private double[] priorProbabilities;
    /* Sets equivalent probability for all classes. */
    private boolean equiprobableClasses;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @return Model.
     */
    @Override public <K, V> BernoulliNaiveBayesModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        return updateModel(null, datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(BernoulliNaiveBayesModel mdl) {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> BernoulliNaiveBayesModel updateModel(BernoulliNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        try (Dataset<EmptyContext, BernoulliNaiveBayesSumsHolder> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                return new BernoulliNaiveBayesSumsHolder();
            })) {
            return null;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /** Sets equal probability for all classes. */
    public BernoulliNaiveBayesTrainer withEquiprobableClasses() {
        resetSettings();
        equiprobableClasses = true;
        return this;
    }

    /** Sets prior probabilities. */
    public BernoulliNaiveBayesTrainer setPriorProbabilities(double[] priorProbabilities) {
        resetSettings();
        this.priorProbabilities = priorProbabilities.clone();
        return this;
    }

    /** Sets default settings. */
    public BernoulliNaiveBayesTrainer resetSettings() {
        equiprobableClasses = false;
        priorProbabilities = null;
        return this;
    }
}
