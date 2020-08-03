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

package org.apache.ignite.ml.naivebayes.compound;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer for the compound Naive Bayes classifier model. It uses a model composition of {@code
 * GaussianNaiveBayesTrainer} and {@code DiscreteNaiveBayesTrainer}. To distinguish which features with which trainer
 * should be used, each trainer should have a collection of feature ids which should be skipped. It can be set by {@code
 * #setFeatureIdsToSkip()} method.
 */
public class CompoundNaiveBayesTrainer extends SingleLabelDatasetTrainer<CompoundNaiveBayesModel> {
    /** Prior probabilities of each class. */
    private double[] priorProbabilities;

    /** Gaussian Naive Bayes trainer. */
    private GaussianNaiveBayesTrainer gaussianNaiveBayesTrainer;

    /** Feature ids which should be skipped in Gaussian model. */
    private Collection<Integer> gaussianFeatureIdsToSkip = Collections.emptyList();

    /** Discrete Naive Bayes trainer. */
    private DiscreteNaiveBayesTrainer discreteNaiveBayesTrainer;

    /** Feature ids which should be skipped in Discrete model. */
    private Collection<Integer> discreteFeatureIdsToSkip = Collections.emptyList();

    /** {@inheritDoc} */
    @Override public <K, V> CompoundNaiveBayesModel fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> extractor) {
        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(CompoundNaiveBayesModel mdl) {
        return gaussianNaiveBayesTrainer.isUpdateable(mdl.getGaussianModel())
            && discreteNaiveBayesTrainer.isUpdateable(mdl.getDiscreteModel());
    }

    /** {@inheritDoc} */
    @Override public CompoundNaiveBayesTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (CompoundNaiveBayesTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CompoundNaiveBayesModel updateModel(CompoundNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> extractor) {

        CompoundNaiveBayesModel compoundModel = new CompoundNaiveBayesModel()
            .withPriorProbabilities(priorProbabilities);

        if (gaussianNaiveBayesTrainer != null) {
            if (priorProbabilities != null)
                gaussianNaiveBayesTrainer.setPriorProbabilities(priorProbabilities);

            GaussianNaiveBayesModel model = (mdl == null)
                ? gaussianNaiveBayesTrainer.fit(datasetBuilder, extractor.map(skipFeatures(gaussianFeatureIdsToSkip)))
                : gaussianNaiveBayesTrainer.update(mdl.getGaussianModel(), datasetBuilder, extractor.map(skipFeatures(gaussianFeatureIdsToSkip)));

            compoundModel.withGaussianModel(model)
                .withGaussianFeatureIdsToSkip(gaussianFeatureIdsToSkip)
                .withLabels(model.getLabels())
                .withPriorProbabilities(priorProbabilities);
        }

        if (discreteNaiveBayesTrainer != null) {
            if (priorProbabilities != null)
                discreteNaiveBayesTrainer.setPriorProbabilities(priorProbabilities);

            DiscreteNaiveBayesModel model = (mdl == null)
                ? discreteNaiveBayesTrainer.fit(datasetBuilder, extractor.map(skipFeatures(discreteFeatureIdsToSkip)))
                : discreteNaiveBayesTrainer.update(mdl.getDiscreteModel(), datasetBuilder, extractor.map(skipFeatures(discreteFeatureIdsToSkip)));

            compoundModel.withDiscreteModel(model)
                .withDiscreteFeatureIdsToSkip(discreteFeatureIdsToSkip)
                .withLabels(model.getLabels())
                .withPriorProbabilities(priorProbabilities);
        }

        return compoundModel;
    }

    /** Sets prior probabilities. */
    public CompoundNaiveBayesTrainer withPriorProbabilities(double[] priorProbabilities) {
        this.priorProbabilities = priorProbabilities.clone();
        return this;
    }

    /** Sets a gaussian trainer. */
    public CompoundNaiveBayesTrainer withGaussianNaiveBayesTrainer(GaussianNaiveBayesTrainer gaussianNaiveBayesTrainer) {
        this.gaussianNaiveBayesTrainer = gaussianNaiveBayesTrainer;
        return this;
    }

    /** Sets a discrete trainer. */
    public CompoundNaiveBayesTrainer withDiscreteNaiveBayesTrainer(DiscreteNaiveBayesTrainer discreteNaiveBayesTrainer) {
        this.discreteNaiveBayesTrainer = discreteNaiveBayesTrainer;
        return this;
    }

    /** Sets feature ids to skip in Gaussian Bayes. */
    public CompoundNaiveBayesTrainer withGaussianFeatureIdsToSkip(Collection<Integer> gaussianFeatureIdsToSkip) {
        this.gaussianFeatureIdsToSkip = gaussianFeatureIdsToSkip;
        return this;
    }

    /** Sets feature ids to skip in discrete Bayes. */
    public CompoundNaiveBayesTrainer withDiscreteFeatureIdsToSkip(Collection<Integer> discreteFeatureIdsToSkip) {
        this.discreteFeatureIdsToSkip = discreteFeatureIdsToSkip;
        return this;
    }

    /** Removes features provided in {@param featureIdsToSkip} from a vector. */
    private static IgniteFunction<LabeledVector<Object>, LabeledVector<Object>> skipFeatures(Collection<Integer> featureIdsToSkip) {
        return featureValues -> {
            final int size = featureValues.features().size();
            int newSize = size - featureIdsToSkip.size();

            double[] newFeaturesValues = new double[newSize];
            int index = 0;
            for (int j = 0; j < size; j++) {
                if (featureIdsToSkip.contains(j)) continue;

                newFeaturesValues[index] = featureValues.get(j);
                ++index;
            }
            return new LabeledVector<>(VectorUtils.of(newFeaturesValues), featureValues.label());
        };
    }
}
