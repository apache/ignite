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

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/** Created by Ravil on 04/02/2019. */
public class CompoundNaiveBayesTrainer extends SingleLabelDatasetTrainer<CompoundNaiveBayesModel> {

    /** Prior probabilities of each class */
    private double[] clsProbabilities;
    /** Labels. */
    private double[] labels;
    private GaussianNaiveBayesTrainer gaussianNaiveBayesTrainer;
    private DiscreteNaiveBayesTrainer discreteNaiveBayesTrainer;

    @Override public <K, V> CompoundNaiveBayesModel fit(DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {
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

    @Override protected <K, V> CompoundNaiveBayesModel updateModel(CompoundNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, FeatureLabelExtractor<K, V, Double> extractor) {

        CompoundNaiveBayesModel compoundModel = new CompoundNaiveBayesModel()
                .withLabels(labels)
                .wirhPriorProbabilities(clsProbabilities);

        if (gaussianNaiveBayesTrainer != null) {
            GaussianNaiveBayesModel model = (mdl == null)
                    ? gaussianNaiveBayesTrainer.fit(datasetBuilder, extractor)
                    : gaussianNaiveBayesTrainer.update(mdl.getGaussianModel(), datasetBuilder, extractor) ;

            compoundModel.withGaussianModel(model);
        }

        if (discreteNaiveBayesTrainer != null) {
            DiscreteNaiveBayesModel model = (mdl == null)
                    ? discreteNaiveBayesTrainer.fit(datasetBuilder, extractor)
                    : discreteNaiveBayesTrainer.update(mdl.getDiscreteModel(), datasetBuilder, extractor) ;

            compoundModel.withDiscreteModel(model);
        }

        return compoundModel;
    }

    /** */
    public CompoundNaiveBayesTrainer setClsProbabilities(double[] clsProbabilities) {
        this.clsProbabilities = clsProbabilities.clone();
        return this;
    }

    /** */
    public CompoundNaiveBayesTrainer setLabels(double[] labels) {
        this.labels = labels.clone();
        return this;
    }

    /** */
    public CompoundNaiveBayesTrainer setGaussianNaiveBayesTrainer(GaussianNaiveBayesTrainer gaussianNaiveBayesTrainer) {
        this.gaussianNaiveBayesTrainer = gaussianNaiveBayesTrainer;
        return this;
    }

    /** */
    public CompoundNaiveBayesTrainer setDiscreteNaiveBayesTrainer(DiscreteNaiveBayesTrainer discreteNaiveBayesTrainer) {
        this.discreteNaiveBayesTrainer = discreteNaiveBayesTrainer;
        return this;
    }
}
