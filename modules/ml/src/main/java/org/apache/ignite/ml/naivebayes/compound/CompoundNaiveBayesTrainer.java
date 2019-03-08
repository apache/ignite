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

import java.util.function.Predicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/** Created by Ravil on 04/02/2019. */
public class CompoundNaiveBayesTrainer extends SingleLabelDatasetTrainer<CompoundNaiveBayesModel> {

    GaussianNaiveBayesTrainer gaussianNaiveBayesTrainer;
    private Predicate<Integer> gaussianSkipFeature;
    DiscreteNaiveBayesTrainer discreteNaiveBayesTrainer;
    private Predicate<Integer> discreteSkipFeature;

    @Override public <K, V> CompoundNaiveBayesModel fit(DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {
        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(CompoundNaiveBayesModel mdl) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public CompoundNaiveBayesTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (CompoundNaiveBayesTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override public <K, V> CompoundNaiveBayesModel updateModel(CompoundNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {
        return super.updateModel(mdl, datasetBuilder, featureExtractor, lbExtractor);
    }

    @Override protected <K, V> CompoundNaiveBayesModel updateModel(CompoundNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, FeatureLabelExtractor<K, V, Double> extractor) {
        if (mdl != null) {
            gaussianNaiveBayesTrainer.setSkipFeature(gaussianSkipFeature);
            gaussianNaiveBayesTrainer.fit(datasetBuilder, extractor);
            discreteNaiveBayesTrainer.setSkipFeature(discreteSkipFeature);
            discreteNaiveBayesTrainer.fit(datasetBuilder, extractor);
        }
        return null;
    }
}
