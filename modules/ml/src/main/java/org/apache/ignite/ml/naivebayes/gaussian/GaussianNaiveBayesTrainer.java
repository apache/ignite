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

package org.apache.ignite.ml.naivebayes.gaussian;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer for the naive Bayes classification model. The trainer calculates prior probabilities from the input dataset.
 * Prior probabilities can be also set by {@code setPriorProbabilities} or {@code withEquiprobableClasses}. If {@code
 * equiprobableClasses} is set, the probabilities of all classes will be {@code 1/k}, where {@code k} is classes count.
 */
public class GaussianNaiveBayesTrainer extends SingleLabelDatasetTrainer<GaussianNaiveBayesModel> {
    /** Preset prior probabilities. */
    private double[] priorProbabilities;

    /** Sets equivalent probability for all classes. */
    private boolean equiprobableClasses;

    /** {@inheritDoc} */
    @Override public <K, V> GaussianNaiveBayesModel fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
                                                        Preprocessor<K, V> extractor) {
        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(GaussianNaiveBayesModel mdl) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GaussianNaiveBayesTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (GaussianNaiveBayesTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> GaussianNaiveBayesModel updateModel(GaussianNaiveBayesModel mdl,
                                                                   DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> extractor) {
        assert datasetBuilder != null;

        try (Dataset<EmptyContext, GaussianNaiveBayesSumsHolder> dataset = datasetBuilder.build(
            envBuilder,
            (env, upstream, upstreamSize) -> new EmptyContext(),
            (env, upstream, upstreamSize, ctx) -> {

                GaussianNaiveBayesSumsHolder res = new GaussianNaiveBayesSumsHolder();
                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();

                    LabeledVector lv = extractor.apply(entity.getKey(), entity.getValue());
                    Vector features = lv.features();
                    Double lbl = (Double) lv.label();

                    double[] toMeans;
                    double[] sqSum;

                    if (!res.featureSumsPerLbl.containsKey(lbl)) {
                        toMeans = new double[features.size()];
                        Arrays.fill(toMeans, 0.);
                        res.featureSumsPerLbl.put(lbl, toMeans);
                    }
                    if (!res.featureSquaredSumsPerLbl.containsKey(lbl)) {
                        sqSum = new double[features.size()];
                        res.featureSquaredSumsPerLbl.put(lbl, sqSum);
                    }
                    if (!res.featureCountersPerLbl.containsKey(lbl))
                        res.featureCountersPerLbl.put(lbl, 0);

                    res.featureCountersPerLbl.put(lbl, res.featureCountersPerLbl.get(lbl) + 1);

                    toMeans = res.featureSumsPerLbl.get(lbl);
                    sqSum = res.featureSquaredSumsPerLbl.get(lbl);
                    for (int j = 0; j < features.size(); j++) {
                        double x = features.get(j);
                        toMeans[j] += x;
                        sqSum[j] += x * x;
                    }
                }
                return res;
            }, learningEnvironment()
        )) {
            GaussianNaiveBayesSumsHolder sumsHolder = dataset.compute(t -> t, (a, b) -> {
                if (a == null)
                    return b;
                if (b == null)
                    return a;
                return a.merge(b);
            });
            if (mdl != null && mdl.getSumsHolder() != null)
                sumsHolder = sumsHolder.merge(mdl.getSumsHolder());

            List<Double> sortedLabels = new ArrayList<>(sumsHolder.featureCountersPerLbl.keySet());
            sortedLabels.sort(Double::compareTo);
            assert !sortedLabels.isEmpty() : "The dataset should contain at least one feature";

            int lblCnt = sortedLabels.size();
            int featureCnt = sumsHolder.featureSumsPerLbl.get(sortedLabels.get(0)).length;

            double[][] means = new double[lblCnt][featureCnt];
            double[][] variances = new double[lblCnt][featureCnt];
            double[] clsProbabilities = new double[lblCnt];
            double[] labels = new double[lblCnt];

            long datasetSize = sumsHolder.featureCountersPerLbl.values().stream().mapToInt(i -> i).sum();

            int lbl = 0;
            for (Double dblLbl : sortedLabels) {
                int cnt = sumsHolder.featureCountersPerLbl.get(dblLbl);
                double[] sum = sumsHolder.featureSumsPerLbl.get(dblLbl);
                double[] sqSum = sumsHolder.featureSquaredSumsPerLbl.get(dblLbl);

                for (int i = 0; i < featureCnt; i++) {
                    means[lbl][i] = sum[i] / cnt;
                    variances[lbl][i] = (sqSum[i] - sum[i] * sum[i] / cnt) / cnt;
                }

                if (equiprobableClasses)
                    clsProbabilities[lbl] = 1. / lblCnt;

                else if (priorProbabilities != null) {
                    assert clsProbabilities.length == priorProbabilities.length;
                    clsProbabilities[lbl] = priorProbabilities[lbl];
                }
                else
                    clsProbabilities[lbl] = (double)cnt / datasetSize;

                labels[lbl] = dblLbl;
                ++lbl;
            }

            return new GaussianNaiveBayesModel(means, variances, clsProbabilities, labels, sumsHolder);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /** Sets equal probability for all classes. */
    public GaussianNaiveBayesTrainer withEquiprobableClasses() {
        resetSettings();
        equiprobableClasses = true;
        return this;
    }

    /** Sets prior probabilities. */
    public GaussianNaiveBayesTrainer setPriorProbabilities(double[] priorProbabilities) {
        resetSettings();
        this.priorProbabilities = priorProbabilities.clone();
        return this;
    }

    /** Sets default settings. */
    public GaussianNaiveBayesTrainer resetSettings() {
        equiprobableClasses = false;
        priorProbabilities = null;
        return this;
    }

}
