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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer for the Bernoulli naive Bayes classification model. The trainer calculates prior probabilities from the input
 * binary dataset. Prior probabilities can be also set by {@code setPriorProbabilities} or {@code
 * withEquiprobableClasses}. If {@code equiprobableClasses} is set, the probalilities of all classes will be {@code
 * 1/k}, where {@code k} is classes count.
 */
public class BernoulliNaiveBayesTrainer extends SingleLabelDatasetTrainer<BernoulliNaiveBayesModel> {
    /** Precision to compare bucketThresholds. */
    private static final double PRECISION = 1e-10;
    /* Preset prior probabilities. */
    private double[] priorProbabilities;
    /* Sets equivalent probability for all classes. */
    private boolean equiprobableClasses;
    /** The threshold to convert a feature to a discret value. */
    private double[][] bucketThresholds;

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

        if (mdl.getBucketThresholds().length != bucketThresholds.length) {
            return false;
        }
        for (int i = 0; i < bucketThresholds.length; i++) {
            for (int j = 0; i < bucketThresholds[i].length; i++) {
                if (Math.abs(mdl.getBucketThresholds()[i][j] - bucketThresholds[i][j]) > PRECISION) {
                    return false;
                }
            }
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> BernoulliNaiveBayesModel updateModel(BernoulliNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        try (Dataset<EmptyContext, BernoulliNaiveBayesSumsHolder> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                BernoulliNaiveBayesSumsHolder res = new BernoulliNaiveBayesSumsHolder();
                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();

                    Vector features = featureExtractor.apply(entity.getKey(), entity.getValue());
                    Double label = lbExtractor.apply(entity.getKey(), entity.getValue());

                    long[][] onesCount;

                    int size = features.size();
                    if (!res.onesCountPerLbl.containsKey(label)) {
                        onesCount = new long[size][];
                        for (int i = 0; i < size; i++) {
                            onesCount[i] = new long[bucketThresholds[i].length + 1];
                        }
//                        Arrays.fill(onesCount, 0L);
                        res.onesCountPerLbl.put(label, onesCount);
                    }
                    if (!res.featureCountersPerLbl.containsKey(label)) {
                        res.featureCountersPerLbl.put(label, 0);
                    }
                    res.featureCountersPerLbl.put(label, res.featureCountersPerLbl.get(label) + 1);

                    onesCount = res.onesCountPerLbl.get(label);
                    for (int j = 0; j < size; j++) {
                        double x = features.get(j);

                        int bucketNumber = toBucketNumber(x, bucketThresholds[j]);
                        ++onesCount[j][bucketNumber];
                    }
                }
                return res;
            })) {
            BernoulliNaiveBayesSumsHolder sumsHolder = dataset.compute(t -> t, (a, b) -> {
                if (a == null)
                    return b == null ? new BernoulliNaiveBayesSumsHolder() : b;
                if (b == null)
                    return a;
                return a.merge(b);
            });
            if (mdl != null && checkState(mdl)) {
                if (checkSumsHolder(sumsHolder, mdl.getSumsHolder())) {
                    sumsHolder = sumsHolder.merge(mdl.getSumsHolder());
                }
            }

            List<Double> sortedLabels = new ArrayList<>(sumsHolder.featureCountersPerLbl.keySet());
            sortedLabels.sort(Double::compareTo);
            assert !sortedLabels.isEmpty() : "The dataset should contain at least one feature";

            int labelCount = sortedLabels.size();
            int featureCount = sumsHolder.onesCountPerLbl.get(sortedLabels.get(0)).length;

            double[][][] probabilities = new double[labelCount][featureCount][];
            double[] classProbabilities = new double[labelCount];
            double[] labels = new double[labelCount];
            long datasetSize = sumsHolder.featureCountersPerLbl.values().stream().mapToInt(i -> i).sum();

            int lbl = 0;

            for (Double label : sortedLabels) {
                int count = sumsHolder.featureCountersPerLbl.get(label);
                long[][] sum = sumsHolder.onesCountPerLbl.get(label);


                for (int i = 0; i < featureCount; i++) {

                    int bucketsCount = sum[i].length;
                    probabilities[lbl][i] = new double[bucketsCount];
                    for (int j = 0; j < bucketsCount; j++) {

                    probabilities[lbl][i][j] = (double)sum[i][j] / count;
                    }
                }

                if (equiprobableClasses) {
                    classProbabilities[lbl] = 1. / labelCount;
                }
                else if (priorProbabilities != null) {
                    assert classProbabilities.length == priorProbabilities.length;
                    classProbabilities[lbl] = priorProbabilities[lbl];
                }
                else {
                    classProbabilities[lbl] = (double)count / datasetSize;
                }

                labels[lbl] = label;
                ++lbl;
            }
            return new BernoulliNaiveBayesModel(probabilities, classProbabilities, labels, bucketThresholds, sumsHolder);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /** Checks that two {@code BernoulliNaiveBayesSumsHolder} contain the same lengths of future vectors. */
    private boolean checkSumsHolder(BernoulliNaiveBayesSumsHolder holder1, BernoulliNaiveBayesSumsHolder holder2) {
        if (holder1 == null || holder2 == null) {
            return false;
        }

        Optional<long[][]> optinalFirst = holder1.onesCountPerLbl.values().stream().findFirst();
        Optional<long[][]> optinalSecond = holder2.onesCountPerLbl.values().stream().findFirst();

        if (optinalFirst.isPresent()) {
            if (optinalSecond.isPresent()) {
                return optinalFirst.get().length == optinalSecond.get().length;
            }
            else {
                return false;
            }
        }
        else {
            return !optinalSecond.isPresent();
        }
    }

    /** Sets equal probability for all classes. */
    public BernoulliNaiveBayesTrainer withEquiprobableClasses() {
        resetProbabilitiesSettings();
        equiprobableClasses = true;
        return this;
    }

    /** Sets prior probabilities. */
    public BernoulliNaiveBayesTrainer setPriorProbabilities(double[] priorProbabilities) {
        resetProbabilitiesSettings();
        this.priorProbabilities = priorProbabilities.clone();
        return this;
    }

    /** */
    public BernoulliNaiveBayesTrainer setBucketThresholds(double[][] bucketThresholds) {
        this.bucketThresholds = bucketThresholds;
        return this;
    }

    /** Sets default settings {@code equiprobableClasses} to {@code false} and removes priorProbabilities. */
    public BernoulliNaiveBayesTrainer resetProbabilitiesSettings() {
        equiprobableClasses = false;
        priorProbabilities = null;
        return this;
    }

    private int toBucketNumber(double value, double[] thresholds) {

        for (int i = 0; i < thresholds.length; i++) {
            if (value < thresholds[i]) {
                return i;
            }
        }
        return thresholds.length;
    }
}
