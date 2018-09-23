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

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.util.MapUtil;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer of the naive Bayes regression model.
 */
public class GaussianNaiveBayesTrainer extends SingleLabelDatasetTrainer<GaussianNaiveBayesModel> {

    @Override public <K, V> GaussianNaiveBayesModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        return updateModel(null, datasetBuilder, featureExtractor, lbExtractor);
    }

    @Override protected boolean checkState(GaussianNaiveBayesModel mdl) {
        return true;
    }

    @Override protected <K, V> GaussianNaiveBayesModel updateModel(GaussianNaiveBayesModel mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {
        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, EmptyContext, LabeledVectorSet<Double, LabeledVector>> partDataBuilder
            = new LabeledDatasetPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor
        );

        try (Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            partDataBuilder
        )) {

            Stat statistic = dataset.compute(
                data -> {
                    Stat stat = new Stat();
                    for (int i = 0; i < data.rowSize(); i++) {
                        LabeledVector row = data.getRow(i);
                        Vector features = row.features();
                        Object label = row.label();

                        double[] toMeans;

                        if (!stat.featureSum.containsKey(label)) {
                            toMeans = new double[features.size()];
                            Arrays.fill(toMeans, 0.);
                            stat.featureSum.put(label, toMeans);
                        }

                        if (!stat.featureCount.containsKey(label)) {
                            stat.featureCount.put(label, 0);
                        }
                        stat.featureCount.put(label, stat.featureCount.get(label)+1);

                        toMeans = stat.featureSum.get(label);
                        for (int j = 0; j < features.size(); j++) {
                            toMeans[j] += features.get(j);
                        }
                    }

                    return stat;
                }, (a, b) -> {
                    if (a == null)
                        return b == null ? new Stat() : b;
                    if (b == null)
                        return a;
                    return a.merge(b);
                });
            int labelCount = statistic.featureCount.keySet().size();
            int featureCount = statistic.featureSum.values().stream().findFirst().get().length;
            double[][] means = new double[labelCount][featureCount];
            double[] classProbabilities = new double[labelCount];
            long rowsSize = statistic.featureCount.values().stream().mapToInt(i -> i).sum();

            int lbl = 0;
            for (Object label : statistic.featureCount.keySet()) {
                int count = statistic.featureCount.get(label);
                double[] tmp = statistic.featureSum.get(label);
                for (int i = 0; i < means.length; i++) {
                    means[lbl][i] = tmp[i] / count;
                }
                classProbabilities[lbl] = (double)count / rowsSize;
                ++lbl;
            }

            return new GaussianNaiveBayesModel(means, null, new DenseVector(classProbabilities));
        }
        catch (Exception e) {
            throw new
                RuntimeException(e);
        }

    }

    public static class Stat implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 1L;
        ConcurrentHashMap<Object, double[]> featureSum = new ConcurrentHashMap<>();
        ConcurrentHashMap<Object, Integer> featureCount = new ConcurrentHashMap<>();

        /** Merge current */
        Stat merge(Stat other) {
            this.featureSum = MapUtil.mergeMaps(featureSum, other.featureSum, this::sum, ConcurrentHashMap::new);
            this.featureCount = MapUtil.mergeMaps(featureCount, other.featureCount, (i1, i2) -> i1 + i2, ConcurrentHashMap::new);
            return this;
        }

        private double[] sum(double[] arr1, double[] arr2) {
            for (int i = 0; i < arr1.length; i++) {
                arr1[i] += arr2[i];
            }
            return arr1;
        }

        public void coutStat() {

        }
    }
}
