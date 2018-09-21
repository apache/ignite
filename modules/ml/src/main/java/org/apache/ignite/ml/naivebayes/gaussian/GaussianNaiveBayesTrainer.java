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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
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
            final Map<Object, double[]> stat = new HashMap<>();
            final Map<Object, Integer> labelCount = new HashMap<>();
            dataset.compute(
                data -> {
                    for (int i = 0; i < data.rowSize(); i++) {
                        LabeledVector row = data.getRow(i);
                        Vector features = row.features();
                        Object label = row.label();

                        double[] toMeans;

                        if (!stat.containsKey(label)) {
                            toMeans = new double[features.size()];
                            Arrays.fill(toMeans, 0.);
                            stat.put(label, toMeans);
                        }

                        if (!labelCount.containsKey(label)) {
                            labelCount.put(label, 1);
                        }
                        toMeans = stat.get(label);
                        for (int j = 0; j < features.size(); j++) {
                            toMeans[j] += features.get(j);
                        }

                    }
                    labelCount.keySet().forEach(key -> {
                        int count = labelCount.get(key);
                        double[] means = stat.get(key);
                        for (int i = 0; i < means.length; i++) {
                            means[i] /= count;
                        }
                    });

                    return null;
                }
                , (a, b) -> {
                    if (a == null)
                        return b == null ? 0 : b;
                    if (b == null)
                        return a;
                    return b;
                });

        }
        catch (Exception e) {
            throw new
                RuntimeException(e);
        }

        return null;
    }

    public static class Stat implements Serializable {

    }
}
