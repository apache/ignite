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

package org.apache.ignite.examples.ml.clustering;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Example of using KMeans clusterization to determine the optimal count of clusters in data.
 *
 * Description of model can be found in: https://en.wikipedia.org/wiki/Kmeans .
 * Original dataset can be downloaded from: https://archive.ics.uci.edu/ml/datasets/Wholesale+customers .
 * Copy of dataset are stored in:  modules/ml/src/main/resources/datasets/wholesale_customers.csv .
 * Score for clusterizer estimation: mean of entropy in clusters.
 * Description of entropy can be found in: https://en.wikipedia.org/wiki/Entropy_(information_theory) .
 */
public class CustomersClusterizationExample {
    /** Runs example. */
    public static void main(String[] args) throws IOException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                System.out.println(">>> Fill dataset cache.");
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.WHOLESALE_CUSTOMERS);

                System.out.println(">>> Start traininig and scoring.");
                for(int amountOfClusters = 1; amountOfClusters < 10; amountOfClusters++) {
                    KMeansTrainer trainer = new KMeansTrainer()
                        .withAmountOfClusters(amountOfClusters)
                        .withDistance(new EuclideanDistance())
                        .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0))
                        .withMaxIterations(50);

                    // This vectorizer works with values in cache of Vector class.
                    Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                        .labeled(Vectorizer.LabelCoordinate.FIRST); // FIRST means "label are stored at first coordinate of vector"

                    // Splits dataset to train and test samples with 80/20 proportion.
                    TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>().split(0.8);

                    KMeansModel mdl = trainer.fit(
                        ignite, dataCache,
                        split.getTrainFilter(),
                        vectorizer
                    );

                    double entropy = computeMeanEntropy(dataCache, split.getTestFilter(), vectorizer, mdl);
                    System.out.println(String.format(">> Clusters mean entropy [%d clusters]: %.2f", amountOfClusters, entropy));
                }
            } finally {
                dataCache.destroy();
            }
        } finally {
            System.out.flush();
        }
    }

    /**
     * Computes mean entropy in clusters.
     *
     * @param cache Dataset cache.
     * @param filter Test dataset filter.
     * @param vectorizer Upstream vectorizer.
     * @param model KMeans model.
     * @return Score.
     */
    private static double computeMeanEntropy(IgniteCache<Integer, Vector> cache,
        IgniteBiPredicate<Integer, Vector> filter,
        Vectorizer<Integer, Vector, Integer, Double> vectorizer,
        KMeansModel model) {

        Map<Integer, Map<Integer, AtomicInteger>> clusterUniqueLabelCounts = new HashMap<>();
        try (QueryCursor<Cache.Entry<Integer, Vector>> cursor = cache.query(new ScanQuery<>(filter))) {
            for (Cache.Entry<Integer, Vector> ent : cursor) {
                LabeledVector<Double> vec = vectorizer.apply(ent.getKey(), ent.getValue());
                int cluster = model.predict(vec.features());
                int channel = vec.label().intValue();

                if (!clusterUniqueLabelCounts.containsKey(cluster))
                    clusterUniqueLabelCounts.put(cluster, new HashMap<>());

                if (!clusterUniqueLabelCounts.get(cluster).containsKey(channel))
                    clusterUniqueLabelCounts.get(cluster).put(channel, new AtomicInteger());

                clusterUniqueLabelCounts.get(cluster).get(channel).incrementAndGet();
            }
        }

        double sumOfClusterEntropies = 0.0;
        for (Integer cluster : clusterUniqueLabelCounts.keySet()) {
            Map<Integer, AtomicInteger> labelCounters = clusterUniqueLabelCounts.get(cluster);
            int sizeOfCluster = labelCounters.values().stream().mapToInt(AtomicInteger::get).sum();
            double entropyInCluster = labelCounters.values().stream()
                .mapToDouble(AtomicInteger::get)
                .map(lblsCount -> lblsCount / sizeOfCluster)
                .map(lblProb -> -lblProb * Math.log(lblProb))
                .sum();

            sumOfClusterEntropies += entropyInCluster;
        }

        return sumOfClusterEntropies / clusterUniqueLabelCounts.size();
    }
}
