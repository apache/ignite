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

package org.apache.ignite.ml.knn.classification;

import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MapUtil;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * kNN algorithm trainer to solve multi-class classification task.
 */
public class KNNACDClassificationTrainer extends SingleLabelDatasetTrainer<KNNWithACDClassificationModel> {
    /** Distance measure. */
    private DistanceMeasure distance = new EuclideanDistance();

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @return Model.
     */
    @Override public <K, V> KNNWithACDClassificationModel fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        return null;
    }

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> KNNWithACDClassificationModel fit(Ignite ignite, IgniteCache<K, V> cache,
                        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        KMeansTrainer trainer = new KMeansTrainer()
            .withK(10)
            .withSeed(7867L);

        CacheBasedDatasetBuilder<K, V> datasetBuilder = new CacheBasedDatasetBuilder<>(ignite, cache);

        KMeansModel mdl = trainer.fit(
            datasetBuilder,
            featureExtractor,
            lbExtractor
        );


        CentroidStat centroidStat = getCentroidStat(datasetBuilder, featureExtractor, lbExtractor, mdl);

        // init
        LabeledVector<Vector, ProbableLabel>[] arr = new LabeledVector[mdl.centers().length];

        // fill label for each centroid
        for (int i = 0; i < mdl.centers().length; i++)
            arr[i] = new LabeledVector<>(mdl.centers()[i], fillProbableLabel(i, centroidStat));

        LabeledDataset<ProbableLabel, LabeledVector> dataset = new LabeledDataset<>(arr);

        return new KNNWithACDClassificationModel(dataset);

    }

    private ProbableLabel fillProbableLabel(int centroidIdx, CentroidStat centroidStat) {
        TreeMap<Double, Double> clsLbls = new TreeMap<>();
        // add all class labels as keys
        clsLbls.keySet().addAll(centroidStat.clsLblsSet);

        ConcurrentHashMap<Double, Integer> centroidLbDistribution
            = centroidStat.getCentroidStat().get(centroidIdx);

        int clusterSize = centroidStat.counts.get(centroidIdx);

        clsLbls.keySet().forEach((label)->{
            clsLbls.put(label,
                (double) (centroidLbDistribution.get(label)/clusterSize) // calculate percentage for specific class
            );
        });

        return new ProbableLabel(clsLbls);
    }


    private <K, V> CentroidStat getCentroidStat(CacheBasedDatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, KMeansModel mdl) {
        PartitionDataBuilder<K, V, EmptyContext, LabeledDataset<Double, LabeledVector>> partDataBuilder = new LabeledDatasetPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor
        );

        Vector[] centers = mdl.centers();
        CentroidStat totalRes = null;

        try (Dataset<EmptyContext, LabeledDataset<Double, LabeledVector>> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            partDataBuilder
        )) {

            return dataset.compute(data -> {

                CentroidStat res = new CentroidStat();

                for (int i = 0; i < data.rowSize(); i++) {
                    final IgniteBiTuple<Integer, Double> closestCentroid = findClosestCentroid(centers, data.getRow(i));

                    int centroidIdx = closestCentroid.get1();

                    double lb = data.label(i);

                    // add new label to label set
                    res.getClsLblsSet().add(lb);


                    ConcurrentHashMap<Double, Integer> centroidStat = res.centroidStat.get(centroidIdx);

                    if(centroidStat == null){
                        centroidStat = new ConcurrentHashMap<>();
                        centroidStat.put(lb, 1);
                        res.centroidStat.put(centroidIdx, centroidStat);
                    } else {
                        int cnt = centroidStat.containsKey(lb) ? centroidStat.get(lb) : 0;
                        centroidStat.put(lb, cnt + 1);
                    }

                    res.counts.merge(centroidIdx, 1,
                        (IgniteBiFunction<Integer, Integer, Integer>)(i1, i2) -> i1 + i2);
                }
                return res;
            }, (a, b) -> a == null ? b : a.merge(b));

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Find the closest cluster center index and distance to it from a given point.
     *
     * @param centers Centers to look in.
     * @param pnt Point.
     */
    private IgniteBiTuple<Integer, Double> findClosestCentroid(Vector[] centers, LabeledVector pnt) {
        double bestDistance = Double.POSITIVE_INFINITY;
        int bestInd = 0;

        for (int i = 0; i < centers.length; i++) {
            if(centers[i] != null) {
                double dist = distance.compute(centers[i], pnt.features());
                if (dist < bestDistance) {
                    bestDistance = dist;
                    bestInd = i;
                }
            }
        }
        return new IgniteBiTuple<>(bestInd, bestDistance);
    }


    /** Service class used for statistics. */
    public static class CentroidStat {

        /** Count of points closest to the center with a given index. */
        ConcurrentHashMap<Integer, ConcurrentHashMap<Double, Integer>> centroidStat = new ConcurrentHashMap<>();

        /** Count of points closest to the center with a given index. */
        ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();

        public ConcurrentSkipListSet<Double> getClsLblsSet() {
            return clsLblsSet;
        }

        ConcurrentSkipListSet<Double> clsLblsSet = new ConcurrentSkipListSet<>();

        /** Merge current */
        CentroidStat merge(CentroidStat other) {
            this.counts = MapUtil.mergeMaps(counts, other.counts, (i1, i2) -> i1 + i2, ConcurrentHashMap::new);
            this.centroidStat = MapUtil.mergeMaps(centroidStat, other.centroidStat, (m1, m2) ->
                MapUtil.mergeMaps(m1, m2, (i1, i2) -> i1 + i2, ConcurrentHashMap::new), ConcurrentHashMap::new);
            this.clsLblsSet.addAll(other.clsLblsSet);
            return this;
        }

        public ConcurrentHashMap<Integer, ConcurrentHashMap<Double, Integer>> getCentroidStat() {
            return centroidStat;
        }

    }
}
