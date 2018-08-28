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

package org.apache.ignite.ml.knn.ann;

import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MapUtil;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * ANN algorithm trainer to solve multi-class classification task.
 * This trainer is based on ACD strategy and KMeans clustering algorithm to find centroids.
 */
public class ANNClassificationTrainer extends SingleLabelDatasetTrainer<ANNClassificationModel> {
    /** Amount of clusters. */
    private int k = 2;

    /** Amount of iterations. */
    private int maxIterations = 10;

    /** Delta of convergence. */
    private double epsilon = 1e-4;

    /** Distance measure. */
    private DistanceMeasure distance = new EuclideanDistance();

    /** KMeans initializer. */
    private long seed;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @return Model.
     */
    @Override public <K, V> ANNClassificationModel fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        final Vector[] centers = getCentroids(featureExtractor, lbExtractor, datasetBuilder);

        final CentroidStat centroidStat = getCentroidStat(datasetBuilder, featureExtractor, lbExtractor, centers);

        final LabeledVectorSet<ProbableLabel, LabeledVector> dataset = buildLabelsForCandidates(centers, centroidStat);

        return new ANNClassificationModel(dataset);
    }

    /** */
    @NotNull private LabeledVectorSet<ProbableLabel, LabeledVector> buildLabelsForCandidates(Vector[] centers, CentroidStat centroidStat) {
        // init
        final LabeledVector<Vector, ProbableLabel>[] arr = new LabeledVector[centers.length];

        // fill label for each centroid
        for (int i = 0; i < centers.length; i++)
            arr[i] = new LabeledVector<>(centers[i], fillProbableLabel(i, centroidStat));

        return new LabeledVectorSet<>(arr);
    }

    /**
     * Perform KMeans clusterization algorithm to find centroids.
     *
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @param datasetBuilder   The dataset builder.
     * @param <K>              Type of a key in {@code upstream} data.
     * @param <V>              Type of a value in {@code upstream} data.
     * @return The arrays of vectors.
     */
    private <K, V> Vector[] getCentroids(IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, DatasetBuilder<K, V> datasetBuilder) {
        KMeansTrainer trainer = new KMeansTrainer()
            .withK(k)
            .withMaxIterations(maxIterations)
            .withSeed(seed)
            .withDistance(distance)
            .withEpsilon(epsilon);

        KMeansModel mdl = trainer.fit(
            datasetBuilder,
            featureExtractor,
            lbExtractor
        );

        return mdl.centers();
    }

    /** */
    private ProbableLabel fillProbableLabel(int centroidIdx, CentroidStat centroidStat) {
        TreeMap<Double, Double> clsLbls = new TreeMap<>();

        // add all class labels as keys
        centroidStat.clsLblsSet.forEach(t -> clsLbls.put(t, 0.0));

        ConcurrentHashMap<Double, Integer> centroidLbDistribution
            = centroidStat.centroidStat().get(centroidIdx);

        if(centroidStat.counts.containsKey(centroidIdx)){

            int clusterSize = centroidStat
                .counts
                .get(centroidIdx);

            clsLbls.keySet().forEach(
                (label) -> clsLbls.put(label, centroidLbDistribution.containsKey(label) ? ((double) (centroidLbDistribution.get(label)) / clusterSize) : 0.0)
            );
        }
        return new ProbableLabel(clsLbls);
    }

    /** */
    private <K, V> CentroidStat getCentroidStat(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, Vector[] centers) {
        PartitionDataBuilder<K, V, EmptyContext, LabeledVectorSet<Double, LabeledVector>> partDataBuilder = new LabeledDatasetPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor
        );

        try (Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset = datasetBuilder.build(
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
                    res.labels().add(lb);

                    ConcurrentHashMap<Double, Integer> centroidStat = res.centroidStat.get(centroidIdx);

                    if (centroidStat == null) {
                        centroidStat = new ConcurrentHashMap<>();
                        centroidStat.put(lb, 1);
                        res.centroidStat.put(centroidIdx, centroidStat);
                    } else {
                        int cnt = centroidStat.getOrDefault(lb, 0);
                        centroidStat.put(lb, cnt + 1);
                    }

                    res.counts.merge(centroidIdx, 1,
                        (IgniteBiFunction<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
                }
                return res;
            }, (a, b) -> {
                if (a == null)
                    return b == null ? new CentroidStat() : b;
                if (b == null)
                    return a;
                return a.merge(b);
            });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Find the closest cluster center index and distance to it from a given point.
     *
     * @param centers Centers to look in.
     * @param pnt     Point.
     */
    private IgniteBiTuple<Integer, Double> findClosestCentroid(Vector[] centers, LabeledVector pnt) {
        double bestDistance = Double.POSITIVE_INFINITY;
        int bestInd = 0;

        for (int i = 0; i < centers.length; i++) {
            if (centers[i] != null) {
                double dist = distance.compute(centers[i], pnt.features());
                if (dist < bestDistance) {
                    bestDistance = dist;
                    bestInd = i;
                }
            }
        }
        return new IgniteBiTuple<>(bestInd, bestDistance);
    }


    /**
     * Gets the amount of clusters.
     *
     * @return The parameter value.
     */
    public int getK() {
        return k;
    }

    /**
     * Set up the amount of clusters.
     *
     * @param k The parameter value.
     * @return Model with new amount of clusters parameter value.
     */
    public ANNClassificationTrainer withK(int k) {
        this.k = k;
        return this;
    }

    /**
     * Gets the max number of iterations before convergence.
     *
     * @return The parameter value.
     */
    public int getMaxIterations() {
        return maxIterations;
    }

    /**
     * Set up the max number of iterations before convergence.
     *
     * @param maxIterations The parameter value.
     * @return Model with new max number of iterations before convergence parameter value.
     */
    public ANNClassificationTrainer withMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
        return this;
    }

    /**
     * Gets the epsilon.
     *
     * @return The parameter value.
     */
    public double getEpsilon() {
        return epsilon;
    }

    /**
     * Set up the epsilon.
     *
     * @param epsilon The parameter value.
     * @return Model with new epsilon parameter value.
     */
    public ANNClassificationTrainer withEpsilon(double epsilon) {
        this.epsilon = epsilon;
        return this;
    }

    /**
     * Gets the distance.
     *
     * @return The parameter value.
     */
    public DistanceMeasure getDistance() {
        return distance;
    }

    /**
     * Set up the distance.
     *
     * @param distance The parameter value.
     * @return Model with new distance parameter value.
     */
    public ANNClassificationTrainer withDistance(DistanceMeasure distance) {
        this.distance = distance;
        return this;
    }

    /**
     * Gets the seed number.
     *
     * @return The parameter value.
     */
    public long getSeed() {
        return seed;
    }

    /**
     * Set up the seed.
     *
     * @param seed The parameter value.
     * @return Model with new seed parameter value.
     */
    public ANNClassificationTrainer withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /** Service class used for statistics. */
    public static class CentroidStat {

        /** Count of points closest to the center with a given index. */
        ConcurrentHashMap<Integer, ConcurrentHashMap<Double, Integer>> centroidStat = new ConcurrentHashMap<>();

        /** Count of points closest to the center with a given index. */
        ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();

        /** Set of unique labels. */
        ConcurrentSkipListSet<Double> clsLblsSet = new ConcurrentSkipListSet<>();

        /** Merge current */
        CentroidStat merge(CentroidStat other) {
            this.counts = MapUtil.mergeMaps(counts, other.counts, (i1, i2) -> i1 + i2, ConcurrentHashMap::new);
            this.centroidStat = MapUtil.mergeMaps(centroidStat, other.centroidStat, (m1, m2) ->
                MapUtil.mergeMaps(m1, m2, (i1, i2) -> i1 + i2, ConcurrentHashMap::new), ConcurrentHashMap::new);
            this.clsLblsSet.addAll(other.clsLblsSet);
            return this;
        }

        /** */
        public ConcurrentSkipListSet<Double> labels() {
            return clsLblsSet;
        }

        /** */
        ConcurrentHashMap<Integer, ConcurrentHashMap<Double, Integer>> centroidStat() {
            return centroidStat;
        }
    }
}
