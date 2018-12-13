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

package org.apache.ignite.ml.clustering.kmeans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.util.MapUtil;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * The trainer for KMeans algorithm.
 */
public class KMeansTrainer extends SingleLabelDatasetTrainer<KMeansModel> {
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
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @return Model.
     */
    @Override public <K, V> KMeansModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        return updateModel(null, datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override public KMeansTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (KMeansTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> KMeansModel updateModel(KMeansModel mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, EmptyContext, LabeledVectorSet<Double, LabeledVector>> partDataBuilder = new LabeledDatasetPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor
        );

        Vector[] centers;

        try (Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset = datasetBuilder.build(
            envBuilder,
            (env, upstream, upstreamSize) -> new EmptyContext(),
            partDataBuilder
        )) {
            final Integer cols = dataset.compute(org.apache.ignite.ml.structures.Dataset::colSize, (a, b) -> {
                if (a == null)
                    return b == null ? 0 : b;
                if (b == null)
                    return a;
                return b;
            });

            if (cols == null)
                return getLastTrainedModelOrThrowEmptyDatasetException(mdl);

            centers = Optional.ofNullable(mdl)
                .map(KMeansModel::getCenters)
                .orElseGet(() -> initClusterCentersRandomly(dataset, k));

            boolean converged = false;
            int iteration = 0;

            while (iteration < maxIterations && !converged) {
                Vector[] newCentroids = new DenseVector[k];

                TotalCostAndCounts totalRes = calcDataForNewCentroids(centers, dataset, cols);

                converged = true;

                for (Integer ind : totalRes.sums.keySet()) {
                    Vector massCenter = totalRes.sums.get(ind).times(1.0 / totalRes.counts.get(ind));

                    if (converged && distance.compute(massCenter, centers[ind]) > epsilon * epsilon)
                        converged = false;

                    newCentroids[ind] = massCenter;
                }

                iteration++;
                for (int i = 0; i < centers.length; i++) {
                    if (newCentroids[i] != null)
                        centers[i] = newCentroids[i];
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new KMeansModel(centers, distance);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(KMeansModel mdl) {
        return mdl.getCenters().length == k && mdl.distanceMeasure().equals(distance);
    }

    /**
     * Prepares the data to define new centroids on current iteration.
     *
     * @param centers Current centers on the current iteration.
     * @param dataset Dataset.
     * @param cols Amount of columns.
     * @return Helper data to calculate the new centroids.
     */
    private TotalCostAndCounts calcDataForNewCentroids(Vector[] centers,
        Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset, int cols) {
        final Vector[] finalCenters = centers;

        return dataset.compute(data -> {
            TotalCostAndCounts res = new TotalCostAndCounts();

            for (int i = 0; i < data.rowSize(); i++) {
                final IgniteBiTuple<Integer, Double> closestCentroid = findClosestCentroid(finalCenters, data.getRow(i));

                int centroidIdx = closestCentroid.get1();

                data.setLabel(i, centroidIdx);

                res.totalCost += closestCentroid.get2();
                res.sums.putIfAbsent(centroidIdx, VectorUtils.zeroes(cols));

                int finalI = i;
                res.sums.compute(centroidIdx,
                    (IgniteBiFunction<Integer, Vector, Vector>)(ind, v) -> {
                        Vector features = data.getRow(finalI).features();
                        return v == null ? features : v.plus(features);
                    });

                res.counts.merge(centroidIdx, 1,
                    (IgniteBiFunction<Integer, Integer, Integer>)(i1, i2) -> i1 + i2);
            }
            return res;
        }, (a, b) -> {
            if (a == null)
                return b == null ? new TotalCostAndCounts() : b;
            if (b == null)
                return a;
            return a.merge(b);
        });
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
            double dist = distance.compute(centers[i], pnt.features());
            if (dist < bestDistance) {
                bestDistance = dist;
                bestInd = i;
            }
        }
        return new IgniteBiTuple<>(bestInd, bestDistance);
    }

    /**
     * K cluster centers are initialized randomly.
     *
     * @param dataset The dataset to pick up random centers.
     * @param k Amount of clusters.
     * @return K cluster centers.
     */
    private Vector[] initClusterCentersRandomly(Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset,
        int k) {
        Vector[] initCenters = new DenseVector[k];

        // Gets k or less vectors from each partition.
        List<LabeledVector> rndPnts = dataset.compute(data -> {
            List<LabeledVector> rndPnt = new ArrayList<>();

            if (data.rowSize() != 0) {
                if (data.rowSize() > k) { // If it's enough rows in partition to pick k vectors.
                    final Random random = new Random(seed);

                    for (int i = 0; i < k; i++) {
                        Set<Integer> uniqueIndices = new HashSet<>();
                        int nextIdx = random.nextInt(data.rowSize());
                        int maxRandomSearch = k; // It required to make the next cycle is finite.
                        int cntr = 0;

                        // Repeat nextIdx generation if it was picked earlier.
                        while (uniqueIndices.contains(nextIdx) && cntr < maxRandomSearch) {
                            nextIdx = random.nextInt(data.rowSize());
                            cntr++;
                        }
                        uniqueIndices.add(nextIdx);

                        rndPnt.add(data.getRow(nextIdx));
                    }
                }
                else // If it's not enough vectors to pick k vectors.
                    for (int i = 0; i < data.rowSize(); i++)
                        rndPnt.add(data.getRow(i));
            }
            return rndPnt;
        }, (a, b) -> {
            if (a == null)
                return b == null ? new ArrayList<>() : b;
            if (b == null)
                return a;
            return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
        });

        // Shuffle them.
        Collections.shuffle(rndPnts);

        // Pick k vectors randomly.
        if (rndPnts.size() >= k) {
            for (int i = 0; i < k; i++) {
                final LabeledVector rndPnt = rndPnts.get(new Random(seed).nextInt(rndPnts.size()));
                rndPnts.remove(rndPnt);
                initCenters[i] = rndPnt.features();
            }
        }
        else
            throw new RuntimeException("The KMeans Trainer required more than " + k + " vectors to find " + k + " clusters");

        return initCenters;
    }

    /** Service class used for statistics. */
    public static class TotalCostAndCounts {
        /** */
        double totalCost;

        /** */
        ConcurrentHashMap<Integer, Vector> sums = new ConcurrentHashMap<>();

        /** Count of points closest to the center with a given index. */
        ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();

        /** Count of points closest to the center with a given index. */
        ConcurrentHashMap<Integer, ConcurrentHashMap<Double, Integer>> centroidStat = new ConcurrentHashMap<>();

        /** Merge current */
        TotalCostAndCounts merge(TotalCostAndCounts other) {
            this.totalCost += totalCost;
            this.sums = MapUtil.mergeMaps(sums, other.sums, Vector::plus, ConcurrentHashMap::new);
            this.counts = MapUtil.mergeMaps(counts, other.counts, (i1, i2) -> i1 + i2, ConcurrentHashMap::new);
            this.centroidStat = MapUtil.mergeMaps(centroidStat, other.centroidStat, (m1, m2) ->
                MapUtil.mergeMaps(m1, m2, (i1, i2) -> i1 + i2, ConcurrentHashMap::new), ConcurrentHashMap::new);
            return this;
        }

        /**
         * @return centroid statistics.
         */
        public ConcurrentHashMap<Integer, ConcurrentHashMap<Double, Integer>> getCentroidStat() {
            return centroidStat;
        }
    }

    /**
     * Gets the amount of clusters.
     *
     * @return The parameter value.
     */
    public int getAmountOfClusters() {
        return k;
    }

    /**
     * Set up the amount of clusters.
     *
     * @param k The parameter value.
     * @return Model with new amount of clusters parameter value.
     */
    public KMeansTrainer withAmountOfClusters(int k) {
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
    public KMeansTrainer withMaxIterations(int maxIterations) {
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
    public KMeansTrainer withEpsilon(double epsilon) {
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
    public KMeansTrainer withDistance(DistanceMeasure distance) {
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
    public KMeansTrainer withSeed(long seed) {
        this.seed = seed;
        return this;
    }
}
