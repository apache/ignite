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

package org.apache.ignite.ml.partitionedClustering.kmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.util.MapUtil;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.svm.SVMPartitionContext;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;


/**
 */
public class KMeansTrainer implements SingleLabelDatasetTrainer<KMeansModel2> {
    /** Amount of clusters. */
    private int k = 2;

    private int maxIterations = 10;

    private double epsilon = 1e-4;

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
    @Override public <K, V> KMeansModel2 fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, SVMPartitionContext, LabeledDataset<Double, LabeledVector>> partDataBuilder = new LabeledDatasetPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor
        );


        Vector[] centers;

        try(Dataset<SVMPartitionContext, LabeledDataset<Double, LabeledVector>> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new SVMPartitionContext(),
            partDataBuilder
        )) {
            final int cols = dataset.compute(data -> data.colSize(), (a, b) -> a == null ? b : a);
            centers = initClusterCenters(dataset, k);

            boolean converged = false;
            int iteration = 0;

            while (iteration < maxIterations && !converged) {

                Vector[] newCentroids = new DenseLocalOnHeapVector[k];

                final Vector[] finalCenters = centers;
                TotalCostAndCounts totalResult = dataset.compute(data -> {
                    TotalCostAndCounts result = new TotalCostAndCounts();

                    for (int i = 0; i < data.rowSize(); i++) {
                        // For each element in the dataset, chose the closest centroid.
                        // Make that centroid the element's label.
                        final IgniteBiTuple<Integer, Double> closestCentroid = findClosestCentroid(finalCenters, data.getRow(i));
                        int centroidIdx = closestCentroid.get1();
                        data.setLabel(i, centroidIdx);

                        result.totalCost += closestCentroid.get2();
                        result.sums.putIfAbsent(centroidIdx, VectorUtils.zeroes(cols));

                        int finalI = i;
                        result.sums.compute(centroidIdx,
                            (IgniteBiFunction<Integer, Vector, Vector>)(ind, v) -> v.plus(data.getRow(finalI).features()));

                        result.counts.merge(centroidIdx, 1,
                            (IgniteBiFunction<Integer, Integer, Integer>)(i1, i2) -> i1 + i2);

                    }
                    return result;
                }, (a, b) -> a == null ? b : a.merge(b));


                converged = true;

                for (Integer ind : totalResult.sums.keySet()) {
                    Vector massCenter = totalResult.sums.get(ind).times(1.0 / totalResult.counts.get(ind));

                    if (converged && distance.compute(massCenter, centers[ind]) > epsilon * epsilon)
                        converged = false;

                    centers[ind] = massCenter;
                }

                iteration++;
                centers = newCentroids;
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new KMeansModel2(centers, distance);
    }

/*
    // TODO: need to use ctx for centroids like shared data
    // Each centroid is the geometric mean of the points that have that centroid's label.
    private Vector[] calculateNewCentroids(Dataset<SVMPartitionContext, LabeledDataset<Double, LabeledVector>> dataset, Vector[] centers, int cols) {
        Vector[] newCentroids = new DenseLocalOnHeapVector[k];

        TotalCostAndCounts totalResult = dataset.compute(data -> {
            TotalCostAndCounts result = new TotalCostAndCounts();

            for (int i = 0; i < data.rowSize(); i++) {
                // For each element in the dataset, chose the closest centroid.
                // Make that centroid the element's label.
                final IgniteBiTuple<Integer, Double> closestCentroid = findClosestCentroid(centers, data.getRow(i));
                int centroidIdx = closestCentroid.get1();
                data.setLabel(i, centroidIdx);

                result.totalCost += closestCentroid.get2();
                result.sums.putIfAbsent(centroidIdx, VectorUtils.zeroes(cols));

                result.sums.compute(centroidIdx,
                    (IgniteBiFunction<Integer, Vector, Vector>)(ind, v) -> v.plus(VectorUtils.fromMap(data.getRow(i), false)));

                result.counts.merge(centroidIdx, 1,
                    (IgniteBiFunction<Integer, Integer, Integer>)(i1, i2) -> i1 + i2);

            }
            return result;
        }, (a, b) -> a == null ? b : a.merge(b));


        boolean converged = true;

        for (Integer ind : totalResult.sums.keySet()) {
            Vector massCenter = totalResult.sums.get(ind).times(1.0 / totalResult.counts.get(ind));

            if (converged && distance.compute(massCenter, centers[ind]) > epsilon * epsilon)
                converged = false;

            centers[ind] = massCenter;
        }
        return  newCentroids;
    }*/


    /**
     * Find the closest cluster center index and distance to it from a given point.
     *
     * @param centers Centers to look in.
     * @param pnt Point.
     */
    private IgniteBiTuple<Integer, Double>  findClosestCentroid(Vector[] centers, LabeledVector pnt) {
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


    // RANDOM
    private Vector[] initClusterCenters(Dataset<SVMPartitionContext, LabeledDataset<Double, LabeledVector>> dataset, int k) {

        Vector[] initCenters = new DenseLocalOnHeapVector[k];

        List<LabeledVector> rndPnts = dataset.compute(data -> {
            List<LabeledVector> rndPnt = new ArrayList<>();
            rndPnt.add(data.getRow( ThreadLocalRandom.current().nextInt(data.rowSize())));
            return rndPnt;
        }, (a, b) -> a == null ? b : Stream.concat(a.stream(), b.stream()).collect(Collectors.toList()));


        for (int i = 0; i < k; i++) {
            final LabeledVector rndPnt = rndPnts.get(ThreadLocalRandom.current().nextInt(rndPnts.size()));
            rndPnts.remove(rndPnt);
            initCenters[i] = rndPnt.features();
        }

        return initCenters;
    }



    /** Service class used for statistics. */
    private static class TotalCostAndCounts {
        /** */
        public double totalCost;

        /** */
        public ConcurrentHashMap<Integer, Vector> sums = new ConcurrentHashMap<>();

        /** Count of points closest to the center with a given index. */
        public ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();

        /** Merge current */
        public TotalCostAndCounts merge(TotalCostAndCounts other) {
            this.totalCost += totalCost;
            MapUtil.mergeMaps(sums, other.sums, Vector::plus, ConcurrentHashMap::new);
            MapUtil.mergeMaps(counts, other.counts, (i1, i2) -> i1 + i2, ConcurrentHashMap::new);
            return this;
        }
    }

}



