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

package org.apache.ignite.ml.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.impl.SparseMatrixKey;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.util.MatrixUtil;

/** This class implements distributed version of Fuzzy C-Means clusterization of equal-weighted points. */
public class FuzzyCMeansDistributedClusterer extends BaseFuzzyCMeansClusterer<SparseDistributedMatrix> {
    /** Random numbers generator which is used in centers selection. */
    private Random rnd;

    /** The value that is used to initialize random numbers generator. */
    private long seed;

    /** The number of initialization steps each of which adds some number of candidates for being a center. */
    private int initSteps;

    /** The maximum number of iterations of K-Means algorithm which selects the required number of centers. */
    private int kMeansMaxIterations;

    /** The maximum number of FCM iterations. */
    private int cMeansMaxIterations;

    /**
     * Constructor that retains all required parameters.
     *
     * @param measure Distance measure.
     * @param exponentialWeight Specific constant which is used in calculating of membership matrix.
     * @param stopCond Flag that tells when algorithm should stop.
     * @param maxDelta The maximum distance between old and new centers or maximum difference between new and old
     *                 membership matrix elements for which algorithm must stop.
     * @param cMeansMaxIterations The maximum number of FCM iterations.
     * @param seed Seed for random numbers generator.
     * @param initSteps Number of steps of primary centers selection (the more steps, the more candidates).
     * @param kMeansMaxIterations The maximum number of K-Means iteration in primary centers selection.
     */
    public FuzzyCMeansDistributedClusterer(DistanceMeasure measure, double exponentialWeight,
                                           StopCondition stopCond, double maxDelta, int cMeansMaxIterations,
                                           Long seed, int initSteps, int kMeansMaxIterations) {
        super(measure, exponentialWeight, stopCond, maxDelta);

        this.seed = seed != null ? seed : new Random().nextLong();
        this.initSteps = initSteps;
        this.cMeansMaxIterations = cMeansMaxIterations;
        this.kMeansMaxIterations = kMeansMaxIterations;
        rnd = new Random(this.seed);
    }

    /** {@inheritDoc} */
    @Override public FuzzyCMeansModel cluster(SparseDistributedMatrix points, int k)
            throws MathIllegalArgumentException, ConvergenceException {
        GridArgumentCheck.notNull(points, "points");

        if (k < 2)
            throw new MathIllegalArgumentException("The number of clusters is less than 2");

        Vector[] centers = initializeCenters(points, k);

        MembershipsAndSums membershipsAndSums = null;

        int iteration = 0;
        boolean finished = false;
        while (!finished && iteration < cMeansMaxIterations) {
            MembershipsAndSums newMembershipsAndSums = calculateMembership(points, centers);
            Vector[] newCenters = calculateNewCenters(points, newMembershipsAndSums, k);

            if (stopCond == StopCondition.STABLE_CENTERS)
                finished = isFinished(centers, newCenters);
            else
                finished = isFinished(membershipsAndSums, newMembershipsAndSums);

            centers = newCenters;
            membershipsAndSums = newMembershipsAndSums;

            iteration++;
        }

        if (iteration == cMeansMaxIterations)
            throw new ConvergenceException("Fuzzy C-Means algorithm has not converged after " +
                    Integer.toString(iteration) + " iterations");

        return new FuzzyCMeansModel(centers, measure);
    }

    /**
     * Choose k primary centers from source points.
     *
     * @param points Matrix with source points.
     * @param k Number of centers.
     * @return Array of primary centers.
     */
    private Vector[] initializeCenters(SparseDistributedMatrix points, int k) {
        int pointsNum = points.rowSize();

        Vector firstCenter = points.viewRow(rnd.nextInt(pointsNum));

        List<Vector> centers = new ArrayList<>();
        List<Vector> newCenters = new ArrayList<>();

        centers.add(firstCenter);
        newCenters.add(firstCenter);

        ConcurrentHashMap<Integer, Double> costs = new ConcurrentHashMap<>();

        int step = 0;
        UUID uuid = points.getUUID();
        String cacheName = ((SparseDistributedMatrixStorage) points.getStorage()).cacheName();

        while(step < initSteps) {
            ConcurrentHashMap<Integer, Double> newCosts = getNewCosts(cacheName, uuid, newCenters);

            for (Integer key : newCosts.keySet())
                costs.merge(key, newCosts.get(key), Math::min);

            double costsSum = costs.values().stream().mapToDouble(Double::valueOf).sum();

            newCenters = getNewCenters(cacheName, uuid, costs, costsSum, k);
            centers.addAll(newCenters);

            step++;
        }

        return chooseKCenters(cacheName, uuid, centers, k);
    }

    /**
     * Calculate new distances from each point to the nearest center.
     *
     * @param cacheName Cache name of point matrix.
     * @param uuid Uuid of point matrix.
     * @param newCenters The list of centers that was added on previous step.
     * @return Hash map with distances.
     */
    private ConcurrentHashMap<Integer, Double> getNewCosts(String cacheName, UUID uuid,
                                                           List<Vector> newCenters) {
        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                        ConcurrentHashMap<Integer, Double>,
                        ConcurrentHashMap<Integer, Double>>)(vectorWithIndex, map) -> {
                    Vector vector = VectorUtils.fromMap(vectorWithIndex.getValue(), false);

                    for (Vector center : newCenters)
                        map.merge(vectorWithIndex.getKey().index(), distance(vector, center), Functions.MIN);

                    return map;
                },
                key -> key.dataStructureId().equals(uuid),
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                },
                ConcurrentHashMap::new);
    }

    /**
     * Choose some number of center candidates from source points according to their costs.
     *
     * @param cacheName Cache name of point matrix.
     * @param uuid Uuid of point matrix.
     * @param costs Hash map with costs (distances to nearest center).
     * @param costsSum The sum of costs.
     * @param k The estimated number of centers.
     * @return The list of new candidates.
     */
    private List<Vector> getNewCenters(String cacheName, UUID uuid,
                                       ConcurrentHashMap<Integer, Double> costs, double costsSum, int k) {
        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, Map<Integer, Double>>,
                                  List<Vector>,
                                  List<Vector>>)(vectorWithIndex, centers) -> {
                    Integer idx = vectorWithIndex.getKey().index();
                    Vector vector = VectorUtils.fromMap(vectorWithIndex.getValue(), false);

                    double probability = (costs.get(idx) * 2.0 * k) / costsSum;

                    if (rnd.nextDouble() < probability)
                        centers.add(vector);

                    return centers;
                },
                key -> key.dataStructureId().equals(uuid),
                (list1, list2) -> {
                    list1.addAll(list2);
                    return list1;
                },
                ArrayList::new);
    }

    /**
     * Weight candidates and use K-Means to choose required number of them.
     *
     * @param cacheName Cache name of the point matrix.
     * @param uuid Uuid of the point matrix.
     * @param centers The list of candidates.
     * @param k The estimated number of centers.
     * @return {@code k} centers.
     */
    private Vector[] chooseKCenters(String cacheName, UUID uuid, List<Vector> centers, int k) {
        centers = centers.stream().distinct().collect(Collectors.toList());

        ConcurrentHashMap<Integer, Integer> weightsMap = weightCenters(cacheName, uuid, centers);

        List<Double> weights = new ArrayList<>(centers.size());

        for (int i = 0; i < centers.size(); i++)
            weights.add(i, Double.valueOf(weightsMap.getOrDefault(i, 0)));

        DenseLocalOnHeapMatrix centersMatrix = MatrixUtil.fromList(centers, true);

        KMeansLocalClusterer clusterer = new KMeansLocalClusterer(measure, kMeansMaxIterations, seed);
        return clusterer.cluster(centersMatrix, k, weights).centers();
    }

    /**
     * Weight each center with number of points for which this center is the nearest.
     *
     * @param cacheName Cache name of the point matrix.
     * @param uuid Uuid of the point matrix.
     * @param centers The list of centers.
     * @return Hash map with weights.
     */
    public ConcurrentHashMap<Integer, Integer> weightCenters(String cacheName, UUID uuid, List<Vector> centers) {
        if (centers.size() == 0)
            return new ConcurrentHashMap<>();

        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                                  ConcurrentHashMap<Integer, Integer>,
                                  ConcurrentHashMap<Integer, Integer>>)(vectorWithIndex, counts) -> {
                    Vector vector = VectorUtils.fromMap(vectorWithIndex.getValue(), false);

                    int nearest = 0;
                    double minDistance = distance(centers.get(nearest), vector);

                    for (int i = 0; i < centers.size(); i++) {
                        double currDistance = distance(centers.get(i), vector);
                        if (currDistance < minDistance) {
                            minDistance = currDistance;
                            nearest = i;
                        }
                    }

                    counts.compute(nearest, (index, value) -> value == null ? 1 : value + 1);

                    return counts;
                },
                key -> key.dataStructureId().equals(uuid),
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                },
                ConcurrentHashMap::new);
    }

    /**
     * Calculate matrix of membership coefficients for each point and each center.
     *
     * @param points Matrix with source points.
     * @param centers Array of current centers.
     * @return Membership matrix and sums of membership coefficients for each center.
     */
    private MembershipsAndSums calculateMembership(SparseDistributedMatrix points, Vector[] centers) {
        String cacheName = ((SparseDistributedMatrixStorage) points.getStorage()).cacheName();
        UUID uuid = points.getUUID();
        double fuzzyMembershipCoefficient = 2 / (exponentialWeight - 1);

        MembershipsAndSumsSupplier supplier = new MembershipsAndSumsSupplier(centers.length);

        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                        MembershipsAndSums,
                        MembershipsAndSums>)(vectorWithIndex, membershipsAndSums) -> {
                    Integer idx = vectorWithIndex.getKey().index();
                    Vector pnt = VectorUtils.fromMap(vectorWithIndex.getValue(), false);
                    Vector distances = new DenseLocalOnHeapVector(centers.length);
                    Vector pntMemberships = new DenseLocalOnHeapVector(centers.length);

                    for (int i = 0; i < centers.length; i++)
                        distances.setX(i, distance(centers[i], pnt));

                    for (int i = 0; i < centers.length; i++) {
                        double invertedFuzzyWeight = 0.0;

                        for (int j = 0; j < centers.length; j++) {
                            double val = Math.pow(distances.getX(i) / distances.getX(j), fuzzyMembershipCoefficient);
                            if (Double.isNaN(val))
                                val = 1.0;

                            invertedFuzzyWeight += val;
                        }

                        double membership = Math.pow(1.0 / invertedFuzzyWeight, exponentialWeight);
                        pntMemberships.setX(i, membership);
                    }

                    membershipsAndSums.memberships.put(idx, pntMemberships);
                    membershipsAndSums.membershipSums = membershipsAndSums.membershipSums.plus(pntMemberships);

                    return membershipsAndSums;
                },
                key -> key.dataStructureId().equals(uuid),
                (mem1, mem2) -> {
                    mem1.merge(mem2);
                    return mem1;
                },
                supplier);
    }

    /**
     * Calculate new centers according to membership matrix.
     *
     * @param points Matrix with source points.
     * @param membershipsAndSums Membership matrix and sums of membership coefficient for each center.
     * @param k The number of centers.
     * @return Array of new centers.
     */
    private Vector[] calculateNewCenters(SparseDistributedMatrix points, MembershipsAndSums membershipsAndSums, int k) {
        String cacheName = ((SparseDistributedMatrixStorage) points.getStorage()).cacheName();
        UUID uuid = points.getUUID();

        CentersArraySupplier supplier = new CentersArraySupplier(k, points.columnSize());

        Vector[] centers = CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                                  Vector[],
                                  Vector[]>)(vectorWithIndex, centerSums) -> {
                    Integer idx = vectorWithIndex.getKey().index();
                    Vector pnt = MatrixUtil.localCopyOf(VectorUtils.fromMap(vectorWithIndex.getValue(), false));
                    Vector pntMemberships = membershipsAndSums.memberships.get(idx);

                    for (int i = 0; i < k; i++) {
                        Vector weightedPnt = pnt.times(pntMemberships.getX(i));
                        centerSums[i] = centerSums[i].plus(weightedPnt);
                    }

                    return centerSums;
                },
                key -> key.dataStructureId().equals(uuid),
                (sums1, sums2) -> {
                    for (int i = 0; i < k; i++)
                        sums1[i] = sums1[i].plus(sums2[i]);

                    return sums1;
                },
                supplier);

        for (int i = 0; i < k; i++)
            centers[i] = centers[i].divide(membershipsAndSums.membershipSums.getX(i));

        return centers;
    }

    /**
     * Check if centers have moved insignificantly.
     *
     * @param centers Old centers.
     * @param newCenters New centers.
     * @return The result of comparison.
     */
    private boolean isFinished(Vector[] centers, Vector[] newCenters) {
        int numCenters = centers.length;

        for (int i = 0; i < numCenters; i++)
            if (distance(centers[i], newCenters[i]) > maxDelta)
                return false;

        return true;
    }

    /**
     * Check memberships difference.
     *
     * @param membershipsAndSums Old memberships.
     * @param newMembershipsAndSums New memberships.
     * @return The result of comparison.
     */
    private boolean isFinished(MembershipsAndSums membershipsAndSums, MembershipsAndSums newMembershipsAndSums) {
        if (membershipsAndSums == null)
            return false;

        double currMaxDelta = 0.0;
        for (Integer key : membershipsAndSums.memberships.keySet()) {
            double distance = measure.compute(membershipsAndSums.memberships.get(key),
                                              newMembershipsAndSums.memberships.get(key));
            if (distance > currMaxDelta)
                currMaxDelta = distance;
        }

        return currMaxDelta <= maxDelta;
    }

    /** Service class used to optimize counting of membership sums. */
    private class MembershipsAndSums {
        /** Membership matrix. */
        public ConcurrentHashMap<Integer, Vector> memberships = new ConcurrentHashMap<>();

        /** Membership sums. */
        public Vector membershipSums;

        /**
         * Default constructor.
         *
         * @param k The number of centers.
         */
        public MembershipsAndSums(int k) {
            membershipSums = new DenseLocalOnHeapVector(k);
        }

        /**
         * Merge results of calculation for different parts of points.
         * @param another Another part of memberships and sums.
         */
        public void merge(MembershipsAndSums another) {
            memberships.putAll(another.memberships);
            membershipSums = membershipSums.plus(another.membershipSums);
        }
    }

    /** Service class that is used to create new {@link MembershipsAndSums} instances. */
    private class MembershipsAndSumsSupplier implements IgniteSupplier<MembershipsAndSums> {
        /** The number of centers */
        int k;

        /**
         * Constructor that retains the number of centers.
         *
         * @param k The number of centers.
         */
        public MembershipsAndSumsSupplier(int k) {
            this.k = k;
        }

        /**
         * Create new instance of {@link MembershipsAndSums}.
         *
         * @return {@link MembershipsAndSums} object.
         */
        @Override public MembershipsAndSums get() {
            return new MembershipsAndSums(k);
        }
    }

    /** Service class that is used to create new arrays of vectors. */
    private class CentersArraySupplier implements IgniteSupplier<Vector[]> {
        /** The number of centers. */
        int k;

        /** The number of coordinates. */
        int dim;

        /**
         * Constructor that retains all required parameters.
         *
         * @param k The number of centers.
         * @param dim The number of coordinates.
         */
        public CentersArraySupplier(int k, int dim) {
            this.k = k;
            this.dim = dim;
        }

        /**
         * Create new array of vectors.
         *
         * @return Array of vectors.
         */
        @Override public Vector[] get() {
            DenseLocalOnHeapVector[] centerSumsArr = new DenseLocalOnHeapVector[k];
            for (int i = 0; i < k; i++)
                centerSumsArr[i] = new DenseLocalOnHeapVector(dim);
            return centerSumsArr;
        }
    }
}
