package org.apache.ignite.ml.clustering;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.impl.SparseMatrixKey;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by kroonk on 21.10.17.
 */
public class FuzzyCMeansDistributedClusterer extends BaseFuzzyCMeansClusterer<SparseDistributedMatrix> {

    private Random random;
    private int initializationSteps;
    private long seed;
    private int kMeansMaxIterations;

    public FuzzyCMeansDistributedClusterer(DistanceMeasure measure, double exponentialWeight, double maxCentersDelta,
                                           Long seed, int initializationSteps, int kMeansMaxIterations) {
        super(measure, exponentialWeight, maxCentersDelta);

        this.seed = seed != null ? seed : new Random().nextLong();
        this.initializationSteps = initializationSteps;
        this.kMeansMaxIterations = kMeansMaxIterations;
        random = new Random(this.seed);
    }

    private ConcurrentHashMap<Integer, Double> getNewCosts(String cacheName, IgniteUuid uuid,
                                                           List<Vector> newCenters) {
        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                ConcurrentHashMap<Integer, Double>,
                ConcurrentHashMap<Integer, Double>>)(vectorWithIndex, map) -> {
                    Vector vector = VectorUtils.fromMap(vectorWithIndex.getValue(), false);

                    for (Vector center : newCenters) {
                        map.merge(vectorWithIndex.getKey().index(),
                                  distance(vector, center),
                                  Functions.MIN);
                    }

                    return map;
                },
                key -> key.matrixId().equals(uuid),
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                },
                new ConcurrentHashMap<>());
    }

    private List<Vector> getNewCenters(String cacheName, IgniteUuid uuid,
                                       ConcurrentHashMap<Integer, Double> costs, double costsSum, int k) {
        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                                  List<Vector>,
                                  List<Vector>>)(vectorWithIndex, centers) -> {
                    Integer index = vectorWithIndex.getKey().index();
                    Vector vector = VectorUtils.fromMap(vectorWithIndex.getValue(), false);

                    double probability = (costs.get(index) * 2.0 * k) / costsSum;

                    if (new Random(seed * (index + 1)).nextDouble() < probability) {
                        centers.add(vector);
                    }

                    return centers;
                },
                key -> key.matrixId().equals(uuid),
                (list1, list2) -> {
                    list1.addAll(list2);
                    return list1;
                },
                new ArrayList<>());
    }

    public ConcurrentHashMap<Integer, Integer> weightCenters(String cacheName, IgniteUuid uuid, List<Vector> centers) {
        if (centers.size() == 0) {
            return new ConcurrentHashMap<>();
        }

        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                                  ConcurrentHashMap<Integer, Integer>,
                                  ConcurrentHashMap<Integer, Integer>>)(vectorWithIndex, counts) -> {
                    Vector vector = VectorUtils.fromMap(vectorWithIndex.getValue(), false);

                    int nearest = 0;
                    double minDistance = distance(centers.get(nearest), vector);

                    for (int i = 0; i < centers.size(); i++) {
                        double currentDistance = distance(centers.get(i), vector);
                        if (currentDistance < minDistance) {
                            minDistance = currentDistance;
                            nearest = i;
                        }
                    }

                    counts.compute(nearest, (index, value) -> value == null ? 1 : value + 1);

                    return counts;
                },
                key -> key.matrixId().equals(uuid),
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                },
                new ConcurrentHashMap<>());
    }

    public Vector[] chooseKCenters(String cacheName, IgniteUuid uuid, List<Vector> centers, int k) {
        centers = centers.stream().distinct().collect(Collectors.toList());

        ConcurrentHashMap<Integer, Integer> weightsMap = weightCenters(cacheName, uuid, centers);

        List<Double> weights = new ArrayList<>(centers.size());

        for (int i = 0; i < centers.size(); i++) {
            weights.add(i, Double.valueOf(weightsMap.getOrDefault(i, 0)));
        }

        DenseLocalOnHeapMatrix centersMatrix = MatrixUtil.fromList(centers, true);

        KMeansLocalClusterer clusterer = new KMeansLocalClusterer(measure, kMeansMaxIterations, seed);
        return clusterer.cluster(centersMatrix, k).centers();
    }

    public Vector[] initializeCenters(SparseDistributedMatrix points, int k) {
        int pointsNumber = points.rowSize();

        Vector firstCenter = points.viewRow(random.nextInt(pointsNumber));

        List<Vector> centers = new ArrayList<>();
        List<Vector> newCenters = new ArrayList<>();

        centers.add(firstCenter);
        newCenters.add(firstCenter);

        ConcurrentHashMap<Integer, Double> costs = new ConcurrentHashMap<>();

        int step = 0;
        IgniteUuid uuid = points.getUUID();
        String cacheName = ((SparseDistributedMatrixStorage) points.getStorage()).cacheName();

        while(step < initializationSteps) {
            ConcurrentHashMap<Integer, Double> newCosts = getNewCosts(cacheName, uuid, newCenters);

            for (Integer key : newCosts.keySet()) {
                costs.merge(key, newCosts.get(key), Math::min);
            }

            double costsSum = costs.values().stream().mapToDouble(Double::valueOf).sum();

            newCenters = getNewCenters(cacheName, uuid, costs, costsSum, k);
            centers.addAll(newCenters);

            step++;
        }

        return chooseKCenters(cacheName, uuid, centers, k);
    }

    public MembershipsAndSums calculateMembership(SparseDistributedMatrix points, Vector[] centers) {
        String cacheName = ((SparseDistributedMatrixStorage) points.getStorage()).cacheName();
        IgniteUuid uuid = points.getUUID();
        double fuzzyMembershipCoefficient = 2 / (exponentialWeight - 1);

        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                        MembershipsAndSums,
                        MembershipsAndSums>)(vectorWithIndex, membershipsAndSums) -> {
                    Integer index = vectorWithIndex.getKey().index();
                    Vector point = VectorUtils.fromMap(vectorWithIndex.getValue(), false);
                    Vector distances = new DenseLocalOnHeapVector(centers.length);
                    Vector pointMemberships = new DenseLocalOnHeapVector(centers.length);

                    for (int i = 0; i < centers.length; i++) {
                        distances.setX(i, distance(centers[i], point));
                    }

                    for (int i = 0; i < centers.length; i++) {
                        double invertedFuzzyWeight = 0.0;
                        for (int j = 0; j < centers.length; j++) {
                            double value = Math.pow(distances.getX(i) / distances.getX(j), fuzzyMembershipCoefficient);
                            if (Double.isNaN(value)) {
                                value = 1.0;
                            }
                            invertedFuzzyWeight += value;
                        }
                        double membership = Math.pow(1.0 / invertedFuzzyWeight, exponentialWeight);
                        pointMemberships.setX(i, membership);
                    }

                    membershipsAndSums.memberships.put(index, pointMemberships);
                    membershipsAndSums.membershipSums = membershipsAndSums.membershipSums.plus(pointMemberships);

                    return membershipsAndSums;
                },
                key -> key.matrixId().equals(uuid),
                (mem1, mem2) -> {
                    mem1.merge(mem2);
                    return mem1;
                },
                new MembershipsAndSums(centers.length));
    }

    public Vector[] calculateNewCenters(SparseDistributedMatrix points, MembershipsAndSums membershipsAndSums, int k) {
        String cacheName = ((SparseDistributedMatrixStorage) points.getStorage()).cacheName();
        IgniteUuid uuid = points.getUUID();

        DenseLocalOnHeapVector[] centerSumsArray = new DenseLocalOnHeapVector[k];
        for (int i = 0; i < k; i++) {
            centerSumsArray[i] = new DenseLocalOnHeapVector(points.columnSize());
        }

        Vector[] centers = CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                                  Vector[],
                                  Vector[]>)(vectorWithIndex, centerSums) -> {
                    Integer index = vectorWithIndex.getKey().index();
                    Vector point = MatrixUtil.localCopyOf(VectorUtils.fromMap(vectorWithIndex.getValue(), false));
                    Vector pointMemberships = membershipsAndSums.memberships.get(index);

                    for (int i = 0; i < k; i++) {
                        Vector weightedPoint = point.times(pointMemberships.getX(i));
                        centerSums[i] = centerSums[i].plus(weightedPoint);
                    }

                    return centerSums;
                },
                key -> key.matrixId().equals(uuid),
                (sums1, sums2) -> {
                    for (int i = 0; i < k; i++) {
                        sums1[i] = sums1[i].plus(sums2[i]);
                    }
                    return sums1;
                },
                centerSumsArray);

        for (int i = 0; i < k; i++) {
            centers[i] = centers[i].divide(membershipsAndSums.membershipSums.getX(i));
        }

        return centers;
    }

    private boolean isFinished(Vector[] centers, Vector[] newCenters) {
        int numCenters = centers.length;

        for (int i = 0; i < numCenters; i++) {
            if (distance(centers[i], newCenters[i]) > maxCentersDelta) {
                return false;
            }
        }

        return true;
    }

    @Override
    public FuzzyCMeansModel cluster(SparseDistributedMatrix points, int k) {
        Vector[] centers = initializeCenters(points, k);

        boolean finished = false;
        while (!finished) {
            MembershipsAndSums newMembershipsAndSums = calculateMembership(points, centers);
            Vector[] newCenters = calculateNewCenters(points, newMembershipsAndSums, k);

            finished = isFinished(centers, newCenters);

            centers = newCenters;
        }

        return new FuzzyCMeansModel(centers, measure);
    }

    private class MembershipsAndSums {
        public ConcurrentHashMap<Integer, Vector> memberships = new ConcurrentHashMap<>();
        public Vector membershipSums;

        public MembershipsAndSums(int k) {
            membershipSums = new DenseLocalOnHeapVector(k);
        }

        public void merge(MembershipsAndSums another) {
            memberships.putAll(another.memberships);
            membershipSums = membershipSums.plus(another.membershipSums);
        }
    }
}
