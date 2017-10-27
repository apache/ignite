package org.apache.ignite.ml.clustering;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.impl.SparseMatrixKey;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kroonk on 21.10.17.
 */
public class FuzzyCMeansDistributedClusterer extends BaseFuzzyCMeansClusterer<SparseDistributedMatrix> {

    private Random random;
    private int initializationSteps;
    private long seed;

    public FuzzyCMeansDistributedClusterer(DistanceMeasure measure, double exponentialWeight, double maxCentersDelta) {
        super(measure, exponentialWeight, maxCentersDelta);
    }

    private double distance(Map<Integer, Double> vecMap, Vector vector) {
        return distance(VectorUtils.fromMap(vecMap, false), vector);
    }

    private ConcurrentHashMap<Integer, Double> getNewCosts(String cacheName, IgniteUuid uuid,
                                                           List<Vector> newCenters) {
        return CacheUtils.distributedFold(cacheName,
                (IgniteBiFunction<Cache.Entry<SparseMatrixKey, ConcurrentHashMap<Integer, Double>>,
                ConcurrentHashMap<Integer, Double>,
                ConcurrentHashMap<Integer, Double>>)(vectorWithIndex, map) -> {
                    for (Vector center : newCenters) {
                        map.merge(vectorWithIndex.getKey().index(),
                                  distance(vectorWithIndex.getValue(), center),
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

                    double probability = costs.get(index) * 2.0 * k / costsSum;

                    if (new Random(seed ^ index).nextDouble() < probability) {
                        centers.add(VectorUtils.fromMap(vectorWithIndex.getValue(), false));
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

    public void chooseKCenters(String cacheName, IgniteUuid uuid, List<Vector> centers, )

    public void initializeCenters(SparseDistributedMatrix points, int k) {
        int pointsNumber = points.rowSize();

        Vector firstCenter = points.viewRow(random.nextInt());

        List<Vector> centers = new ArrayList<>();
        List<Vector> newCenters = new ArrayList<>();

        centers.add(firstCenter);

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
    }

    @Override
    public FuzzyCMeansModel cluster(SparseDistributedMatrix points, int k) {
        return null;
    }
}
