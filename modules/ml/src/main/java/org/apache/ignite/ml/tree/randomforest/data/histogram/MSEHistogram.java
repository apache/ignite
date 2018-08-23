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

package org.apache.ignite.ml.tree.randomforest.data.histogram;

import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MSEHistogram implements ImpurityComputer<BaggedVector, MSEHistogram> {
    private final BucketMeta bucketMeta;
    private final int featureId;
    private final int sampleId;
    private final Set<Integer> bucketIds;
    private final FeatureHistogram<BaggedVector> counters;
    private final FeatureHistogram<BaggedVector> ys;
    private final FeatureHistogram<BaggedVector> y2s;

    public MSEHistogram(int sampleId, BucketMeta bucketMeta) {
        this.bucketMeta = bucketMeta;
        this.featureId = bucketMeta.getFeatureMeta().getFeatureId();
        this.sampleId = sampleId;

        counters = new FeatureHistogram<>(this::bucketMap, this::counterMap);
        ys = new FeatureHistogram<>(this::bucketMap, this::ysMap);
        y2s = new FeatureHistogram<>(this::bucketMap, this::y2sMap);
        bucketIds = new TreeSet<>();
    }

    @Override public void addElement(BaggedVector vector) {
        counters.addElement(vector);
        ys.addElement(vector);
        y2s.addElement(vector);
    }

    @Override public void addHist(MSEHistogram other) {
        assert featureId == other.featureId;
        assert sampleId == other.sampleId;

        bucketIds.addAll(other.bucketIds);
        counters.addHist(other.counters);
        ys.addHist(other.ys);
        y2s.addHist(other.y2s);
    }

    @Override public Set<Integer> buckets() {
        return bucketIds;
    }

    @Override public Optional<Double> get(Integer bucket) {
        throw new NotImplementedException();
    }

    @Override public Optional<NodeSplit> findBestSplit() {
        double bestImpurity = Double.POSITIVE_INFINITY;
        double bestSplitValue = Double.NEGATIVE_INFINITY;
        int bestBucketId = -1;

        TreeMap<Integer, Double> counterDistrib = counters.computeDistributionFunction();
        TreeMap<Integer, Double> ysDistrib = ys.computeDistributionFunction();
        TreeMap<Integer, Double> y2sDistrib = y2s.computeDistributionFunction();

        System.out.println(counterDistrib.size());
        counterDistrib.forEach((bucket, counter) -> {
            System.out.print(counter + " ");
        });
        System.out.println();
        if (0 == 0)
            throw new RuntimeException();

        double cntrMax = counterDistrib.lastEntry().getValue();
        double ysMax = ysDistrib.lastEntry().getValue();
        double y2sMax = y2sDistrib.lastEntry().getValue();

        double lastLeftCntrVal = 0.0;
        double lastLeftYVal = 0.0;
        double lastLeftY2Val = 0.0;

        for (Integer bucketId : bucketIds) {
            double leftCnt = counterDistrib.getOrDefault(bucketId, lastLeftCntrVal);
            double rightCnt = cntrMax - leftCnt;
            double leftY = ysDistrib.getOrDefault(bucketId, lastLeftYVal);
            double rightY = ysMax - leftY;
            double leftY2 = y2sDistrib.getOrDefault(bucketId, lastLeftY2Val);
            double rightY2 = y2sMax - leftY2;

            double impurity = 0.0;

            if (leftCnt > 0)
                impurity += impurity(leftCnt, leftY, leftY2);
            if (rightCnt > 0)
                impurity += impurity(rightCnt, rightY, rightY2);

            if (impurity < bestImpurity) {
                bestImpurity = impurity;
                bestSplitValue = bucketMeta.bucketIdToValue(bucketId);
                bestBucketId = bucketId;
            }
        }

        int minBucketId = Integer.MAX_VALUE;
        int maxBucketId = Integer.MIN_VALUE;
        for (Integer bucketId : bucketIds) {
            minBucketId = Math.min(minBucketId, bucketId);
            maxBucketId = Math.max(maxBucketId, bucketId);
        }

        if (bestBucketId == minBucketId || bestBucketId == maxBucketId)
            return Optional.empty();
        else
            return Optional.of(new NodeSplit(featureId, bestSplitValue, bestImpurity));
    }

    private double impurity(double count, double ys, double y2s) {
        return y2s - 2.0 * ys / count * ys + Math.pow(ys / count, 2) * count;
    }

    private Integer bucketMap(BaggedVector vec) {
        int bucketId = bucketMeta.getBucketId(vec.getFeatures().get(featureId));
        this.bucketIds.add(bucketId);
        return bucketId;
    }

    private Double counterMap(BaggedVector vec) {
        return (double)vec.getRepetitionsCounters()[sampleId];
    }

    private Double ysMap(BaggedVector vec) {
        return vec.getRepetitionsCounters()[sampleId] * vec.getLabel();
    }

    private Double y2sMap(BaggedVector vec) {
        return vec.getRepetitionsCounters()[sampleId] * Math.pow(vec.getLabel(), 2);
    }

    FeatureHistogram<BaggedVector> getCounters() {
        return counters;
    }

    FeatureHistogram<BaggedVector> getYs() {
        return ys;
    }

    FeatureHistogram<BaggedVector> getY2s() {
        return y2s;
    }
}
