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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class GiniHistogram implements ImpurityComputer<BaggedVector, GiniHistogram> {
    private final BucketMeta bucketMeta;
    private final int featureId;
    private final int sampleId;
    private final ArrayList<FeatureHistogram<BaggedVector>> hists;
    private final Map<Double, Integer> lblMapping;
    private final Set<Integer> bucketIds;

    public GiniHistogram(Map<Double, Integer> lblMapping, int featureId, int sampleId, BucketMeta bucketMeta) {
        this.hists = new ArrayList<>(lblMapping.size());
        this.featureId = featureId;
        this.sampleId = sampleId;
        this.bucketMeta = bucketMeta;
        this.lblMapping = lblMapping;

        for (int i = 0; i < lblMapping.size(); i++)
            hists.add(new FeatureHistogram<>(this::bucketMap, this::counterMap));

        this.bucketIds = new TreeSet<>();
    }

    @Override public void addElement(BaggedVector vector) {
        Integer lblId = lblMapping.get(vector.getLabel());
        hists.get(lblId).addElement(vector);
    }

    @Override public Optional<Double> get(Integer bucket) {
        throw new NotImplementedException();
    }

    @Override public void addHist(GiniHistogram other) {
        assert featureId == other.featureId;
        assert sampleId == other.sampleId;

        bucketIds.addAll(other.bucketIds);
        for (int i = 0; i < hists.size(); i++)
            hists.get(i).addHist(other.hists.get(i));
    }

    @Override public NodeSplit findBestSplit() {
        double bestImpurity = Double.POSITIVE_INFINITY;
        double bestSplitValue = Double.NEGATIVE_INFINITY;

        List<TreeMap<Integer, Double>> distributions = hists.stream().map(FeatureHistogram::computeDistributionFunction)
            .collect(Collectors.toList());
        double[] totalCounts = distributions.stream().mapToDouble(x -> x.lastEntry().getValue())
            .toArray();

        Map<Integer, Double> lastLeftValues = new HashMap<>();
        for(int i = 0; i < lblMapping.size(); i++)
            lastLeftValues.put(i, 0.0);

        for (Integer bucketId : bucketIds) {
            double totalToleftCnt = 0;
            double totalToRightCnt = 0;

            double leftImpurity = 0;
            double rightImpurity = 0;

            for (int lbId = 0; lbId < lblMapping.size(); lbId++) {
                Double left = distributions.get(lbId).get(bucketId);
                if(left == null)
                    left = lastLeftValues.get(lbId);

                totalToleftCnt += left;
                totalToRightCnt += totalCounts[lbId] - left;

                lastLeftValues.put(lbId, left);
            }

            for (int lbId = 0; lbId < lblMapping.size(); lbId++) {
                Double toLeftCnt = distributions.get(lbId).getOrDefault(bucketId, lastLeftValues.get(lbId));

                if (toLeftCnt > 0)
                    leftImpurity += Math.pow(toLeftCnt, 2) / totalToleftCnt;

                double toRightCnt = totalCounts[lbId] - toLeftCnt;
                if (toRightCnt > 0)
                    rightImpurity += (Math.pow(toRightCnt, 2)) / totalToRightCnt;
            }

            double impurityInBucket = -(leftImpurity + rightImpurity);
            if (impurityInBucket < bestImpurity) {
                bestImpurity = impurityInBucket;
                bestSplitValue = bucketMeta.bucketIdToValue(bucketId);
            }
        }

        return new NodeSplit(featureId, bestSplitValue, bestImpurity);
    }

    @Override public Set<Integer> buckets() {
        return bucketIds;
    }

    FeatureHistogram<BaggedVector> getHistForLabel(Double lbl) {
        return hists.get(lblMapping.get(lbl));
    }

    private Double counterMap(BaggedVector vec) {
        return (double)vec.getRepetitionsCounters()[sampleId];
    }

    private Integer bucketMap(BaggedVector vec) {
        int bucketId = bucketMeta.getBucketId(vec.getFeatures().get(featureId));
        this.bucketIds.add(bucketId);
        return bucketId;
    }

}
