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

package org.apache.ignite.ml.tree.randomforest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetPartition;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.histogram.BucketMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.MSEHistogram;

public class RandomForestRegressor extends RandomForest<MSEHistogram, RandomForestRegressor> {
    public RandomForestRegressor(List<FeatureMeta> meta) {
        super(meta);
    }

    @Override protected RandomForestRegressor instance() {
        return this;
    }

    @Override
    protected void computeLeafValues(ArrayList<TreeRoot> roots, Dataset<EmptyContext, BaggedDatasetPartition> dataset) {
        Map<NodeId, TreeNode> leafs = roots.stream().flatMap(r -> r.getLeafs().stream())
            .collect(Collectors.toMap(TreeNode::getId, n -> n));

        Map<NodeId, IgniteBiTuple<Double, Integer>> stats = dataset.compute(
            data -> {
                Map<NodeId, IgniteBiTuple<Double, Integer>> res = new HashMap<>();
                for (int sampleId = 0; sampleId < roots.size(); sampleId++) {
                    final int sampleIdConst = sampleId;

                    data.foreach(vec -> {
                        NodeId leafId = roots.get(sampleIdConst).getNode().predictNextNodeKey(vec.getFeatures());
                        if (!leafs.containsKey(leafId))
                            throw new IllegalStateException();

                        if (!res.containsKey(leafId))
                            res.put(leafId, new IgniteBiTuple<>(0.0, 0));

                        IgniteBiTuple<Double, Integer> t = res.get(leafId);
                        t.set1(t.get1() + vec.getLabel() * vec.getRepetitionsCounters()[sampleIdConst]);
                        t.set2(t.get2() + vec.getRepetitionsCounters()[sampleIdConst]);
                    });
                }

                return res;
            },
            (l, r) -> {
                if (l == null)
                    return r;
                if (r == null)
                    return l;

                Set<NodeId> keys = new HashSet<>(l.keySet());
                keys.addAll(r.keySet());
                for (NodeId key : keys) {
                    if (!l.containsKey(key))
                        l.put(key, r.get(key));
                    else if (r.containsKey(key)) {
                        IgniteBiTuple<Double, Integer> t1 = l.get(key);
                        IgniteBiTuple<Double, Integer> t2 = r.get(key);
                        t1.set1(t1.get1() + t2.get1());
                        t1.set2(t1.get2() + t2.get2());
                    }
                }

                return l;
            });

        leafs.forEach((id, leaf) -> {
            IgniteBiTuple<Double, Integer> t = stats.get(id);
            leaf.setValue(t.get1() / t.get2());
        });
    }

    @Override protected ModelsComposition buildComposition(List<TreeRoot> models) {
        return new ModelsComposition(models, new MeanValuePredictionsAggregator());
    }

    @Override protected MSEHistogram createImpurityComputer(int sampleId, BucketMeta meta) {
        return new MSEHistogram(sampleId, meta);
    }
}
