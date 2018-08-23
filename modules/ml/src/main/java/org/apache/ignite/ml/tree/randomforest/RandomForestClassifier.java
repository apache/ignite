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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetPartition;
import org.apache.ignite.ml.tree.randomforest.data.BaggedVector;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.histogram.BucketMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureHistogram;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.GiniHistogram;

public class RandomForestClassifier extends RandomForest<GiniHistogram, RandomForestClassifier> {
    private Map<Double, Integer> lblMapping = new HashMap<>();

    public RandomForestClassifier(List<FeatureMeta> meta) {
        super(meta);
    }

    @Override protected RandomForestClassifier instance() {
        return this;
    }

    @Override protected void init(Dataset<EmptyContext, BaggedDatasetPartition> dataset) {
        Set<Double> uniqLabels = dataset.compute(
            x -> {
                Set<Double> labels = new HashSet<>();
                for (int i = 0; i < x.getRowsCount(); i++)
                    labels.add(x.getRow(i).getLabel());
                return labels;
            },
            (l, r) -> {
                if (l == null)
                    return r;
                if (r == null)
                    return l;
                Set<Double> lbls = new HashSet<>();
                lbls.addAll(l);
                lbls.addAll(r);
                return lbls;
            }
        );

        int i = 0;
        for (Double label : uniqLabels)
            lblMapping.put(label, i++);

        super.init(dataset);
    }

    @Override protected ModelsComposition buildComposition(List<RandomForest.TreeRoot> models) {
        return new ModelsComposition(models, new OnMajorityPredictionsAggregator());
    }

    @Override protected GiniHistogram createImpurityComputer(int sampleId, BucketMeta meta) {
        return new GiniHistogram(sampleId, lblMapping, meta);
    }

    @Override protected void computeLeafValues(ArrayList<TreeRoot> roots,
        Dataset<EmptyContext, BaggedDatasetPartition> dataset) {

        Map<NodeId, TreeNode> leafs = roots.stream().flatMap(r -> r.getLeafs().stream())
            .collect(Collectors.toMap(TreeNode::getId, n -> n));

        Map<NodeId, FeatureHistogram<BaggedVector>> stats = dataset.compute(
            data -> {
                Map<NodeId, FeatureHistogram<BaggedVector>> res = new HashMap<>();
                for (int sampleId = 0; sampleId < roots.size(); sampleId++) {
                    final int sampleIdConst = sampleId;

                    data.foreach(vec -> {
                        NodeId leafId = roots.get(sampleIdConst).getNode().predictNextNodeKey(vec.getFeatures());
                        if (!leafs.containsKey(leafId))
                            throw new IllegalStateException();

                        if (!res.containsKey(leafId)) {
                            res.put(leafId, new FeatureHistogram<>(
                                x -> lblMapping.get(x.getLabel()),
                                x -> (double)x.getRepetitionsCounters()[sampleIdConst])
                            );
                        }

                        res.get(leafId).addElement(vec);
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
                    else if (r.containsKey(key))
                        l.get(key).addHist(r.get(key));
                }

                return l;
            });

        leafs.forEach((id, leaf) -> {
            FeatureHistogram<BaggedVector> histogram = stats.get(id);
            Integer bucketId = histogram.buckets().stream()
                .max(Comparator.comparing(b -> histogram.get(b).orElse(0.0)))
                .get();
            Double lb = lblMapping.entrySet().stream()
                .filter(x -> x.getValue().equals(bucketId))
                .findFirst()
                .get().getKey();
            leaf.setValue(lb);
        });
    }
}
