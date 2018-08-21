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

package org.apache.ignite.ml.tree.randomforest.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.tree.randomforest.RandomForest;
import org.apache.ignite.ml.tree.randomforest.data.BaggedDatasetPartition;
import org.apache.ignite.ml.tree.randomforest.data.histogram.BucketMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.histogram.GiniHistogram;

public class RandomForestClassifier extends RandomForest<GiniHistogram> {
    private Map<Double, Integer> lblMapping = new HashMap<>();

    public RandomForestClassifier(List<FeatureMeta> meta,
        int countOfTrees, double subsampleSize, int maxDepth,
        double minImpurityDelta, long seed, IgniteFunction<List<FeatureMeta>, Integer> strat) {

        super(meta, countOfTrees, subsampleSize, maxDepth, minImpurityDelta, strat, seed);
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
        for(Double label : uniqLabels)
            lblMapping.put(label, i++);

        super.init(dataset);
    }

    @Override protected ModelsComposition buildComposition(List<RandomForest.TreeRoot> models) {
        return new ModelsComposition(models, new OnMajorityPredictionsAggregator());
    }

    @Override protected GiniHistogram createImpurityComputer(int sampleId, BucketMeta meta) {
        return new GiniHistogram(sampleId, lblMapping, meta);
    }
}
