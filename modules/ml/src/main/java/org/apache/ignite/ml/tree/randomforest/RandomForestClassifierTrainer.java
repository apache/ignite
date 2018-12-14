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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.randomforest.data.TreeRoot;
import org.apache.ignite.ml.tree.randomforest.data.impurity.GiniHistogram;
import org.apache.ignite.ml.tree.randomforest.data.impurity.GiniHistogramsComputer;
import org.apache.ignite.ml.tree.randomforest.data.impurity.ImpurityHistogramsComputer;
import org.apache.ignite.ml.tree.randomforest.data.statistics.ClassifierLeafValuesComputer;
import org.apache.ignite.ml.tree.randomforest.data.statistics.LeafValuesComputer;

/**
 * Classifier trainer based on RandomForest algorithm.
 */
public class RandomForestClassifierTrainer
    extends RandomForestTrainer<ObjectHistogram<BootstrappedVector>, GiniHistogram, RandomForestClassifierTrainer> {
    /** Label mapping. */
    private Map<Double, Integer> lblMapping = new HashMap<>();

    /**
     * Constructs an instance of RandomForestClassifierTrainer.
     *
     * @param meta Features meta.
     */
    public RandomForestClassifierTrainer(List<FeatureMeta> meta) {
        super(meta);
    }

    /** {@inheritDoc} */
    @Override protected RandomForestClassifierTrainer instance() {
        return this;
    }

    /**
     * Aggregates all unique labels from dataset and assigns integer id value for each label.
     * This id can be used as index in arrays or lists.
     *
     * @param dataset Dataset.
     * @return true if initialization was done.
     */
    @Override protected boolean init(Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {
        Set<Double> uniqLabels = dataset.compute(
            x -> {
                Set<Double> labels = new HashSet<>();
                for (int i = 0; i < x.getRowsCount(); i++)
                    labels.add(x.getRow(i).label());
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

        if(uniqLabels == null)
            return false;

        int i = 0;
        for (Double label : uniqLabels)
            lblMapping.put(label, i++);

        return super.init(dataset);
    }

    /** {@inheritDoc} */
    @Override protected ModelsComposition buildComposition(List<TreeRoot> models) {
        return new ModelsComposition(models, new OnMajorityPredictionsAggregator());
    }

    /** {@inheritDoc} */
    @Override protected ImpurityHistogramsComputer<GiniHistogram> createImpurityHistogramsComputer() {
        return new GiniHistogramsComputer(lblMapping);
    }

    /** {@inheritDoc} */
    @Override protected LeafValuesComputer<ObjectHistogram<BootstrappedVector>> createLeafStatisticsAggregator() {
        return new ClassifierLeafValuesComputer(lblMapping);
    }
}
