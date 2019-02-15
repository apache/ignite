/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
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

    /** {@inheritDoc} */
    @Override public RandomForestClassifierTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (RandomForestClassifierTrainer)super.withEnvironmentBuilder(envBuilder);
    }
}
