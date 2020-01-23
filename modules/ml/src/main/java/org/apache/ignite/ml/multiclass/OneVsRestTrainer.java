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

package org.apache.ignite.ml.multiclass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.developer.PatchedPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.partition.LabelPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.structures.partition.LabelPartitionDataOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * This is a common heuristic trainer for multi-class labeled models.
 *
 * NOTE: The current implementation suffers from unbalanced training over the dataset due to unweighted approach during
 * the process of reassign labels from all range of labels to 0,1.
 */
public class OneVsRestTrainer<M extends IgniteModel<Vector, Double>>
    extends SingleLabelDatasetTrainer<MultiClassModel<M>> {
    /** The common binary classifier with all hyper-parameters to spread them for all separate trainings . */
    private SingleLabelDatasetTrainer<M> classifier;

    /** */
    public OneVsRestTrainer(SingleLabelDatasetTrainer<M> classifier) {
        this.classifier = classifier;
    }

    /** {@inheritDoc} */
    @Override public <K, V> MultiClassModel<M> fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
                                                   Preprocessor<K, V> extractor) {

        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> MultiClassModel<M> updateModel(MultiClassModel<M> newMdl,
                                                              DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> extractor) {

        List<Double> classes = extractClassLabels(datasetBuilder, extractor);

        if (classes.isEmpty())
            return getLastTrainedModelOrThrowEmptyDatasetException(newMdl);

        MultiClassModel<M> multiClsMdl = new MultiClassModel<>();

        classes.forEach(clsLb -> {
            IgniteFunction<Double, Double> lbTransformer = lb -> lb.equals(clsLb) ? 1.0 : 0.0;

            IgniteFunction<LabeledVector<Double>, LabeledVector<Double>> func = lv -> new LabeledVector<>(lv.features(), lbTransformer.apply(lv.label()));

            PatchedPreprocessor<K, V, Double, Double> patchedPreprocessor = new PatchedPreprocessor<>(func, extractor);

            M mdl = Optional.ofNullable(newMdl)
                .flatMap(multiClassModel -> multiClassModel.getModel(clsLb))
                .map(learnedModel -> classifier.update(learnedModel, datasetBuilder, patchedPreprocessor))
                .orElseGet(() -> classifier.fit(datasetBuilder, patchedPreprocessor));

            multiClsMdl.add(clsLb, mdl);
        });

        return multiClsMdl;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(MultiClassModel<M> mdl) {
        return true;
    }

    /** Iterates among dataset and collects class labels. */
    private <K, V> List<Double> extractClassLabels(DatasetBuilder<K, V> datasetBuilder,
                                                   Preprocessor<K, V> preprocessor) {
        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, EmptyContext, LabelPartitionDataOnHeap> partDataBuilder =
            new LabelPartitionDataBuilderOnHeap<>(preprocessor);

        List<Double> res = new ArrayList<>();

        try (Dataset<EmptyContext, LabelPartitionDataOnHeap> dataset = datasetBuilder.build(
            envBuilder,
            (env, upstream, upstreamSize) -> new EmptyContext(),
            partDataBuilder, learningEnvironment()
        )) {
            final Set<Double> clsLabels = dataset.compute(data -> {
                final Set<Double> locClsLabels = new HashSet<>();

                final double[] lbs = data.getY();

                for (double lb : lbs)
                    locClsLabels.add(lb);

                return locClsLabels;
            }, (a, b) -> {
                if (a == null)
                    return b == null ? new HashSet<>() : b;
                if (b == null)
                    return a;
                return Stream.of(a, b).flatMap(Collection::stream).collect(Collectors.toSet());
            });

            if (clsLabels != null)
                res.addAll(clsLabels);

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return res;
    }
}
