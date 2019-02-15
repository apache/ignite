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

package org.apache.ignite.ml.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * A simple pipeline, which acts as a global trainer which produce a Pipeline Model.
 * A Pipeline consists of a sequence of stages, each of which is either a Preprocessing Stage or a Trainer.
 * When {@code fit()} method is called, the stages are executed in order.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 * @param <R> Type of a result in {@code upstream} feature extractor.
 */
public class Pipeline<K, V, R> {
    /** Feature extractor. */
    private IgniteBiFunction<K, V, R> finalFeatureExtractor;

    /** Label extractor. */
    private IgniteBiFunction<K, V, Double> lbExtractor;

    /** Prerpocessor stages. */
    private List<PreprocessingTrainer> preprocessors = new ArrayList<>();

    /** Final trainer stage. */
    private DatasetTrainer finalStage;

    /**
     * Adds feature extractor as a zero stage.
     *
     * @param featureExtractor The parameter value.
     * @return The updated Pipeline.
     */
    public Pipeline<K, V, R> addFeatureExtractor(IgniteBiFunction<K, V, R> featureExtractor) {
        this.finalFeatureExtractor = featureExtractor;
        return this;
    }

    /**
     * Adds a label extractor for the produced model.
     *
     * @param lbExtractor The parameter value.
     * @return The updated Pipeline.
     */
    public Pipeline<K, V, R> addLabelExtractor(IgniteBiFunction<K, V, Double> lbExtractor) {
        this.lbExtractor = lbExtractor;
        return this;
    }

    /**
     * Adds a preprocessor.
     *
     * @param preprocessor The parameter value.
     * @return The updated Pipeline.
     */
    public Pipeline<K, V, R> addPreprocessor(PreprocessingTrainer preprocessor) {
        preprocessors.add(preprocessor);
        return this;
    }

    /**
     * Adds a trainer.
     *
     * @param trainer The parameter value.
     * @return The updated Pipeline.
     */
    public Pipeline<K, V, R> addTrainer(DatasetTrainer trainer) {
        this.finalStage = trainer;
        return this;
    }

    /**
     * Fits the pipeline to the input cache.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache with {@code upstream} data.
     * @return The fitted model based on chain of preprocessors and final trainer.
     */
    public PipelineMdl<K, V> fit(Ignite ignite, IgniteCache<K, V> cache) {
        DatasetBuilder datasetBuilder = new CacheBasedDatasetBuilder<>(ignite, cache);
        return fit(datasetBuilder);
    }

    /**
     * Fits the pipeline to the input mock data.
     *
     * @param data Data.
     * @param parts Number of partitions.
     * @return The fitted model based on chain of preprocessors and final trainer.
     */
    public PipelineMdl<K, V> fit(Map<K, V> data, int parts) {
        DatasetBuilder datasetBuilder = new LocalDatasetBuilder<>(data, parts);
        return fit(datasetBuilder);
    }

    /** Fits the pipeline to the input dataset builder. */
    private PipelineMdl<K, V> fit(DatasetBuilder datasetBuilder) {
        assert lbExtractor != null;
        assert finalFeatureExtractor != null;

        if (finalStage == null)
            throw new IllegalStateException("The Pipeline should be finished with the Training Stage.");

        preprocessors.forEach(e -> {

            finalFeatureExtractor = e.fit(
                datasetBuilder,
                finalFeatureExtractor
            );
        });

        Model<Vector, Double> internalMdl = finalStage
            .fit(
                datasetBuilder,
                finalFeatureExtractor,
                lbExtractor
            );

        return new PipelineMdl<K, V>()
            .withFeatureExtractor(finalFeatureExtractor)
            .withLabelExtractor(lbExtractor)
            .withInternalMdl(internalMdl);
    }
}
