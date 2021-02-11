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

package org.apache.ignite.ml.pipeline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * A simple pipeline, which acts as a global trainer which produce a Pipeline Model.
 * A Pipeline consists of a sequence of stages, each of which is either a Preprocessing Stage or a Trainer.
 * When {@code fit()} method is called, the stages are executed in order.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class Pipeline<K, V, C extends Serializable, L> implements Serializable {
    /** Final Feature extractor. */
    private Preprocessor<K, V> finalPreprocessor;

    /** Vectorizer. */
    private Vectorizer<K, V, C, L> vectorizer;

    /** Preprocessor stages. */
    private List<PreprocessingTrainer> preprocessingTrainers = new ArrayList<>();

    /** Final trainer stage. */
    private DatasetTrainer finalStage;

    /** Learning environment builder. */
    private LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /**
     * @param vectorizer Vectorizer.
     */
    public Pipeline<K, V, C, L> addVectorizer(Vectorizer<K, V, C, L> vectorizer) {
        this.vectorizer = vectorizer;
        return this;
    }

    /**
     * Adds a preprocessor.
     *
     * @param preprocessingTrainer The parameter value.
     * @return The updated Pipeline.
     */
    public Pipeline<K, V, C, L> addPreprocessingTrainer(PreprocessingTrainer preprocessingTrainer) {
        preprocessingTrainers.add(preprocessingTrainer);
        return this;
    }

    /**
     * Adds a trainer.
     *
     * @param trainer The parameter value.
     * @return The updated Pipeline.
     */
    public Pipeline<K, V, C, L> addTrainer(DatasetTrainer trainer) {
        this.finalStage = trainer;
        return this;
    }

    /**
     * Returns trainer.
     */
    public DatasetTrainer getTrainer() {
        return finalStage;
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
     * Set learning environment builder.
     *
     * @param envBuilder Learning environment builder.
     */
    public void setEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        this.envBuilder = envBuilder;
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
    public PipelineMdl<K, V> fit(DatasetBuilder datasetBuilder) {
        if (finalStage == null)
            throw new IllegalStateException("The Pipeline should be finished with the Training Stage.");

        // Reload for new fit
        finalPreprocessor = vectorizer;

        preprocessingTrainers.forEach(e -> {
            finalPreprocessor = e.fit(
                envBuilder,
                datasetBuilder,
                finalPreprocessor
            );
        });

        LearningEnvironment env = LearningEnvironmentBuilder.defaultBuilder().buildForTrainer();
        env.initDeployingContext(finalPreprocessor);
        IgniteModel<Vector, Double> internalMdl = finalStage
            .fit(
                datasetBuilder,
                finalPreprocessor,
                env
            );

        return new PipelineMdl<K, V>()
            .withPreprocessor(finalPreprocessor)
            .withInternalMdl(internalMdl);
    }

    /**
     * Returns the final preprocessor for evaluation needs.
     */
    public Preprocessor<K, V> getFinalPreprocessor() {
        return finalPreprocessor;
    }
}
