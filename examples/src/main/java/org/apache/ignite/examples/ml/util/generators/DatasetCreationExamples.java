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

package org.apache.ignite.examples.ml.util.generators;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.extractor.impl.FeatureLabelExtractorWrapper;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;

import java.util.UUID;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Examples of using {@link DataStreamGenerator} methods for filling cache or creating local datasets.
 */
public class DatasetCreationExamples {
    /**
     * Size of dataset.
     */
    private static final int DATASET_SIZE = 1000;

    /**
     * Runs example.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) throws Exception {
        // Creates a simple generator based on 1-dimension vector uniformly distributed between [-1, 1] having mean = 0.0 .
        DataStreamGenerator generator = new UniformRandomProducer(-1, 1., 0)
            .vectorize(1)
            .asDataStream();

        // DataStreamGenerator can be represented as stream of (un-)labeled vectors.
        double expMean = computeMean(generator.unlabeled());
        // DataStreamGenerator can fill map of vectors as keys and labels as values.
        double meanFromMap = computeMean(generator.asMap(DATASET_SIZE).keySet().stream());

        // DataStreamGenerator can prepare local DatasetBuilder with data from gemerator.
        double meanFromLocDataset;
        try (Dataset<EmptyContext, SimpleDatasetData> dataset = generator.asDatasetBuilder(DATASET_SIZE, 10)
            .build(LearningEnvironmentBuilder.defaultBuilder(), new EmptyContextBuilder<>(),
                new SimpleDatasetDataBuilder<>(FeatureLabelExtractorWrapper.wrap((k, v) -> k)))) {

            meanFromLocDataset = dataset.compute(
                data -> DoubleStream.of(data.getFeatures()).sum(),
                (l, r) -> asPrimitive(l) + asPrimitive(r)
            );

            meanFromLocDataset /= DATASET_SIZE;
        }

        double meanFromCache;
        IgniteConfiguration configuration = new IgniteConfiguration().setPeerClassLoadingEnabled(true);
        try (Ignite ignite = Ignition.start(configuration)) {
            String cacheName = "TEST_CACHE";
            IgniteCache<UUID, LabeledVector<Double>> withCustomKeyCache = null;
            try {
                withCustomKeyCache = ignite.getOrCreateCache(
                    new CacheConfiguration<UUID, LabeledVector<Double>>(cacheName)
                        .setAffinity(new RendezvousAffinityFunction(false, 10))
                );

                // DataStreamGenerator can fill cache with vectors as values and HashCodes/random UUID/custom keys.
                generator.fillCacheWithVecUUIDAsKey(DATASET_SIZE, withCustomKeyCache);
                meanFromCache = computeMean(ignite, withCustomKeyCache);
            } finally {
                ignite.destroyCache(cacheName);
            }
        }

        // Results should be near to expected value.
        System.out.println(String.format("Expected mean from stream: %.2f", expMean));
        System.out.println(String.format("Mean from map built from stream: %.2f", meanFromMap));
        System.out.println(String.format("Mean from local dataset filled by data stream: %.2f", meanFromLocDataset));
        System.out.println(String.format("Mean from cache filled by data stream: %.2f", meanFromCache));
    }

    /**
     * Converts null values from Double to 0.
     *
     * @param val Value.
     * @return Double converted to primitive type.
     */
    private static double asPrimitive(Double val) {
        return val == null ? 0.0 : val;
    }

    /**
     * Compute mean from stream with size = DATASET_SIZE.
     *
     * @param vectors Stream of 1-dimension vectors.
     * @return mean value.
     */
    private static double computeMean(Stream<Vector> vectors) {
        return vectors.mapToDouble(x -> x.get(0))
            .limit(DATASET_SIZE)
            .sum() / DATASET_SIZE;
    }

    /**
     * Computes mean value for dataset in cache.
     *
     * @param ignite Ignite.
     * @param cache Cache.
     * @return mean value computed on cache.
     */
    private static double computeMean(Ignite ignite, IgniteCache<UUID, LabeledVector<Double>> cache) {
        double result;
        CacheBasedDatasetBuilder<UUID, LabeledVector<Double>> builder = new CacheBasedDatasetBuilder<>(ignite, cache);
        try (CacheBasedDataset<UUID, LabeledVector<Double>, EmptyContext, SimpleDatasetData> dataset =
                 builder.build(LearningEnvironmentBuilder.defaultBuilder(),
                     new EmptyContextBuilder<>(),
                     new SimpleDatasetDataBuilder<>(FeatureLabelExtractorWrapper.wrap((k, v) -> v.features())))) {

            result = dataset.compute(
                data -> DoubleStream.of(data.getFeatures()).sum(),
                (l, r) -> asPrimitive(l) + asPrimitive(r)
            );

            result /= DATASET_SIZE;
        }

        return result;
    }
}
