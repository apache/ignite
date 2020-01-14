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

package org.apache.ignite.ml.util.generators;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.DoubleStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducer;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link DataStreamGenerator} cache filling.
 */
public class DataStreamGeneratorFillCacheTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testCacheFilling() {
        IgniteConfiguration configuration = new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Arrays.asList("127.0.0.1:47500..47509"))));

        String cacheName = "TEST_CACHE";
        CacheConfiguration<UUID, LabeledVector<Double>> cacheConfiguration =
            new CacheConfiguration<UUID, LabeledVector<Double>>(cacheName)
                .setAffinity(new RendezvousAffinityFunction(false, 10));
        int datasetSize = 5000;

        try (Ignite ignite = Ignition.start(configuration)) {
            IgniteCache<UUID, LabeledVector<Double>> cache = ignite.getOrCreateCache(cacheConfiguration);
            DataStreamGenerator generator = new GaussRandomProducer(0).vectorize(1).asDataStream();
            generator.fillCacheWithVecUUIDAsKey(datasetSize, cache);

            LabeledDummyVectorizer<UUID, Double> vectorizer = new LabeledDummyVectorizer<>();
            CacheBasedDatasetBuilder<UUID, LabeledVector<Double>> datasetBuilder = new CacheBasedDatasetBuilder<>(ignite, cache);

            IgniteFunction<SimpleDatasetData, StatPair> map = data ->
                new StatPair(DoubleStream.of(data.getFeatures()).sum(), data.getRows());
            LearningEnvironment env = LearningEnvironmentBuilder.defaultBuilder().buildForTrainer();
            env.deployingContext().initByClientObject(map);

            try (CacheBasedDataset<UUID, LabeledVector<Double>, EmptyContext, SimpleDatasetData> dataset =
                     datasetBuilder.build(
                         LearningEnvironmentBuilder.defaultBuilder(),
                         new EmptyContextBuilder<>(),
                         new SimpleDatasetDataBuilder<>(vectorizer),
                         env
                     )) {

                StatPair res = dataset.compute(map, StatPair::sum);
                assertEquals(datasetSize, res.cntOfRows);
                assertEquals(0.0, res.elementsSum / res.cntOfRows, 1e-2);
            }

            ignite.destroyCache(cacheName);
        }
    }

    /** */
    static class StatPair {
        /** */
        private double elementsSum;

        /** */
        private int cntOfRows;

        /** */
        public StatPair(double elementsSum, int cntOfRows) {
            this.elementsSum = elementsSum;
            this.cntOfRows = cntOfRows;
        }

        /** */
        static StatPair sum(StatPair left, StatPair right) {
            if (left == null && right == null)
                return new StatPair(0, 0);
            else if (left == null)
                return right;
            else if (right == null)
                return left;
            else
                return new StatPair(
                    right.elementsSum + left.elementsSum,
                    right.cntOfRows + left.cntOfRows
                );
        }
    }
}
