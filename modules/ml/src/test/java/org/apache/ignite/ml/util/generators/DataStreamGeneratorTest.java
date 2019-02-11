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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataStreamGenerator}.
 */
public class DataStreamGeneratorTest {
    /** */
    @Test
    public void testUnlabeled() {
        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> new LabeledVector<>(VectorUtils.of(1., 2.), 100.));
            }
        };

        generator.unlabeled().limit(100).forEach(v -> {
            assertArrayEquals(new double[] {1., 2.}, v.asArray(), 1e-7);
        });
    }

    /** */
    @Test
    public void testLabeled() {
        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> new LabeledVector<>(VectorUtils.of(1., 2.), 100.));
            }
        };

        generator.labeled(v -> -100.).limit(100).forEach(v -> {
            assertArrayEquals(new double[] {1., 2.}, v.features().asArray(), 1e-7);
            assertEquals(-100., v.label(), 1e-7);
        });
    }

    /** */
    @Test
    public void testMapVectors() {
        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> new LabeledVector<>(VectorUtils.of(1., 2.), 100.));
            }
        };

        generator.mapVectors(v -> VectorUtils.of(2., 1.)).labeled().limit(100).forEach(v -> {
            assertArrayEquals(new double[] {2., 1.}, v.features().asArray(), 1e-7);
            assertEquals(100., v.label(), 1e-7);
        });
    }

    /** */
    @Test
    public void testBlur() {
        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> new LabeledVector<>(VectorUtils.of(1., 2.), 100.));
            }
        };

        generator.blur(() -> 1.).labeled().limit(100).forEach(v -> {
            assertArrayEquals(new double[] {2., 3.}, v.features().asArray(), 1e-7);
            assertEquals(100., v.label(), 1e-7);
        });
    }

    /** */
    @Test
    public void testAsMap() {
        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> new LabeledVector<>(VectorUtils.of(1., 2.), 100.));
            }
        };

        int N = 100;
        Map<Vector, Double> dataset = generator.asMap(N);
        assertEquals(N, dataset.size());
        dataset.forEach(((vector, label) -> {
            assertArrayEquals(new double[] {1., 2.}, vector.asArray(), 1e-7);
            assertEquals(100., label, 1e-7);
        }));
    }

    /** */
    @Test
    public void testAsDatasetBuilder() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> {
                    int value = counter.getAndIncrement();
                    return new LabeledVector<>(VectorUtils.of(value), (double)value % 2);
                });
            }
        };

        int N = 100;
        counter.set(0);
        DatasetBuilder<Vector, Double> b1 = generator.asDatasetBuilder(N, 2);
        counter.set(0);
        DatasetBuilder<Vector, Double> b2 = generator.asDatasetBuilder(N, (v, l) -> l == 0, 2);
        counter.set(0);
        DatasetBuilder<Vector, Double> b3 = generator.asDatasetBuilder(N, (v, l) -> l == 1, 2,
            new UpstreamTransformerBuilder() {
                @Override public UpstreamTransformer build(LearningEnvironment env) {
                    return new UpstreamTransformerForTest();
                }
            });

        checkDataset(N, b1, v -> (Double)v.label() == 0 || (Double)v.label() == 1);
        checkDataset(N / 2, b2, v -> (Double)v.label() == 0);
        checkDataset(N / 2, b3, v -> (Double)v.label() < 0);
    }

    /** */
    @Test
    public void testCacheFilling() {
        IgniteConfiguration configuration = new IgniteConfiguration().setPeerClassLoadingEnabled(true);
        String cacheName = "TEST_CACHE";
        CacheConfiguration<UUID, LabeledVector<Double>> cacheConfiguration =
            new CacheConfiguration<UUID, LabeledVector<Double>>(cacheName)
                .setAffinity(new RendezvousAffinityFunction(false, 10));
        int datasetSize = 5000;

        try (Ignite ignite = Ignition.start(configuration)) {
            IgniteCache<UUID, LabeledVector<Double>> cache = ignite.getOrCreateCache(cacheConfiguration);
            DataStreamGenerator generator = new GaussRandomProducer(0).vectorize(1).asDataStream();
            generator.fillCacheWithVecUUIDAsKey(datasetSize, cache);

            CacheBasedDatasetBuilder<UUID, LabeledVector<Double>> datasetBuilder = new CacheBasedDatasetBuilder<>(ignite, cache);
            try (CacheBasedDataset<UUID, LabeledVector<Double>, EmptyContext, SimpleDatasetData> dataset =
                     datasetBuilder.build(LearningEnvironmentBuilder.defaultBuilder(),
                         new EmptyContextBuilder<>(), new SimpleDatasetDataBuilder<>((k, v) -> v.features()))) {

                StatPair result = dataset.compute(
                    data -> new StatPair(DoubleStream.of(data.getFeatures()).sum(), data.getRows()),
                    StatPair::sum
                );

                assertEquals(datasetSize, result.countOfRows);
                assertEquals(0.0, result.elementsSum / result.countOfRows, 1e-2);
            }

            ignite.destroyCache(cacheName);
        }
    }

    /** */
    private void checkDataset(int sampleSize, DatasetBuilder<Vector, Double> datasetBuilder,
        Predicate<LabeledVector> labelCheck) throws Exception {

        try (Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset = buildDataset(datasetBuilder)) {
            List<LabeledVector> res = dataset.compute(this::map, this::reduce);
            assertEquals(sampleSize, res.size());

            res.forEach(v -> assertTrue(labelCheck.test(v)));
        }
    }

    /** */
    private Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> buildDataset(
        DatasetBuilder<Vector, Double> b1) {
        return b1.build(LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new LabeledDatasetPartitionDataBuilderOnHeap<>((v, l) -> v, (v, l) -> l)
        );
    }

    /** */
    private List<LabeledVector> map(LabeledVectorSet<Double, LabeledVector> d) {
        return IntStream.range(0, d.rowSize()).mapToObj(d::getRow).collect(Collectors.toList());
    }

    /** */
    private List<LabeledVector> reduce(List<LabeledVector> l, List<LabeledVector> r) {
        if (l == null) {
            if (r == null)
                return Collections.emptyList();
            else
                return r;
        }
        else {
            List<LabeledVector> res = new ArrayList<>();
            res.addAll(l);
            res.addAll(r);
            return res;
        }
    }

    /** */
    private static class UpstreamTransformerForTest implements UpstreamTransformer {
        @Override public Stream<UpstreamEntry> transform(
            Stream<UpstreamEntry> upstream) {
            return upstream.map(entry -> new UpstreamEntry<>(entry.getKey(), -((double)entry.getValue())));
        }
    }

    /** */
    static class StatPair {
        /** */
        private double elementsSum;

        /** */
        private int countOfRows;

        /** */
        public StatPair(double elementsSum, int countOfRows) {
            this.elementsSum = elementsSum;
            this.countOfRows = countOfRows;
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
                    right.countOfRows + left.countOfRows
                );
        }
    }
}
