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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
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

        generator.unlabeled().limit(100).forEach(v -> assertArrayEquals(new double[] {1., 2.}, v.asArray(), 1e-7));
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
        AtomicInteger cntr = new AtomicInteger();

        DataStreamGenerator generator = new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(() -> {
                    int val = cntr.getAndIncrement();
                    return new LabeledVector<>(VectorUtils.of(val), (double)val % 2);
                });
            }
        };

        int N = 100;
        cntr.set(0);
        DatasetBuilder<Vector, Double> b1 = generator.asDatasetBuilder(N, 2);
        cntr.set(0);
        DatasetBuilder<Vector, Double> b2 = generator.asDatasetBuilder(N, (v, l) -> l == 0, 2);
        cntr.set(0);
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
    private void checkDataset(int sampleSize, DatasetBuilder<Vector, Double> datasetBuilder,
        Predicate<LabeledVector> lbCheck) throws Exception {

        try (Dataset<EmptyContext, LabeledVectorSet<LabeledVector>> dataset = buildDataset(datasetBuilder)) {
            List<LabeledVector> res = dataset.compute(this::map, this::reduce);
            assertEquals(sampleSize, res.size());

            res.forEach(v -> assertTrue(lbCheck.test(v)));
        }
    }

    /** */
    private Dataset<EmptyContext, LabeledVectorSet<LabeledVector>> buildDataset(
        DatasetBuilder<Vector, Double> b1) {
        return b1.build(LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new LabeledDatasetPartitionDataBuilderOnHeap<>((Preprocessor<Vector, Double>)LabeledVector::new),
            LearningEnvironmentBuilder.defaultBuilder().buildForTrainer()
        );
    }

    /** */
    private List<LabeledVector> map(LabeledVectorSet<LabeledVector> d) {
        return IntStream.range(0, d.rowSize()).mapToObj(d::getRow).collect(Collectors.toList());
    }

    /** */
    private List<LabeledVector> reduce(List<LabeledVector> l, List<LabeledVector> r) {
        if (l == null)
            return r == null ? Collections.emptyList() : r;
        else {
            List<LabeledVector> res = new ArrayList<>();
            res.addAll(l);
            res.addAll(r);
            return res;
        }
    }

    /** */
    private static class UpstreamTransformerForTest implements UpstreamTransformer {
        /** {@inheritDoc} */
        @Override public Stream<UpstreamEntry> transform(
            Stream<UpstreamEntry> upstream) {
            return upstream.map(entry -> new UpstreamEntry<>(entry.getKey(), -((double)entry.getValue())));
        }
    }
}
