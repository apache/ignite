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

package org.apache.ignite.ml.dataset.impl.bootstrapping;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Builder for bootstrapped dataset. Bootstrapped dataset consist of several subsamples created in according to random
 * sampling with replacements selection of vectors from original dataset. This realization uses
 * {@link BootstrappedVector} containing each vector from original sample with counters of repetitions
 * for each subsample. As heuristic this implementation uses Poisson Distribution for generating counter values.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class BootstrappedDatasetBuilder<K,V> implements PartitionDataBuilder<K,V, EmptyContext, BootstrappedDatasetPartition> {
    /** Serial version uid. */
    private static final long serialVersionUID = 8146220902914010559L;

    /** Feature extractor. */
    private final IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Label extractor. */
    private final IgniteBiFunction<K, V, Double> lbExtractor;

    /** Samples count. */
    private final int samplesCnt;

    /** Subsample size. */
    private final double subsampleSize;

    /**
     * Creates an instance of BootstrappedDatasetBuilder.
     *
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param samplesCnt Samples count.
     * @param subsampleSize Subsample size.
     */
    public BootstrappedDatasetBuilder(IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, int samplesCnt, double subsampleSize) {

        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.samplesCnt = samplesCnt;
        this.subsampleSize = subsampleSize;
    }

    /** {@inheritDoc} */
    @Override public BootstrappedDatasetPartition build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize,
        EmptyContext ctx) {

        BootstrappedVector[] dataset = new BootstrappedVector[Math.toIntExact(upstreamDataSize)];

        int cntr = 0;
        PoissonDistribution poissonDistribution = new PoissonDistribution(subsampleSize);
        while(upstreamData.hasNext()) {
            UpstreamEntry<K, V> nextRow = upstreamData.next();
            Vector features = featureExtractor.apply(nextRow.getKey(), nextRow.getValue());
            Double lb = lbExtractor.apply(nextRow.getKey(), nextRow.getValue());
            int[] repetitionCounters = new int[samplesCnt];
            Arrays.setAll(repetitionCounters, i -> poissonDistribution.sample());
            dataset[cntr++] = new BootstrappedVector(features, lb, repetitionCounters);
        }

        return new BootstrappedDatasetPartition(dataset);
    }
}
