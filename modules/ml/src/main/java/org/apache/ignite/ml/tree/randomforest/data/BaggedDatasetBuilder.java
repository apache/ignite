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

package org.apache.ignite.ml.tree.randomforest.data;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class BaggedDatasetBuilder<K,V> implements PartitionDataBuilder<K,V, EmptyContext, BaggedDatasetPartition> {
    private final IgniteBiFunction<K, V, Vector> featureExtractor;
    private final IgniteBiFunction<K, V, Double> lbExtractor;
    private final int samplesCount;
    private final double subsampleSize;
    private long seed;

    public BaggedDatasetBuilder(IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor,
        int samplesCount, double subsampleSize) {

        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.samplesCount = samplesCount;
        this.subsampleSize = subsampleSize;
        this.seed = System.currentTimeMillis();
    }

    @Override public BaggedDatasetPartition build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize,
        EmptyContext ctx) {

        BaggedVector[] dataset = (BaggedVector[]) Array.newInstance(BaggedVector.class, Math.toIntExact(upstreamDataSize));
        AtomicInteger ptr = new AtomicInteger();
        PoissonDistribution poissonDistribution = new PoissonDistribution(subsampleSize);
        poissonDistribution.reseedRandomGenerator(seed);
        upstreamData.forEachRemaining(entry -> {
            Vector features = featureExtractor.apply(entry.getKey(), entry.getValue());
            Double label = lbExtractor.apply(entry.getKey(), entry.getValue());
            int[] repetitionCounters = new int[samplesCount];
            Arrays.setAll(repetitionCounters, i -> poissonDistribution.sample());
            dataset[ptr.getAndIncrement()] = new BaggedVector(features, label, repetitionCounters);
        });

        return new BaggedDatasetPartition(dataset);
    }

    public BaggedDatasetBuilder<K,V> withSeed(long seed) {
        this.seed = seed;
        return this;
    }
}
