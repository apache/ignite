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

package org.apache.ignite.ml.dataset.impl.bootstrapping;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

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

    /** Mapper of upstream entries into {@link LabeledVector}. */
    private final FeatureLabelExtractor<K, V, Double> extractor;

    /** Samples count. */
    private final int samplesCnt;

    /** Subsample size. */
    private final double subsampleSize;

    /**
     * Creates an instance of BootstrappedDatasetBuilder.
     *
     * @param extractor Mapper of upstream entries into {@link LabeledVector}.
     * @param samplesCnt Samples count.
     * @param subsampleSize Subsample size.
     */
    public BootstrappedDatasetBuilder(FeatureLabelExtractor<K, V, Double> extractor,
        int samplesCnt,
        double subsampleSize) {

        this.extractor = extractor;
        this.samplesCnt = samplesCnt;
        this.subsampleSize = subsampleSize;
    }

    /** {@inheritDoc} */
    @Override public BootstrappedDatasetPartition build(
        LearningEnvironment env,
        Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize,
        EmptyContext ctx) {

        BootstrappedVector[] dataset = new BootstrappedVector[Math.toIntExact(upstreamDataSize)];

        int cntr = 0;

        PoissonDistribution poissonDistribution = new PoissonDistribution(
            new Well19937c(env.randomNumbersGenerator().nextLong()),
            subsampleSize,
            PoissonDistribution.DEFAULT_EPSILON,
            PoissonDistribution.DEFAULT_MAX_ITERATIONS);

        while(upstreamData.hasNext()) {
            UpstreamEntry<K, V> nextRow = upstreamData.next();
            LabeledVector<Double> vecAndLb = extractor.extract(nextRow.getKey(), nextRow.getValue());
            Vector features = vecAndLb.features();
            Double lb = vecAndLb.label();
            int[] repetitionCounters = new int[samplesCnt];
            Arrays.setAll(repetitionCounters, i -> poissonDistribution.sample());
            dataset[cntr++] = new BootstrappedVector(features, lb, repetitionCounters);
        }

        return new BootstrappedDatasetPartition(dataset);
    }
}
