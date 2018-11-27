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

package org.apache.ignite.ml.trainers.transformers;

import java.util.Random;
import java.util.stream.Stream;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;

/**
 * This class encapsulates the logic needed to do bagging (bootstrap aggregating) by features.
 * The action of this class on a given upstream is to replicate each entry in accordance to
 * Poisson distribution.
 *
 * @param <K> Type of upstream keys.
 * @param <V> Type of upstream values.
 */
public class BaggingUpstreamTransformer<K, V> implements UpstreamTransformer<K, V> {
    /** Ratio of subsample to entire upstream size */
    private double subsampleRatio;

    /**
     * Construct instance of this transformer with a given subsample ratio.
     *
     * @param subsampleRatio Subsample ratio.
     */
    public BaggingUpstreamTransformer(double subsampleRatio) {
        this.subsampleRatio = subsampleRatio;
    }

    /** {@inheritDoc} */
    @Override public Stream<UpstreamEntry<K, V>> transform(Random rnd, Stream<UpstreamEntry<K, V>> upstream) {
        PoissonDistribution poisson = new PoissonDistribution(
            new Well19937c(rnd.nextLong()),
            subsampleRatio,
            PoissonDistribution.DEFAULT_EPSILON,
            PoissonDistribution.DEFAULT_MAX_ITERATIONS);

        return upstream.sequential().flatMap(en -> Stream.generate(() -> en).limit(poisson.sample()));
    }
}
