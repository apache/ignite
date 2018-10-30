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

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;

import java.util.Random;
import java.util.stream.Stream;

public class BaggingUpstreamTransformer<K, V> extends UpstreamTransformer<K, V, PoissonDistribution> {
    private double subsampleRatio;

    public BaggingUpstreamTransformer(double subsampleRatio) {
        this.subsampleRatio = subsampleRatio;
    }

    @Override public PoissonDistribution createData(Random rnd) {
        return new PoissonDistribution(
            new Well19937c(rnd.nextLong()),
            subsampleRatio,
            PoissonDistribution.DEFAULT_EPSILON,
            PoissonDistribution.DEFAULT_MAX_ITERATIONS);
    }

    @Override protected Stream<UpstreamEntry<K, V>> transform(PoissonDistribution poisson, Stream<UpstreamEntry<K, V>> upstream) {
        // Sequentiality of stream here is needed because we use instance of
        // RNG as data, to make it deterministic we should fix order.
        // TODO: Maybe more efficient way to make stream transformation deterministic would be to
        // use mapping of the form (entryIdx, en) -> Stream.generate(() -> en).limit(new PoissonDistribution(Well19937c(entryIdx + seed), ...).sample())
        return upstream.sequential().flatMap(en -> Stream.generate(() -> en).limit(poisson.sample()));
    }
}
