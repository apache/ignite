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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Class representing chain of transformers applied to upstream.
 *
 * @param <K> Type of upstream keys.
 * @param <V> Type of upstream values.
 */
public class UpstreamTransformerChain<K, V> implements Serializable {
    /** Seed used for transformations. */
    private Long seed;

    /** List of upstream transformations. */
    private List<UpstreamTransformer<K, V>> list;

    /**
     * Creates empty upstream transformers chain (basically identity function).
     *
     * @param <K> Type of upstream keys.
     * @param <V> Type of upstream values.
     * @return Empty upstream transformers chain.
     */
    public static <K, V> UpstreamTransformerChain<K, V> empty() {
        return new UpstreamTransformerChain<>();
    }

    /**
     * Creates upstream transformers chain consisting of one specified transformer.
     *
     * @param <K> Type of upstream keys.
     * @param <V> Type of upstream values.
     * @return Upstream transformers chain consisting of one specified transformer.
     */
    public static <K, V> UpstreamTransformerChain<K, V> of(UpstreamTransformer<K, V> trans) {
        UpstreamTransformerChain<K, V> res = new UpstreamTransformerChain<>();
        return res.addUpstreamTransformer(trans);
    }

    /**
     * Construct instance of this class.
     */
    private UpstreamTransformerChain() {
        list = new ArrayList<>();
        seed = new Random().nextLong();
    }

    /**
     * Adds upstream transformer to this chain.
     *
     * @param next Transformer to add.
     * @return This chain with added transformer.
     */
    public UpstreamTransformerChain<K, V> addUpstreamTransformer(UpstreamTransformer<K, V> next) {
        list.add(next);

        return this;
    }

    /**
     * Add upstream transformer based on given lambda.
     *
     * @param transformer Transformer.
     * @return This object.
     */
    public UpstreamTransformerChain<K, V> addUpstreamTransformer(IgniteFunction<Stream<UpstreamEntry<K, V>>,
        Stream<UpstreamEntry<K, V>>> transformer) {
        return addUpstreamTransformer((rnd, upstream) -> transformer.apply(upstream));
    }

    /**
     * Performs stream transformation using RNG based on provided seed as pseudo-randomness source for all
     * transformers in the chain.
     *
     * @param upstream Upstream.
     * @return Transformed upstream.
     */
    public Stream<UpstreamEntry<K, V>> transform(Stream<UpstreamEntry<K, V>> upstream) {
        Random rnd = new Random(seed);

        Stream<UpstreamEntry<K, V>> res = upstream;

        for (UpstreamTransformer<K, V> kvUpstreamTransformer : list) {
            res = kvUpstreamTransformer.transform(rnd, res);
        }

        return res;
    }

    /**
     * Checks if this chain is empty.
     *
     * @return Result of check if this chain is empty.
     */
    public boolean isEmpty() {
        return list.isEmpty();
    }

    /**
     * Set seed for transformations.
     *
     * @param seed Seed.
     * @return This object.
     */
    public UpstreamTransformerChain<K, V> setSeed(long seed) {
        this.seed = seed;

        return this;
    }

    /**
     * Modifies seed for transformations if it is present.
     *
     * @param f Modification function.
     * @return This object.
     */
    public UpstreamTransformerChain<K, V> modifySeed(IgniteFunction<Long, Long> f) {
        seed = f.apply(seed);

        return this;
    }

    /**
     * Get seed used for RNG in transformations.
     *
     * @return Seed used for RNG in transformations.
     */
    public Long seed() {
        return seed;
    }
}
