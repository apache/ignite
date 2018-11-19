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
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Class representing chain of transformers applied to upstream.
 *
 * @param <K> Type of upstream keys.
 * @param <V> Type of upstream values.
 */
public class UpstreamTransformerBuildersChain<K, V> implements Serializable {
    /** Seed used for transformations. */
    private Long seed;

    /** List of upstream transformations. */
    private List<UpstreamTransformerBuilder<K, V>> list;

    /**
     * Creates empty upstream transformers chain (basically identity function).
     *
     * @param <K> Type of upstream keys.
     * @param <V> Type of upstream values.
     * @return Empty upstream transformers chain.
     */
    public static <K, V> UpstreamTransformerBuildersChain<K, V> empty() {
        return new UpstreamTransformerBuildersChain<>();
    }

    /**
     * Creates upstream transformers chain consisting of one specified transformer.
     *
     * @param <K> Type of upstream keys.
     * @param <V> Type of upstream values.
     * @return Upstream transformers chain consisting of one specified transformer.
     */
    public static <K, V> UpstreamTransformerBuildersChain<K, V> of(UpstreamTransformerBuilder<K, V> trans) {
        UpstreamTransformerBuildersChain<K, V> res = new UpstreamTransformerBuildersChain<>();
        return res.addUpstreamTransformer(trans);
    }

    /**
     * Construct instance of this class.
     */
    private UpstreamTransformerBuildersChain() {
        list = new ArrayList<>();
        seed = new Random().nextLong();
    }

    /**
     * Adds upstream transformer to this chain.
     *
     * @param next Transformer to add.
     * @return This chain with added transformer.
     */
    public UpstreamTransformerBuildersChain<K, V> addUpstreamTransformer(UpstreamTransformerBuilder<K, V> next) {
        list.add(next);

        return this;
    }

    public UpstreamTransformer<K, V> build(LearningEnvironment env) {
        UpstreamTransformer<K, V> res = x -> x;

        for (UpstreamTransformerBuilder<K, V> builder : list) {
            res = res.then(builder.build(env));
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
    public UpstreamTransformerBuildersChain<K, V> setSeed(long seed) {
        this.seed = seed;

        return this;
    }

    /**
     * Modifies seed for transformations if it is present.
     *
     * @param f Modification function.
     * @return This object.
     */
    public UpstreamTransformerBuildersChain<K, V> modifySeed(IgniteFunction<Long, Long> f) {
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
