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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Class representing chain of transformers applied to upstream.
 *
 * @param <K> Type of upstream keys.
 * @param <V> Type of upstream values.
 */
public class UpstreamTransformerChain<K, V> {
    private List<UpstreamTransformer<K, V, ?>> list;

    /**
     * Creates empty upstream transformers chain.
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> UpstreamTransformerChain<K, V> empty() {
        return new UpstreamTransformerChain<>();
    }

    public static <K, V> UpstreamTransformerChain<K, V> of(UpstreamTransformer<K, V, ?> trans) {
        UpstreamTransformerChain<K, V> res = new UpstreamTransformerChain<>();
        return res.addUpstreamTransformer(trans);
    }

    private UpstreamTransformerChain() {
        list = new ArrayList<>();
    }

    /**
     * Adds upstream transformer to this chain.
     *
     * @param next Transformer to add.
     * @param <T> Type of data neede by this transformer.
     * @return This chain with added transformer.
     */
    public <T> UpstreamTransformerChain<K, V> addUpstreamTransformer(UpstreamTransformer<K, V, T> next) {
        list.add(next);

        return this;
    }

    /**
     * Performs stream transformation using RNG based on provided seed as pseudo-randomness source for all
     * transformers in the chain.
     *
     * @param seed Seed for RNG.
     * @param upstream Upstream.
     * @return Transformed upstream.
     */
    public Stream<UpstreamEntry<K, V>> transform(long seed, Stream<UpstreamEntry<K, V>> upstream) {
        Random rnd = new Random(seed);

        Stream<UpstreamEntry<K, V>> res = upstream;

        for (UpstreamTransformer<K, V, ?> kvUpstreamTransformer : list) {
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
}
