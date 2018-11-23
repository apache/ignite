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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.environment.LearningEnvironment;

/**
 * Class representing chain of transformers applied to upstream.
 *
 * @param <K> Type of upstream keys.
 * @param <V> Type of upstream values.
 */
public class UpstreamTransformerBuildersChain<K, V> implements Serializable {
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
        return new UpstreamTransformerBuildersChain<>(
            new ArrayList<>(Collections.singletonList(trans)));
    }

    /**
     * Construct instance of this class.
     */
    private UpstreamTransformerBuildersChain() {
        list = new ArrayList<>();
    }

    /**
     * Constructs instance of this class from the specified list of {@link UpstreamTransformerBuilder}.
     *
     * @param list
     */
    private UpstreamTransformerBuildersChain(List<UpstreamTransformerBuilder<K, V>> list) {
        this.list = list;
    }

    /**
     * Returns new instance of this class with added upstream transformer builder.
     *
     * @param next Upstream transformer builder to add.
     * @return This chain with added transformer.
     */
    public UpstreamTransformerBuildersChain<K, V> withAddedUpstreamTransformer(UpstreamTransformerBuilder<K, V> next) {
        ArrayList<UpstreamTransformerBuilder<K, V>> newList = new ArrayList<>(list);
        newList.add(next);

        return new UpstreamTransformerBuildersChain<>(newList);
    }

    /**
     * Build upstream transformer from learning environment.
     *
     * @param env Learning environment.
     * @return Upstream transformer.
     */
    public UpstreamTransformer<K, V> build(LearningEnvironment env) {
        UpstreamTransformer<K, V> res = x -> x;

        for (UpstreamTransformerBuilder<K, V> builder : list)
            res = res.then(builder.build(env));

        return res;
    }

    /**
     * Checks if transformation which is built from environment will be trivial (identity transformation).
     *
     * @return Result of check if this chain produces trivial upstream transformer.
     */
    public boolean isTrivial() {
        return list.isEmpty();
    }
}
