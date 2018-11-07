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
import java.util.Random;
import java.util.stream.Stream;

/**
 * Class encapsulating transformer of upstream.
 *
 * @param <K> Type of keys in the upstream.
 * @param <V> Type of values in the upstream.
 * @param <T> Data needed for this transformer.
 */
public abstract class UpstreamTransformer<K, V, T> implements Serializable {
    /**
     * Performs transformation.
     *
     * @param rnd RNG.
     * @param upstream Upstream.
     * @return Transformed upstream.
     */
    public final Stream<UpstreamEntry<K, V>> transform(Random rnd, Stream<UpstreamEntry<K, V>> upstream) {
        return transform(createData(rnd), upstream);
    }

    /**
     * Creates data needed for this transformer based (if needed) on given RNG.
     *
     * @param rnd RNG.
     * @return Data needed for this transformer based (if needed) on given RNG.
     */
    protected abstract T createData(Random rnd);

    /**
     * Perform transformation of upstream.
     *
     * @param data Data needed for transformation.
     * @param upstream Upstream.
     * @return Transformed upstream.
     */
    protected abstract Stream<UpstreamEntry<K, V>> transform(T data, Stream<UpstreamEntry<K, V>> upstream);
}
