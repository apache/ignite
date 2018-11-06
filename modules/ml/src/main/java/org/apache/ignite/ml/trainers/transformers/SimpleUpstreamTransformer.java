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
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Performs transformation based on lambda which should functionally depend only on passed upstream.
 *
 * @param <K> Type of keys.
 * @param <V> Type of values.
 */
public class SimpleUpstreamTransformer<K, V> extends UpstreamTransformer<K, V, Void> {
    /** Transform to perform. */
    private IgniteFunction<Stream<UpstreamEntry<K, V>>, Stream<UpstreamEntry<K, V>>> transform;

    /**
     * Create instance of upstream transformer.
     *
     * @param transform Transform to perform.
     */
    public SimpleUpstreamTransformer(
        IgniteFunction<Stream<UpstreamEntry<K, V>>, Stream<UpstreamEntry<K, V>>> transform) {
        this.transform = transform;
    }

    /** {@inheritDoc} */
    @Override protected Void createData(Random rnd) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Stream<UpstreamEntry<K, V>> transform(Void data, Stream<UpstreamEntry<K, V>> upstream) {
        return transform.apply(upstream);
    }
}
