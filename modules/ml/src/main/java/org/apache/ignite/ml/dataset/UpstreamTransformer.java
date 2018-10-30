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

import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;

import java.util.Random;
import java.util.stream.Stream;

public class UpstreamTransformer<K, V, T> {
    private IgniteFunction<Random, T> dataCreator;
    private IgniteBiFunction<Stream<UpstreamEntry<K, V>>, T, Stream<UpstreamEntry<K, V>>> transformer;
    private UpstreamTransformer<K, V, ?> next;

    public UpstreamTransformer(
        IgniteFunction<Random, T> dataCreator,
        IgniteBiFunction<Stream<UpstreamEntry<K, V>>, T, Stream<UpstreamEntry<K, V>>> transformer,
        UpstreamTransformer<K, V, ?> next) {
        this.dataCreator = dataCreator;
        this.transformer = transformer;
        this.next = next;
    }

    public void setNext(UpstreamTransformer<K, V, ?> next) {
        this.next = next;
    }

    public Stream<UpstreamEntry<K, V>> transform(Random rnd, Stream<UpstreamEntry<K, V>> stream) {
        Stream<UpstreamEntry<K, V>> res = transformer.apply(stream, dataCreator.apply(rnd));

        if (next != null) {
            res = next.transform(rnd, res);
        }

        return res;
    }
}
