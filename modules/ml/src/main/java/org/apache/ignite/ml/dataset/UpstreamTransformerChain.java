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

public class UpstreamTransformerChain<K, V> {
    private UpstreamTransformer<K, V, ?> head;
    private UpstreamTransformer<K, V, ?> tail;

    public UpstreamTransformerChain(UpstreamTransformer<K, V, ?> head) {
        this.head = head;
        this.tail = head;
    }

    public <T> UpstreamTransformerChain<K, V> addUpstreamTransformer(
        IgniteBiFunction<Stream<UpstreamEntry<K, V>>, T, Stream<UpstreamEntry<K, V>>> trans,
        IgniteFunction<Random, T> dataCreator
    ) {
        UpstreamTransformer<K, V, T> next = new UpstreamTransformer<>(dataCreator, trans, null);
        tail.setNext(next);
        tail = next;

        return this;
    }

    public Stream<UpstreamEntry<K, V>> transform(long seed, Stream<UpstreamEntry<K, V>> stream) {
        if (head == null) {
            return stream;
        }

        return head.transform(new Random(seed), stream);
    }
}
