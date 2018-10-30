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

// TODO: place comment in the right place.
//The same {@link Stream} of upstream entries
//    * should be passed to both {@code context} builder and partition {@code data} builder. But in transformer
//    * pseudo-random logic can present.
//    * To fix outcome of such a logic for each act of dataset building, for each transformer
//    * we pass supplier of data making transformer deterministic.
public abstract class UpstreamTransformer<K, V, T> {
    private UpstreamTransformer<K, V, ?> next;

    public UpstreamTransformer() {
        this(null);
    }

    public UpstreamTransformer(UpstreamTransformer<K, V, ?> next) {
        this.next = next;
    }

    public void setNext(UpstreamTransformer<K, V, ?> next) {
        this.next = next;
    }

    public Stream<UpstreamEntry<K, V>> transform(Random rnd, Stream<UpstreamEntry<K, V>> stream) {
        Stream<UpstreamEntry<K, V>> res = transform(createData(rnd), stream);

        if (next != null) {
            res = next.transform(rnd, res);
        }

        return res;
    }

    public abstract T createData(Random rnd);
    public abstract Stream<UpstreamEntry<K, V>> transform(T data, Stream<UpstreamEntry<K, V>> upstream);
}
