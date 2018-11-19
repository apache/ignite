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

import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Interface of transformer of upstream.
 *
 * @param <K> Type of keys in the upstream.
 * @param <V> Type of values in the upstream.
 */
@FunctionalInterface
public interface UpstreamTransformer<K, V> extends IgniteFunction<Stream<UpstreamEntry<K, V>>, Stream<UpstreamEntry<K, V>>> {
    default UpstreamTransformer<K, V> then(UpstreamTransformer<K, V> other) {
        return s -> other.apply(this.apply(s));
    }
}
