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

package org.apache.ignite.internal.processors.metric.list;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/** Utils methods for monitoring list feature. */
public final class ListUtils {
    /**
     * Return consumer that filters all events expept those equals to {@code pattern}.
     *
     * @param pattern Pattern to search.
     * @param f Function to extract pattern from value.
     * @param c Consumer of filtered values.
     * @param <V> Value type.
     * @param <P> Pattern type.
     * @return Consumer that filters values that not satisfy pattern.
     */
    public static <V, P> Consumer<V> listenOnlyEqual(final P pattern, final Function<V, P> f, final Consumer<V> c) {
        return v -> {
            if (!Objects.equals(pattern, f.apply(v)))
                return;

            c.accept(v);
        };
    }
}
