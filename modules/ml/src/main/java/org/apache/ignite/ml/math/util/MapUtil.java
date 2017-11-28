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

package org.apache.ignite.ml.math.util;

import java.util.Collection;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Some {@link Map} related utils.
 */
public class MapUtil {
    /** */
    public static <K, V, M extends Map<K, V>> M mergeMaps(M m1, M m2, BinaryOperator<V> op, Supplier<M> mapSupplier) {
        return Stream.of(m1, m2)
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, op, mapSupplier));
    }
}
