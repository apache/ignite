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

package org.apache.ignite.ml.math.functions;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Serializable function.
 *
 * @see java.util.function.Function
 */
public interface IgniteFunction<T, R> extends Function<T, R>, Serializable {
    static <T, R> IgniteFunction<T, R> constant(R r) {
        return t -> r;
    }
//    R apply(T t);
//
//    default <V> IgniteFunction<V, R> compose(IgniteFunction<? super V, ? extends T> before) {
//        Objects.requireNonNull(before);
//        return (V v) -> apply(before.apply(v));
//    }
//
//    default <V> IgniteFunction<T, V> andThen(IgniteFunction<? super R, ? extends V> after) {
//        Objects.requireNonNull(after);
//        return (T t) -> after.apply(apply(t));
//    }
//
//    static <T> IgniteFunction<T, T> identity() {
//        return t -> t;
//    }
}
