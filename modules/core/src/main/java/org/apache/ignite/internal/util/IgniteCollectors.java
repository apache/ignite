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

package org.apache.ignite.internal.util;

import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;

/**
 * Custom collectors for {@link java.util.stream.Stream} API.
 */
public class IgniteCollectors {
    /**
     * Empty constructor.
     */
    private IgniteCollectors() { }

    /**
     * Collector of {@link IgniteInternalFuture} inheritors stream to {@link GridCompoundFuture}.
     *
     * @param <T> Result type of inheritor {@link IgniteInternalFuture}.
     * @param <R> Result type of {@link GridCompoundFuture}.
     * @return Compound future that contains all stream futures
     *         and initialized with {@link GridCompoundFuture#markInitialized()}.
     */
    public static <T, R> Collector<? super IgniteInternalFuture,
        ? super GridCompoundFuture<T, R>, GridCompoundFuture<T, R>> toCompoundFuture() {
        final GridCompoundFuture<T, R> res = new GridCompoundFuture<>();

        return Collectors.collectingAndThen(
            Collectors.reducing(
                res,
                res::add,
                (a, b) -> a // No needs to merge compound futures.
            ),
            GridCompoundFuture::markInitialized
        );
    }
}
