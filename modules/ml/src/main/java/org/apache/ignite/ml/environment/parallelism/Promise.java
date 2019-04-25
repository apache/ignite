/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.environment.parallelism;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Future interface extension for lambda-friendly interface.
 *
 * @param <T> Type of result.
 */
public interface Promise<T> extends Future<T> {
    /**
     * Await result of Future and return it.
     * Wrap exceptions from Future to RuntimeException.
     */
    public default T unsafeGet() {
        try {
            return get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap result of Future to Optional-object.
     * If Future throws an exception then it returns Optional.empty.
     */
    public default Optional<T> getOpt() {
        try {
            return Optional.of(get());
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
    }
}
