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
import java.util.function.Supplier;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public final class ListUtils {

    /**
     * Adds {@code row} to the {@code list} if row not exists.
     *
     * @param list List.
     * @param row Row supplier.
     * @param <Id> Id type.
     * @param <R> Row type.
     */
    public static <Id, R extends MonitoringRow<Id>> void addIfAbsentToList(@Nullable final MonitoringListImpl<Id, R> list,
        final Supplier<R> row) {
        if (list == null)
            return;

        list.addIfAbsent(row.get());
    }

    /**
     * Adds {@code row} to the {@code list}.
     *
     * @param list List.
     * @param row Row supplier.
     * @param <Id> Id type.
     * @param <R> Row type.
     */
    public static <Id, R extends MonitoringRow<Id>> void addToList(@Nullable final MonitoringListImpl<Id, R> list,
        final Supplier<R> row) {
        if (list == null)
            return;

        list.add(row.get());
    }

    /**
     * Removes row with the {@code id} from the {@code list}.
     *
     * @param list List.
     * @param id Id.
     * @param <Id> Id type.
     * @param <R> Row type.
     */
    public static <Id, R extends MonitoringRow<Id>> void removeFromList(@Nullable final MonitoringListImpl<Id, R> list,
        final Id id) {
        if (list == null)
            return;

        list.remove(id);
    }

    /**
     * Clears list.
     *
     * @param list List.
     */
    public static void clearList(@Nullable final MonitoringListImpl<?, ?> list) {
        if (list == null)
            return;

        list.clear();
    }

    /**
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
