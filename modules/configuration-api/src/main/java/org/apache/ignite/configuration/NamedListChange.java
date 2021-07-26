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

package org.apache.ignite.configuration;

import java.util.function.Consumer;

/**
 * Closure parameter for {@link NamedConfigurationTree#change(Consumer)} method. Contains methods to modify named lists.
 *
 * @param <Change> Type for changing named list elements of this particular list.
 */
public interface NamedListChange<Change> extends NamedListView<Change> {
    /**
     * Creates a new value in the named list configuration.
     *
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of the parameters is null.
     * @throws IllegalArgumentException If an element with the given name already exists.
     */
    NamedListChange<Change> create(String key, Consumer<Change> valConsumer);

    /**
     * Creates a new value at the given position in the named list configuration.
     *
     * @param index Index of the inserted element.
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of the parameters is null.
     * @throws IndexOutOfBoundsException If index is negative of exceeds the size of the list.
     * @throws IllegalArgumentException If an element with the given name already exists.
     */
    NamedListChange<Change> create(int index, String key, Consumer<Change> valConsumer);

    /**
     * Create a new value after a given precedingKey key in the named list configuration.
     *
     * @param precedingKey Name of the preceding element.
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If element with given name already exists
     *      or if {@code precedingKey} element doesn't exist.
     */
    NamedListChange<Change> createAfter(String precedingKey, String key, Consumer<Change> valConsumer);

    /**
     * Updates a value in the named list configuration. If the value cannot be found, creates a new one instead.
     *
     * @param key Key for the value to be updated.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If {@link #delete(String)} has been invoked with the same key previously.
     */
    NamedListChange<Change> createOrUpdate(String key, Consumer<Change> valConsumer);

    /**
     * Remove the value from named list configuration.
     *
     * @param key Key for the value to be removed.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If key is null.
     * @throws IllegalArgumentException If {@link #createOrUpdate(String, Consumer)} has been invoked with the same key
     *      previously.
     */
    NamedListChange<Change> delete(String key);
}
