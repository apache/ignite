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

package org.apache.ignite.internal.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Key-value view implementation for binary user-object representation.
 *
 * @implNote Key-value {@link Tuple}s represents marshalled user-objects
 * regarding the binary object concept.
 */
public class KeyValueBinaryViewImpl implements KeyValueBinaryView {
    /** Underlying storage. */
    private final TableStorage tbl;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     */
    public KeyValueBinaryViewImpl(TableStorage tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Tuple get(Tuple key) {
        Objects.requireNonNull(key);

        return marshaller().marshalKVPair(key, null);
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAsync(Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<Tuple, Tuple> getAll(Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Map<Tuple, Tuple>> getAllAsync(Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Tuple key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        final TableRow row = marshaller().marshalKVPair(key, val);

        tbl.put(row);
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> putAsync(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<Tuple, Tuple> pairs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> putAllAsync(Map<Tuple, Tuple> pairs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndPut(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndPutAsync(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(Tuple key, Tuple val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> putIfAbsentAsync(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Tuple key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> removeAsync(Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Tuple key, Tuple val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> removeAsync(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> removeAll(Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> removeAllAsync(Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndRemove(Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndRemoveAsync(Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple key, Tuple val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple key, Tuple oldVal, Tuple newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(Tuple key, Tuple oldVal,
        Tuple newVal) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndReplaceAsync(Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
        Tuple key,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> IgniteFuture<R> invokeAsync(
        Tuple key,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<Tuple, R> invokeAll(
        Collection<Tuple> keys,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> IgniteFuture<Map<Tuple, R>> invokeAllAsync(
        Collection<Tuple> keys,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return null;
    }

    /**
     * @return Marshaller.
     */
    private Marshaller marshaller() {
        return null;
    }
}
