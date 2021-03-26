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
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements Table {
    /** Table. */
    private final TableStorage tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public TableImpl(TableStorage tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(tbl, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        return new KeyValueBinaryViewImpl(tbl);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(Tuple keyRec) {
        Marshaller marsh = marshaller();

        return tbl.get(marsh.marshalRecord(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAsync(Tuple keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(Collection<Tuple> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> getAllAsync(Collection<Tuple> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsert(Tuple rec) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<Tuple> recs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAllAsync(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndUpsertAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(Tuple rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> insertAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> insertAllAsync(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple oldRec, Tuple newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(Tuple oldRec, Tuple newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndReplaceAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Tuple keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteAsync(Tuple keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(Tuple oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteExactAsync(Tuple oldRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndDeleteAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> deleteAllAsync(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> deleteAllExactAsync(
        Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(
        Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<T> invokeAsync(
        Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(
        Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<Map<Tuple, T>> invokeAllAsync(
        Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
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
