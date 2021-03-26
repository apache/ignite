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
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.RecordMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Record view implementation.
 */
public class RecordViewImpl<R> implements RecordView<R> {
    /** Table */
    private final TableStorage tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param mapper Record class mapper.
     */
    public RecordViewImpl(TableStorage tbl, RecordMapper<R> mapper) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public R get(R keyRec) {
        Marshaller marsh = marshaller();

        TableRow kRow = marsh.serialize(keyRec);

        TableRow tRow = tbl.get(kRow);

        return marsh.deserializeToRecord(tRow);
    }

    /** {@inheritDoc} */
    @Override public R fill(R recObjToFill) {
        Marshaller marsh = marshaller();

        TableRow kRow = marsh.serialize(recObjToFill);

        TableRow tRow = tbl.get(kRow);

        return marsh.deserializeToRecord(tRow, recObjToFill);
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<R> getAsync(R keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> getAll(Collection<R> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<R>> getAllAsync(Collection<R> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsert(R rec) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<R> recs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAllAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public R getAndUpsert(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<R> getAndUpsertAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> insertAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> insertAll(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<R>> insertAllAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(R oldRec, R newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(R oldRec, R newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public R getAndReplace(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<R> getAndReplaceAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(R keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteAsync(R keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(R oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteExactAsync(R oldRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public R getAndDelete(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<R> getAndDeleteAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> deleteAll(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<R>> deleteAllAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> deleteAllExact(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<R>> deleteAllExactAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(R keyRec, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<T> invokeAsync(R keyRec,
        InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<R, T> invokeAll(
        Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<Map<R, T>> invokeAllAsync(
        Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc
    ) {
        return null;
    }

    /**
     * @return Marshaller.
     */
    private Marshaller marshaller() {
        return null;        // table.schemaManager().marshaller();
    }

}
