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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.util.Optional;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Decorator of {@link WALIterator} which allow filter record by {@link WALPointer} and {@link WALRecord}.
 */
public class FilteredWalIterator extends GridCloseableIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>> implements WALIterator {
    /** Source WAL iterator which provide data for filtering. */
    private final WALIterator delegateWalIter;

    /** Filter for filtering iterated data. */
    private final Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter;

    /** Next record in iterator for supporting iterator pattern. */
    private IgniteBiTuple<WALPointer, WALRecord> next;

    /**
     * @param walIterator Source WAL iterator which provide data for filtering.
     * @param filter Filter for filtering iterated data.
     */
    public FilteredWalIterator(WALIterator walIterator,
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter) throws IgniteCheckedException {
        this.filter = filter == null ? (r) -> true : filter;
        this.delegateWalIter = walIterator;

        //Initiate iterator by first record.
        onNext();
    }

    /** {@inheritDoc} **/
    @Override public Optional<WALPointer> lastRead() {
        return Optional.ofNullable(next == null ? null : next.get1());
    }

    /** {@inheritDoc} **/
    @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
        IgniteBiTuple<WALPointer, WALRecord> cur = next;

        next = nextFilteredRecord();

        return cur;
    }

    /**
     * @return Next filtered record.
     */
    private IgniteBiTuple<WALPointer, WALRecord> nextFilteredRecord() {
        while (delegateWalIter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = delegateWalIter.next();

            if (filter.test(next))
                return next;
        }

        return null;
    }

    /** {@inheritDoc} **/
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return next != null;
    }
}
