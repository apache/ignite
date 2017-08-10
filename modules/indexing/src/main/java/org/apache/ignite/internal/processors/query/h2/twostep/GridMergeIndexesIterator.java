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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.lang.IgniteOutClosure;
import org.h2.index.Cursor;
import org.h2.result.Row;

/**
 * Iterator that transparently and sequentially traverses a bunch of {@link GridMergeIndex}es.
 */
class GridMergeIndexesIterator extends GridCloseableIteratorAdapterEx<List<?>> {
    /** Iterator over indexes. */
    private final Iterator<GridMergeIndex> idxs;

    /** Current cursor. */
    private Cursor cur;

    /** Next row to return. */
    private List<Object> next;

    /** Closure to run when this iterator is closed, or when all indexes have been traversed. */
    private final IgniteOutClosure<?> clo;

    /** Cleanup flag - {@code null} iff {@link #clo} is null. */
    private final AtomicBoolean cleanupDone;

    /**
     * Constructor.
     * @param idxs Indexes to iterate over.
     * @param clo Closure to run when this iterator is closed, or when all indexes have been traversed.
     * @throws IgniteCheckedException if failed.
     */
    GridMergeIndexesIterator(Iterable<GridMergeIndex> idxs, IgniteOutClosure<?> clo) throws IgniteCheckedException {
        this.idxs = idxs.iterator();
        this.clo = clo;

        cleanupDone = (clo != null ? new AtomicBoolean() : null);

        goNext();
    }

    /** {@inheritDoc} */
    @Override public boolean onHasNext() throws IgniteCheckedException {
        return next != null;
    }

    /** {@inheritDoc} */
    @Override public List<?> onNext() throws IgniteCheckedException {
        final List<?> res = next;

        if (res == null)
            throw new NoSuchElementException();

        goNext();

        return res;
    }

    /**
     * Retrieve next row.
     * @throws IgniteCheckedException if failed.
     */
    private void goNext() throws IgniteCheckedException {
        next = null;

        try {
            boolean gotNext = false;

            while (cur == null || !(gotNext = cur.next())) {
                if (idxs.hasNext())
                    cur = idxs.next().findInStream(null, null);
                else {
                    // We're out of records, let's clean up.
                    doCleanup();

                    break;
                }
            }

            if (gotNext) {
                Row row = cur.get();

                int cols = row.getColumnCount();

                List<Object> res = new ArrayList<>(cols);

                for (int c = 0; c < cols; c++)
                    res.add(row.getValue(c).getObject());

                next = res;
            }
        }
        catch (Throwable e) {
            doCleanup();

            throw new IgniteCheckedException(e);
        }
    }

    /** Run {@link #clo} if it's present and hasn't been invoked yet. */
    private void doCleanup() {
        if (clo != null && cleanupDone.compareAndSet(false, true))
            clo.apply();
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        doCleanup();

        super.onClose();
    }
}
