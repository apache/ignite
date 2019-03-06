/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Iterator that transparently and sequentially traverses a bunch of {@link ReduceIndex} objects.
 */
public class ReduceIndexIterator implements Iterator<List<?>>, AutoCloseable {
    /** Reduce query executor. */
    private final GridReduceQueryExecutor rdcExec;

    /** Participating nodes. */
    private final Collection<ClusterNode> nodes;

    /** Query run. */
    private final ReduceQueryRun run;

    /** Query request ID. */
    private final long qryReqId;

    /** Distributed joins. */
    private final boolean distributedJoins;

    /** Iterator over indexes. */
    private final Iterator<ReduceIndex> idxIter;

    /** Current cursor. */
    private Cursor cursor;

    /** Next row to return. */
    private List<Object> next;

    /** Whether remote resources were released. */
    private boolean released;

    /** */
    private MvccQueryTracker mvccTracker;

    /**
     * Constructor.
     *
     * @param rdcExec Reduce query executor.
     * @param nodes Participating nodes.
     * @param run Query run.
     * @param qryReqId Query request ID.
     * @param distributedJoins Distributed joins.
     */
    public ReduceIndexIterator(GridReduceQueryExecutor rdcExec,
        Collection<ClusterNode> nodes,
        ReduceQueryRun run,
        long qryReqId,
        boolean distributedJoins,
        @Nullable MvccQueryTracker mvccTracker
    ) {
        this.rdcExec = rdcExec;
        this.nodes = nodes;
        this.run = run;
        this.qryReqId = qryReqId;
        this.distributedJoins = distributedJoins;
        this.mvccTracker = mvccTracker;

        idxIter = run.indexes().iterator();

        advance();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return next != null;
    }

    /** {@inheritDoc} */
    @Override public List<?> next() {
        List<Object> res = next;

        if (res == null)
            throw new NoSuchElementException();

        advance();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        releaseIfNeeded();
    }

    /**
     * Advance iterator.
     */
    private void advance() {
        next = null;

        try {
            boolean hasNext = false;

            while (cursor == null || !(hasNext = cursor.next())) {
                if (idxIter.hasNext())
                    cursor = idxIter.next().findInStream(null, null);
                else {
                    releaseIfNeeded();

                    break;
                }
            }

            if (hasNext) {
                Row row = cursor.get();

                int cols = row.getColumnCount();

                List<Object> res = new ArrayList<>(cols);

                for (int c = 0; c < cols; c++)
                    res.add(row.getValue(c).getObject());

                next = res;
            }
        }
        catch (Exception e) {
            releaseIfNeeded();

            throw e;
        }
    }

    /**
     * Close routine.
     */
    private void releaseIfNeeded() {
        if (!released) {
            try {
                rdcExec.releaseRemoteResources(nodes, run, qryReqId, distributedJoins, mvccTracker);
            }
            finally {
                released = true;
            }
        }
    }
}
