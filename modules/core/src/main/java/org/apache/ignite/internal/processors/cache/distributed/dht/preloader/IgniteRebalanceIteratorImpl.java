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
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Default iterator for rebalancing.
 */
public class IgniteRebalanceIteratorImpl implements IgniteRebalanceIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** Iterators for full preloading, ordered by partition ID. */
    @Nullable private final NavigableMap<Integer, GridCloseableIterator<CacheDataRow>> fullIterators;

    /** Iterator for historical preloading. */
    @Nullable private final IgniteHistoricalIterator historicalIterator;

    /** Partitions marked as missing. */
    private final Set<Integer> missingParts = new HashSet<>();

    /** Current full iterator. */
    private Map.Entry<Integer, GridCloseableIterator<CacheDataRow>> current;

    /** */
    private boolean reachedEnd;

    /** */
    private boolean closed;

    /**
     *
     * @param fullIterators
     * @param historicalIterator
     * @throws IgniteCheckedException
     */
    public IgniteRebalanceIteratorImpl(
        NavigableMap<Integer, GridCloseableIterator<CacheDataRow>> fullIterators,
        IgniteHistoricalIterator historicalIterator) throws IgniteCheckedException {
        this.fullIterators = fullIterators;
        this.historicalIterator = historicalIterator;

        advance();
    }

    /** */
    private synchronized void advance() throws IgniteCheckedException {
        if (fullIterators.isEmpty())
            reachedEnd = true;

        while (!reachedEnd && (current == null || !current.getValue().hasNextX() || missingParts.contains(current.getKey()))) {
            if (current == null)
                current = fullIterators.firstEntry();
            else {
                current = fullIterators.ceilingEntry(current.getKey() + 1);

                if (current == null)
                    reachedEnd = true;
            }
        }

        assert current != null || reachedEnd;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean historical(int partId) {
        return historicalIterator != null && historicalIterator.contains(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isPartitionDone(int partId) {
        if (missingParts.contains(partId))
            return false;

        if (historical(partId))
            return historicalIterator.isDone(partId);

        return current == null || current.getKey() > partId;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isPartitionMissing(int partId) {
        return missingParts.contains(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized void setPartitionMissing(int partId) {
        missingParts.add(partId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasNextX() throws IgniteCheckedException {
        if (historicalIterator != null && historicalIterator.hasNextX())
            return true;

        return current != null && current.getValue().hasNextX();
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheDataRow nextX() throws IgniteCheckedException {
        if (historicalIterator != null && historicalIterator.hasNextX())
            return historicalIterator.nextX();

        if (current == null || !current.getValue().hasNextX())
            throw new NoSuchElementException();

        CacheDataRow result = current.getValue().nextX();

        assert result.partition() == current.getKey();

        advance();

        return result;
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException("remove");
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IgniteCheckedException {
        if (historicalIterator != null)
            historicalIterator.close();

        if (fullIterators != null) {
            for (GridCloseableIterator<CacheDataRow> iter : fullIterators.values())
                iter.close();
        }

        closed = true;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<CacheDataRow> iterator() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasNext() {
        try {
            return hasNextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheDataRow next() {
        try {
            return nextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void remove() {
        try {
            removeX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
