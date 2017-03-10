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
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.jetbrains.annotations.Nullable;

public class IgniteRebalanceIteratorImpl implements IgniteRebalanceIterator {

    @Nullable private final NavigableMap<Integer, GridCloseableIterator<CacheDataRow>> fullIterators;

    @Nullable private final IgniteHistoricalIterator historicalIterator;

    private final Set<Integer> missingParts = new HashSet<>();

    private Map.Entry<Integer, GridCloseableIterator<CacheDataRow>> current;

    private boolean reachedEnd;

    private boolean closed;

    public IgniteRebalanceIteratorImpl(
        NavigableMap<Integer, GridCloseableIterator<CacheDataRow>> fullIterators,
        IgniteHistoricalIterator historicalIterator) throws IgniteCheckedException {
        this.fullIterators = fullIterators;
        this.historicalIterator = historicalIterator;

        advance();
    }

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

    @Override public synchronized boolean historical(int partId) {
        return historicalIterator != null && historicalIterator.includes(partId);
    }

    @Override public synchronized boolean isPartitionDone(int partId) {
        if (historical(partId))
            return historicalIterator.isDone(partId);

        return current == null || current.getKey() > partId;
    }

    @Override public synchronized boolean isPartitionMissing(int partId) {
        return missingParts.contains(partId);
    }

    @Override public synchronized void setPartitionMissing(int partId) {
        missingParts.add(partId);
    }

    @Override public synchronized boolean hasNextX() throws IgniteCheckedException {
        if (historicalIterator != null && historicalIterator.hasNextX())
            return true;

        return current != null && current.getValue().hasNextX();
    }

    @Override public synchronized CacheDataRow nextX() throws IgniteCheckedException {
        if (historicalIterator != null && historicalIterator.hasNextX())
            return historicalIterator.nextX();

        if (current == null || !current.getValue().hasNextX())
            throw new NoSuchElementException();

        CacheDataRow result = current.getValue().nextX();

        advance();

        return result;
    }

    @Override public synchronized void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException("remove");
    }

    @Override public synchronized void close() throws IgniteCheckedException {
        if (historicalIterator != null)
            historicalIterator.close();

        if (fullIterators != null) {
            for (GridCloseableIterator<CacheDataRow> iter : fullIterators.values())
                iter.close();
        }

        closed = true;
    }

    @Override public synchronized boolean isClosed() {
        return closed;
    }

    @Override public synchronized Iterator<CacheDataRow> iterator() {
        return this;
    }

    @Override public synchronized boolean hasNext() {
        try {
            return hasNextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override public synchronized CacheDataRow next() {
        try {
            return nextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override public synchronized void remove() {
        try {
            removeX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
