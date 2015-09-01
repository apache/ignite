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

package org.apache.ignite.internal.managers.indexing;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.SkipDaemon;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;

/**
 * Manages cache indexing.
 */
@SkipDaemon
public class GridIndexingManager extends GridManagerAdapter<IndexingSpi> {
    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * @param ctx  Kernal context.
     */
    public GridIndexingManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getIndexingSpi());
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        busyLock.block();
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Writes key-value pair to index.
     *
     * @param space Space.
     * @param key Key.
     * @param val Value.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    public <K, V> void store(final String space, final K key, final V val, long expirationTime) throws IgniteCheckedException {
        assert key != null;
        assert val != null;
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            if (log.isDebugEnabled())
                log.debug("Storing key to cache query index [key=" + key + ", value=" + val + "]");

            getSpi().store(space, key, val, expirationTime);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param key Key.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings("unchecked")
    public void remove(String space, Object key) throws IgniteCheckedException {
        assert key != null;
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            getSpi().remove(space, key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param params Parameters collection.
     * @param filters Filters.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public IgniteSpiCloseableIterator<?> query(String space, Collection<Object> params, IndexingQueryFilter filters)
        throws IgniteCheckedException {
        if (!enabled())
            throw new IgniteCheckedException("Indexing SPI is not configured.");

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final Iterator<?> res = getSpi().query(space, params, filters);

            if (res == null)
                return new GridEmptyCloseableIterator<>();

            return new IgniteSpiCloseableIterator<Object>() {
                @Override public void close() throws IgniteCheckedException {
                    if (res instanceof AutoCloseable) {
                        try {
                            ((AutoCloseable)res).close();
                        }
                        catch (Exception e) {
                            throw new IgniteCheckedException(e);
                        }
                    }
                }

                @Override public boolean hasNext() {
                    return res.hasNext();
                }

                @Override public Object next() {
                    return res.next();
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be swapped.
     *
     * @param spaceName Space name.
     * @param key key.
     * @throws IgniteSpiException If failed.
     */
    public void onSwap(String spaceName, Object key) throws IgniteSpiException {
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            getSpi().onSwap(spaceName, key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws IgniteSpiException If failed.
     */
    public void onUnswap(String spaceName, Object key, Object val)
        throws IgniteSpiException {
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            getSpi().onUnswap(spaceName, key, val);
        }
        finally {
            busyLock.leaveBusy();
        }
    }
}