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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.U;

public abstract class AtomicDataStructureProxy<V extends AtomicDataStructureValue>
    implements GridCacheRemovable,IgniteChangeGlobalStateSupport {
    /** Logger. */
    protected IgniteLogger log;

    /** Removed flag. */
    protected volatile boolean rmvd;

    /** Suspended future. */
    private volatile GridFutureAdapter<Void> suspendFut;

    /** Check removed flag. */
    private boolean rmvCheck;

    /** Structure name. */
    protected String name;

    /** Structure key. */
    protected GridCacheInternalKey key;

    /** Structure projection. */
    protected IgniteInternalCache<GridCacheInternalKey, V> cacheView;

    /** Cache context. */
    protected volatile GridCacheContext<GridCacheInternalKey, V> ctx;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public AtomicDataStructureProxy() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Structure name.
     * @param key Structure key.
     * @param cacheView Cache projection.
     */
    public AtomicDataStructureProxy(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, V> cacheView)
    {
        assert key != null;
        assert cacheView != null;

        this.ctx = cacheView.context();
        this.key = key;
        this.cacheView = cacheView;
        this.name = name;

        log = ctx.logger(getClass());
    }

    /**
     * @return Datastructure name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Key value.
     */
    public GridCacheInternalKey key() {
        return key;
    }

    /**
     * @return Removed flag.
     */
    public boolean removed() {
        return rmvd;
    }

    /**
     * Check removed status.
     *
     * @throws IllegalStateException If removed.
     */
    protected void checkRemoved() throws IllegalStateException {
        if (rmvd)
            throw removedError();

        GridFutureAdapter<Void> suspendFut0 = suspendFut;

        if (suspendFut0 != null && !suspendFut0.isDone())
            throw suspendedError();

        if (rmvCheck) {
            try {
                rmvd = cacheView.get(key) == null;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            rmvCheck = false;

            if (rmvd) {
                ctx.kernalContext().dataStructures().onRemoved(key, this);

                throw removedError();
            }
        }
    }

    /**
     * @return Error.
     */
    private IllegalStateException removedError() {
        return new IllegalStateException("Sequence was removed from cache: " + name);
    }

    /**
     * @return Error.
     */
    private IllegalStateException suspendedError() {
        throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(suspendFut), ctx.name());
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return rmvd = true;
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        rmvCheck = true;
    }

    /** {@inheritDoc} */
    @Override public void suspend() {
        suspendFut = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public void restart(IgniteInternalCache cache) {
        invalidateLocalState();

        cacheView = cache;
        ctx = cache.context();
        rmvCheck = true;
        suspendFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        this.ctx = kctx.cache().<GridCacheInternalKey, V>context().cacheContext(ctx.cacheId());
        this.cacheView = ctx.cache();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        // No-op.
    }

    protected void invalidateLocalState() {
        // No-op
    }

}
