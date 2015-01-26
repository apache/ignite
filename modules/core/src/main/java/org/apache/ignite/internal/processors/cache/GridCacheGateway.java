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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Cache gateway.
 */
@GridToStringExclude
public class GridCacheGateway<K, V> {
    /** Context. */
    private final GridCacheContext<K, V> ctx;

    /**
     * @param ctx Cache context.
     */
    public GridCacheGateway(GridCacheContext<K, V> ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /**
     * Enter a cache call.
     */
    public void enter() {
        if (ctx.deploymentEnabled())
            ctx.deploy().onEnter();

        // Must unlock in case of unexpected errors to avoid
        // deadlocks during kernal stop.
        try {
            ctx.kernalContext().gateway().readLock();
        }
        catch (IllegalStateException e) {
            // This exception is thrown only in case if grid has already been stopped
            // and we must not call readUnlock.
            throw e;
        }
        catch (RuntimeException | Error e) {
            try {
                ctx.kernalContext().gateway().readUnlock();
            }
            catch (IllegalMonitorStateException ignore) {
                // No-op.
            }

            throw e;
        }
    }

    /**
     * Leave a cache call entered by {@link #enter()} method.
     */
    public void leave() {
        try {
            // Unwind eviction notifications.
            CU.unwindEvicts(ctx);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }

    /**
     * @param prj Projection to guard.
     * @return Previous projection set on this thread.
     */
    @Nullable public GridCacheProjectionImpl<K, V> enter(@Nullable GridCacheProjectionImpl<K, V> prj) {
        try {
            ctx.itHolder().checkWeakQueue();

            GridCacheAdapter<K, V> cache = ctx.cache();

            GridCachePreloader<K, V> preldr = cache != null ? cache.preloader() : null;

            if (preldr == null)
                throw new IllegalStateException("Grid is in invalid state to perform this operation. " +
                    "It either not started yet or has already being or have stopped [gridName=" + ctx.gridName() + ']');

            preldr.startFuture().get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for cache preloader start [cacheName=" +
                ctx.name() + "]", e);
        }

        if (ctx.deploymentEnabled())
            ctx.deploy().onEnter();

        // Must unlock in case of unexpected errors to avoid
        // deadlocks during kernal stop.
        try {
            ctx.kernalContext().gateway().readLock();

            // Set thread local projection per call.
            GridCacheProjectionImpl<K, V> prev = ctx.projectionPerCall();

            if (prev != null || prj != null)
                ctx.projectionPerCall(prj);

            return prev;
        }
        catch (IllegalStateException e) {
            // This exception is thrown only in case if grid has already been stopped
            // and we must not call readUnlock.
            throw e;
        }
        catch (RuntimeException | Error e) {
            try {
                ctx.kernalContext().gateway().readUnlock();
            }
            catch (IllegalMonitorStateException ignore) {
                // No-op.
            }

            throw e;
        }
    }

    /**
     * @param prev Previous.
     */
    public void leave(GridCacheProjectionImpl<K, V> prev) {
        try {
            // Unwind eviction notifications.
            CU.unwindEvicts(ctx);

            // Return back previous thread local projection per call.
            ctx.projectionPerCall(prev);
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }
    }
}
