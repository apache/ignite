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

package org.apache.ignite.internal.managers.collision;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.SkipDaemon;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.jetbrains.annotations.Nullable;

/**
 * This class defines a collision manager.
 */
@SkipDaemon
public class GridCollisionManager extends GridManagerAdapter<CollisionSpi> {
    /** Reference for external listener. */
    private final AtomicReference<CollisionExternalListener> extLsnr =
        new AtomicReference<>();

    /**
     * @param ctx Grid kernal context.
     */
    public GridCollisionManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getCollisionSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        if (enabled()) {
            getSpi().setExternalCollisionListener(new CollisionExternalListener() {
                @Override public void onExternalCollision() {
                    CollisionExternalListener lsnr = extLsnr.get();

                    if (lsnr != null)
                        lsnr.onExternalCollision();
                }
            });
        }
        else
            U.warn(log, "Collision resolution is disabled (all jobs will be activated upon arrival).");

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        // Unsubscribe.
        if (enabled())
            getSpi().setExternalCollisionListener(null);

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Unsets external collision listener.
     */
    public void unsetCollisionExternalListener() {
        if (enabled())
            getSpi().setExternalCollisionListener(null);
    }

    /**
     * @param lsnr Listener to external collision events.
     */
    public void setCollisionExternalListener(@Nullable CollisionExternalListener lsnr) {
        if (enabled()) {
            if (lsnr != null && !extLsnr.compareAndSet(null, lsnr))
                assert false : "Collision external listener has already been set " +
                    "(perhaps need to add support for multiple listeners)";
            else if (log.isDebugEnabled())
                log.debug("Successfully set external collision listener: " + lsnr);
        }
    }

    /**
     * @param waitJobs List of waiting jobs.
     * @param activeJobs List of active jobs.
     * @param heldJobs List of held jobs.
     */
    public void onCollision(
        final Collection<CollisionJobContext> waitJobs,
        final Collection<CollisionJobContext> activeJobs,
        final Collection<CollisionJobContext> heldJobs) {
        if (enabled()) {
            if (log.isDebugEnabled())
                log.debug("Resolving job collisions [waitJobs=" + waitJobs + ", activeJobs=" + activeJobs + ']');

            getSpi().onCollision(new CollisionContext() {
                @Override public Collection<CollisionJobContext> activeJobs() {
                    return activeJobs;
                }

                @Override public Collection<CollisionJobContext> waitingJobs() {
                    return waitJobs;
                }

                @Override public Collection<CollisionJobContext> heldJobs() {
                    return heldJobs;
                }
            });
        }
    }
}