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

package org.apache.ignite.spi;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class provides convenient adapter for threads used by SPIs.
 * This class adds necessary plumbing on top of the {@link Thread} class:
 * <ul>
 * <li>Consistent naming of threads</li>
 * <li>Dedicated parent thread group</li>
 * </ul>
 */
public abstract class IgniteSpiThread extends Thread {
    /** Default thread's group. */
    public static final ThreadGroup DFLT_GRP = new ThreadGroup("ignite-spi");

    /** Number of all system threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** Grid logger. */
    private final IgniteLogger log;

    /**
     * Creates thread with given {@code name}.
     *
     * @param gridName Name of grid this thread is created in.
     * @param name Thread's name.
     * @param log Grid logger to use.
     */
    protected IgniteSpiThread(String gridName, String name, IgniteLogger log) {
        super(DFLT_GRP, name + "-#" + cntr.incrementAndGet() + '%' + gridName);

        assert log != null;

        this.log = log;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass"})
    @Override public final void run() {
        try {
            body();
        }
        catch (InterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);

            Thread.currentThread().interrupt();
        }
        // Catch everything to make sure that it gets logged properly and
        // not to kill any threads from the underlying thread pool.
        catch (Throwable e) {
            U.error(log, "Runtime error caught during grid runnable execution: " + this, e);

            if (e instanceof Error)
                throw e;
        }
        finally {
            cleanup();

            if (log.isDebugEnabled()) {
                if (isInterrupted())
                    log.debug("Grid runnable finished due to interruption without cancellation: " + getName());
                else
                    log.debug("Grid runnable finished normally: " + getName());
            }
        }
    }

    /**
     * Should be overridden by child classes if cleanup logic is required.
     */
    protected void cleanup() {
        // No-op.
    }

    /**
     * Body of SPI thread.
     *
     * @throws InterruptedException If thread got interrupted.
     */
    protected abstract void body() throws InterruptedException;

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSpiThread.class, this, "name", getName());
    }
}