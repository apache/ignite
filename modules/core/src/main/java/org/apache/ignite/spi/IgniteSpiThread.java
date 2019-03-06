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

package org.apache.ignite.spi;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThread;

/**
 * This class provides convenient adapter for threads used by SPIs.
 * This class adds necessary plumbing on top of the {@link IgniteThread} class:
 * <ul>
 *      <li>Proper exception handling in {@link #body()}</li>
 * </ul>
 */
public abstract class IgniteSpiThread extends IgniteThread {
    /** Default thread's group. */
    public static final ThreadGroup DFLT_GRP = new ThreadGroup("ignite-spi");

    /** Number of all system threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** Grid logger. */
    private final IgniteLogger log;

    /**
     * Creates thread with given {@code name}.
     *
     * @param igniteInstanceName Name of grid this thread is created in.
     * @param name Thread's name.
     * @param log Grid logger to use.
     */
    protected IgniteSpiThread(String igniteInstanceName, String name, IgniteLogger log) {
        super(igniteInstanceName, DFLT_GRP, createName(cntr.incrementAndGet(), name, igniteInstanceName));

        assert log != null;

        this.log = log;
    }

    /** {@inheritDoc} */
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