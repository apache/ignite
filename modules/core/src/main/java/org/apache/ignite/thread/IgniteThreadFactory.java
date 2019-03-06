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

package org.apache.ignite.thread;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * This class provides implementation of {@link ThreadFactory} factory
 * for creating grid threads.
 */
public class IgniteThreadFactory implements ThreadFactory {
    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** Thread name. */
    private final String threadName;

    /** Index generator for threads. */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** */
    private final byte plc;

    /** Exception handler. */
    private final UncaughtExceptionHandler eHnd;

    /**
     * Constructs new thread factory for given grid. All threads will belong
     * to the same default thread group.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param threadName Thread name.
     */
    public IgniteThreadFactory(String igniteInstanceName, String threadName) {
        this(igniteInstanceName, threadName, null);
    }

    /**
     * Constructs new thread factory for given grid. All threads will belong
     * to the same default thread group.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param threadName Thread name.
     * @param eHnd Uncaught exception handler.
     */
    public IgniteThreadFactory(String igniteInstanceName, String threadName, UncaughtExceptionHandler eHnd) {
        this(igniteInstanceName, threadName, GridIoPolicy.UNDEFINED, eHnd);
    }

    /**
     * Constructs new thread factory for given grid. All threads will belong
     * to the same default thread group.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param threadName Thread name.
     * @param plc {@link GridIoPolicy} for thread pool.
     * @param eHnd Uncaught exception handler.
     */
    public IgniteThreadFactory(String igniteInstanceName, String threadName, byte plc, UncaughtExceptionHandler eHnd) {
        this.igniteInstanceName = igniteInstanceName;
        this.threadName = threadName;
        this.plc = plc;
        this.eHnd = eHnd;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        Thread thread = new IgniteThread(igniteInstanceName, threadName, r, idxGen.incrementAndGet(), -1, plc);

        if (eHnd != null)
            thread.setUncaughtExceptionHandler(eHnd);

        return thread;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThreadFactory.class, this, super.toString());
    }
}
