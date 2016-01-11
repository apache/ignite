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

package org.apache.ignite.thread;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.worker.GridWorker;

/**
 * This class adds some necessary plumbing on top of the {@link Thread} class.
 * Specifically, it adds:
 * <ul>
 *      <li>Consistent naming of threads</li>
 *      <li>Dedicated parent thread group</li>
 *      <li>Backing interrupted flag</li>
 *      <li>Name of the grid this thread belongs to</li>
 * </ul>
 * <b>Note</b>: this class is intended for internal use only.
 */
public class IgniteThread extends Thread {
    /** Default thread's group. */
    private static final ThreadGroup DFLT_GRP = new ThreadGroup("ignite");

    /** Number of all grid threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** The name of the grid this thread belongs to. */
    protected final String gridName;

    /**
     * Creates thread with given worker.
     *
     * @param worker Runnable to create thread with.
     */
    public IgniteThread(GridWorker worker) {
        this(DFLT_GRP, worker.gridName(), worker.name(), worker);
    }

    /**
     * Creates grid thread with given name for a given grid.
     *
     * @param gridName Name of grid this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public IgniteThread(String gridName, String threadName, Runnable r) {
        this(DFLT_GRP, gridName, threadName, r);
    }

    /**
     * Creates grid thread with given name for a given grid with specified
     * thread group.
     *
     * @param grp Thread group.
     * @param gridName Name of grid this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public IgniteThread(ThreadGroup grp, String gridName, String threadName, Runnable r) {
        super(grp, r, createName(cntr.incrementAndGet(), threadName, gridName));

        this.gridName = gridName;
    }

    /**
     * @param gridName Name of grid this thread is created for.
     * @param threadGrp Thread group.
     * @param threadName Name of thread.
     */
    protected IgniteThread(String gridName, ThreadGroup threadGrp, String threadName) {
        super(threadGrp, threadName);

        this.gridName = gridName;
    }

    /**
     * Gets name of the grid this thread belongs to.
     *
     * @return Name of the grid this thread belongs to.
     */
    public String getGridName() {
        return gridName;
    }

    /**
     * Creates new thread name.
     *
     * @param num Thread number.
     * @param threadName Thread name.
     * @param gridName Grid name.
     * @return New thread name.
     */
    protected static String createName(long num, String threadName, String gridName) {
        return threadName + "-#" + num + '%' + gridName + '%';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThread.class, this, "name", getName());
    }
}