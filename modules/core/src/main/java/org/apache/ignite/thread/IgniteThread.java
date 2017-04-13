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
import org.apache.ignite.internal.util.typedef.internal.A;
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
    /** Index for unassigned thread. */
    public static final int GRP_IDX_UNASSIGNED = -1;

    /** Default thread's group. */
    private static final ThreadGroup DFLT_GRP = new ThreadGroup("ignite");

    /** Number of all grid threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** The name of the Ignite instance this thread belongs to. */
    protected final String igniteInstanceName;

    /** */
    private int compositeRwLockIdx;

    /** */
    private final int stripe;

    /**
     * Creates thread with given worker.
     *
     * @param worker Runnable to create thread with.
     */
    public IgniteThread(GridWorker worker) {
        this(DFLT_GRP, worker.igniteInstanceName(), worker.name(), worker, GRP_IDX_UNASSIGNED, -1);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance.
     *
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public IgniteThread(String igniteInstanceName, String threadName, Runnable r) {
        this(igniteInstanceName, threadName, r, GRP_IDX_UNASSIGNED, -1);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance.
     *
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     * @param grpIdx Index within a group.
     * @param stripe Non-negative stripe number if this thread is striped pool thread.
     */
    public IgniteThread(String igniteInstanceName, String threadName, Runnable r, int grpIdx, int stripe) {
        this(DFLT_GRP, igniteInstanceName, threadName, r, grpIdx, stripe);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance with specified
     * thread group.
     *
     * @param grp Thread group.
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     * @param grpIdx Thread index within a group.
     * @param stripe Non-negative stripe number if this thread is striped pool thread.
     */
    public IgniteThread(ThreadGroup grp, String igniteInstanceName, String threadName, Runnable r, int grpIdx, int stripe) {
        super(grp, r, createName(cntr.incrementAndGet(), threadName, igniteInstanceName));

        A.ensure(grpIdx >= -1, "grpIdx >= -1");

        this.igniteInstanceName = igniteInstanceName;
        this.compositeRwLockIdx = grpIdx;
        this.stripe = stripe;
    }

    /**
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadGrp Thread group.
     * @param threadName Name of thread.
     */
    protected IgniteThread(String igniteInstanceName, ThreadGroup threadGrp, String threadName) {
        super(threadGrp, threadName);

        this.igniteInstanceName = igniteInstanceName;
        this.compositeRwLockIdx = GRP_IDX_UNASSIGNED;
        this.stripe = -1;
    }

    /**
     * @return Non-negative stripe number if this thread is striped pool thread.
     */
    public int stripe() {
        return stripe;
    }

    /**
     * Gets name of the Ignite instance this thread belongs to.
     *
     * @return Name of the Ignite instance this thread belongs to.
     */
    public String getIgniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * @return Composite RW lock index.
     */
    public int compositeRwLockIndex() {
        return compositeRwLockIdx;
    }

    /**
     * @param compositeRwLockIdx Composite RW lock index.
     */
    public void compositeRwLockIndex(int compositeRwLockIdx) {
        this.compositeRwLockIdx = compositeRwLockIdx;
    }

    /**
     * Creates new thread name.
     *
     * @param num Thread number.
     * @param threadName Thread name.
     * @param igniteInstanceName Ignite instance name.
     * @return New thread name.
     */
    protected static String createName(long num, String threadName, String igniteInstanceName) {
        return threadName + "-#" + num + '%' + igniteInstanceName + '%';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThread.class, this, "name", getName());
    }
}
