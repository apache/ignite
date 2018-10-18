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
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
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

    /** */
    private final byte plc;

    /** */
    private boolean executingEntryProcessor;

    /** */
    private boolean holdsTopLock;

    /**
     * Creates thread with given worker.
     *
     * @param worker Runnable to create thread with.
     */
    public IgniteThread(GridWorker worker) {
        this(worker.igniteInstanceName(), worker.name(), worker, GRP_IDX_UNASSIGNED, -1, GridIoPolicy.UNDEFINED);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance.
     *
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public IgniteThread(String igniteInstanceName, String threadName, Runnable r) {
        this(igniteInstanceName, threadName, r, GRP_IDX_UNASSIGNED, -1, GridIoPolicy.UNDEFINED);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance with specified
     * thread group.
     *
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     * @param grpIdx Thread index within a group.
     * @param stripe Non-negative stripe number if this thread is striped pool thread.
     */
    public IgniteThread(String igniteInstanceName, String threadName, Runnable r, int grpIdx, int stripe, byte plc) {
        super(DFLT_GRP, r, createName(cntr.incrementAndGet(), threadName, igniteInstanceName));

        A.ensure(grpIdx >= -1, "grpIdx >= -1");

        this.igniteInstanceName = igniteInstanceName;
        this.compositeRwLockIdx = grpIdx;
        this.stripe = stripe;
        this.plc = plc;
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
        this.plc = GridIoPolicy.UNDEFINED;
    }

    /**
     * @return Related {@link GridIoPolicy} for internal Ignite pools.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @return Non-negative stripe number if this thread is striped pool thread.
     */
    public int stripe() {
        return stripe;
    }

    /**
     * @return {@code True} if thread belongs to pool processing cache operations.
     */
    public boolean cachePoolThread() {
        return stripe >= 0 ||
            plc == GridIoPolicy.SYSTEM_POOL ||
            plc == GridIoPolicy.UTILITY_CACHE_POOL;
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
     * @return {@code True} if thread is currently executing entry processor.
     */
    public boolean executingEntryProcessor() {
        return executingEntryProcessor;
    }

    /**
     * @return {@code True} if thread is currently holds topology lock.
     */
    public boolean holdsTopLock() {
        return holdsTopLock;
    }

    /**
     * Callback before entry processor execution is started.
     */
    public static void onEntryProcessorEntered(boolean holdsTopLock) {
        Thread curThread = Thread.currentThread();

        if (curThread instanceof IgniteThread) {
            ((IgniteThread)curThread).executingEntryProcessor = true;

            ((IgniteThread)curThread).holdsTopLock = holdsTopLock;
        }
    }

    /**
     * Callback after entry processor execution is finished.
     */
    public static void onEntryProcessorLeft() {
        Thread curThread = Thread.currentThread();

        if (curThread instanceof IgniteThread)
            ((IgniteThread)curThread).executingEntryProcessor = false;
    }

    /**
     * @return IgniteThread or {@code null} if current thread is not an instance of IgniteThread.
     */
    public static IgniteThread current() {
        Thread thread = Thread.currentThread();

        return thread.getClass() == IgniteThread.class || thread instanceof IgniteThread ?
            ((IgniteThread)thread) : null;
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
        return threadName + "-#" + num + (igniteInstanceName != null ? '%' + igniteInstanceName + '%' : "");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThread.class, this, "name", getName());
    }
}
