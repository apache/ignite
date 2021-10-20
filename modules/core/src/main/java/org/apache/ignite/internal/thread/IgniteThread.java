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

package org.apache.ignite.internal.thread;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.internal.tostring.S;

/**
 * This class adds some necessary plumbing on top of the {@link Thread} class.
 * Specifically, it adds:
 * <ul>
 *      <li>Consistent naming of threads</li>;
 *      <li>Name of the grid this thread belongs to</li>.
 * </ul>
 * <b>Note</b>: this class is intended for internal use only.
 */
public class IgniteThread extends Thread {
    /** Number of all grid threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** The name of the Ignite instance this thread belongs to. */
    protected final String igniteInstanceName;

    /**
     * Creates grid thread with given name for a given Ignite instance.
     *
     * @param nodeName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     */
    public IgniteThread(String nodeName, String threadName) {
        this(nodeName, threadName, null);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance.
     *
     * @param nodeName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public IgniteThread(String nodeName, String threadName, Runnable r) {
        super(r, createName(cntr.incrementAndGet(), threadName, nodeName));

        this.igniteInstanceName = nodeName;
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
     * @return IgniteThread or {@code null} if current thread is not an instance of IgniteThread.
     */
    public static IgniteThread current() {
        Thread thread = Thread.currentThread();

        return thread.getClass() == IgniteThread.class || thread instanceof IgniteThread ?
            ((IgniteThread)thread) : null;
    }

    /**
     * Create prefix for thread name.
     */
    public static String threadPrefix(String nodeName, String threadName) {
        return "%" + nodeName + "%" + threadName + "-";
    }

    /**
     * Creates new thread name.
     *
     * @param num Thread number.
     * @param threadName Thread name.
     * @param nodeName Ignite instance name.
     * @return New thread name.
     */
    protected static String createName(long num, String threadName, String nodeName) {
        return threadPrefix(nodeName, threadName) + num;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThread.class, this, "name", getName());
    }
}
