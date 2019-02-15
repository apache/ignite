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

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Replacement for Hadoop {@code org.apache.hadoop.util.Daemon} class.
 */
public class HadoopDaemon extends Thread {
    /** Lock object used for synchronization. */
    private static final Object lock = new Object();

    /** Collection to hold the threads to be stopped. */
    private static Collection<HadoopDaemon> daemons = new LinkedList<>();

    {
        setDaemon(true); // always a daemon
    }

    /** Runnable of this thread, may be this. */
    final Runnable runnable;

    /**
     * Construct a daemon thread.
     */
    public HadoopDaemon() {
        super();

        runnable = this;

        enqueueIfNeeded();
    }

    /**
     * Construct a daemon thread.
     */
    public HadoopDaemon(Runnable runnable) {
        super(runnable);

        this.runnable = runnable;

        this.setName(runnable.toString());

        enqueueIfNeeded();
    }

    /**
     * Construct a daemon thread to be part of a specified thread group.
     */
    public HadoopDaemon(ThreadGroup grp, Runnable runnable) {
        super(grp, runnable);

        this.runnable = runnable;

        this.setName(runnable.toString());

        enqueueIfNeeded();
    }

    /**
     * Getter for the runnable. May return this.
     *
     * @return the runnable
     */
    public Runnable getRunnable() {
        return runnable;
    }

    /**
     * if the runnable is a Hadoop org.apache.hadoop.hdfs.PeerCache Runnable.
     *
     * @param r the runnable.
     * @return true if it is.
     */
    private static boolean isPeerCacheRunnable(Runnable r) {
        String name = r.getClass().getName();

        return name.startsWith("org.apache.hadoop.hdfs.PeerCache");
    }

    /**
     * Enqueue this thread if it should be stopped upon the task end.
     */
    private void enqueueIfNeeded() {
        synchronized (lock) {
            if (daemons == null)
                throw new RuntimeException("Failed to create HadoopDaemon (its registry is already cleared): " +
                    "[classLoader=" + getClass().getClassLoader() + ']');

            if (runnable.getClass().getClassLoader() == getClass().getClassLoader() && isPeerCacheRunnable(runnable))
                daemons.add(this);
        }
    }

    /**
     * Stops all the registered threads.
     */
    public static void dequeueAndStopAll() {
        synchronized (lock) {
            if (daemons != null) {
                for (HadoopDaemon daemon : daemons)
                    daemon.interrupt();

                daemons = null;
            }
        }
    }
}