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

package org.apache.ignite.internal.worker;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_BLOCKED;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Workers registry. Maintains a set of workers currently running.
 * Can perform periodic liveness checks for these workers on behalf of any of them.
 */
public class WorkersRegistry implements GridWorkerListener {
    /** */
    private static final long DFLT_CHECK_INTERVAL = 3_000;

    /** Registered workers. */
    private final ConcurrentMap<String, GridWorker> registeredWorkers = new ConcurrentHashMap<>();

    /** Whether workers' liveness checking enabled or not. */
    private volatile boolean livenessCheckEnabled = true;

    /** Points to the next worker to check. */
    private volatile Iterator<Map.Entry<String, GridWorker>> checkIter = registeredWorkers.entrySet().iterator();

    /** It's safe to omit 'volatile' due to memory effects of lastChecker. */
    private long lastCheckTs = U.currentTimeMillis();

    /** Last thread that performed the check. Null reference denotes "checking is in progress". */
    private final AtomicReference<Thread> lastChecker = new AtomicReference<>(Thread.currentThread());

    /** */
    private final IgniteBiInClosure<GridWorker, FailureType> workerFailedHnd;

    /** Worker heartbeat timeout in milliseconds, when exceeded, worker is considered as blocked. */
    private final long heartbeatTimeout;

    /** Time in milliseconds between successive workers checks. */
    private final long checkInterval;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param workerFailedHnd Closure to invoke on worker failure.
     * @param heartbeatTimeout Maximum allowed worker heartbeat interval in milliseconds, should be positive.
     */
    public WorkersRegistry(
        @NotNull IgniteBiInClosure<GridWorker, FailureType> workerFailedHnd,
        long heartbeatTimeout,
        IgniteLogger log) {
        this.workerFailedHnd = workerFailedHnd;
        this.heartbeatTimeout = heartbeatTimeout;
        this.checkInterval = Math.min(DFLT_CHECK_INTERVAL, heartbeatTimeout);
        this.log = log;
    }

    /**
     * Adds worker to the registry.
     *
     * @param w Worker.
     */
    public void register(GridWorker w) {
        if (registeredWorkers.putIfAbsent(w.runner().getName(), w) != null)
            throw new IllegalStateException("Worker is already registered [worker=" + w + ']');

        checkIter = registeredWorkers.entrySet().iterator();
    }

    /**
     * Removes worker from the registry.
     *
     * @param name Worker name.
     */
    public void unregister(String name) {
        registeredWorkers.remove(name);

        checkIter = registeredWorkers.entrySet().iterator();
    }

    /**
     * Returns names of all registered workers.
     *
     * @return Registered worker names.
     */
    public Collection<String> names() {
        return registeredWorkers.keySet();
    }

    /**
     * Returns worker with given name.
     *
     * @param name Name.
     * @return Registered {@link GridWorker} with name {@code name} or {@code null} if not found.
     */
    public GridWorker worker(String name) {
        return registeredWorkers.get(name);
    }

    /** */
    boolean livenessCheckEnabled() {
        return livenessCheckEnabled;
    }

    /** */
    void livenessCheckEnabled(boolean val) {
        livenessCheckEnabled = val;
    }

    /** {@inheritDoc} */
    @Override public void onStarted(GridWorker w) {
        register(w);
    }

    /** {@inheritDoc} */
    @Override public void onStopped(GridWorker w) {
        unregister(w.runner().getName());
    }

    /** {@inheritDoc} */
    @Override public void onIdle(GridWorker w) {
        if (!livenessCheckEnabled)
            return;

        Thread prevCheckerThread = lastChecker.get();

        if (prevCheckerThread == null ||
            U.currentTimeMillis() - lastCheckTs <= checkInterval ||
            !lastChecker.compareAndSet(prevCheckerThread, null))
            return;

        try {
            lastCheckTs = U.currentTimeMillis();

            long workersToCheck = Math.max(registeredWorkers.size() * checkInterval / heartbeatTimeout, 1);

            int workersChecked = 0;

            while (workersChecked < workersToCheck) {
                if (!checkIter.hasNext())
                    checkIter = registeredWorkers.entrySet().iterator();

                GridWorker worker;

                try {
                    worker = checkIter.next().getValue();
                }
                catch (NoSuchElementException e) {
                    return;
                }

                Thread runner = worker.runner();

                if (runner != null && runner != Thread.currentThread() && !worker.isCancelled()) {
                    if (!runner.isAlive()) {
                        // In normal operation GridWorker implementation guarantees:
                        // worker termination happens before its removal from registeredWorkers.
                        // That is, if worker is dead, but still resides in registeredWorkers
                        // then something went wrong, the only extra thing is to test
                        // whether the iterator refers to actual state of registeredWorkers.
                        GridWorker worker0 = registeredWorkers.get(worker.runner().getName());

                        if (worker0 != null && worker0 == worker)
                            workerFailedHnd.apply(worker, SYSTEM_WORKER_TERMINATION);
                    }

                    long heartbeatDelay = U.currentTimeMillis() - worker.heartbeatTs();

                    if (heartbeatDelay > heartbeatTimeout) {
                        GridWorker worker0 = registeredWorkers.get(worker.runner().getName());

                        if (worker0 != null && worker0 == worker) {
                            log.error("Blocked system-critical thread has been detected. " +
                                "This can lead to cluster-wide undefined behaviour " +
                                "[threadName=" + worker.name() + ", blockedFor=" + heartbeatDelay / 1000 + "s]");

                            U.dumpThread(worker.runner(), log);

                            workerFailedHnd.apply(worker, SYSTEM_WORKER_BLOCKED);
                        }

                        // Iterator should not be reset:
                        // otherwise we'll never iterate beyond the blocked worker,
                        // that may stay in the map for indefinite time.
                    }
                }

                if (runner != Thread.currentThread())
                    workersChecked++;
            }
        }
        finally {
            boolean set = lastChecker.compareAndSet(null, Thread.currentThread());

            assert set;
        }
    }
}
