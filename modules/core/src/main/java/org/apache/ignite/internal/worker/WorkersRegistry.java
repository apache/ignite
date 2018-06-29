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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerDiedException;
import org.apache.ignite.internal.util.worker.GridWorkerFailureException;
import org.apache.ignite.internal.util.worker.GridWorkerIdlenessHandler;
import org.apache.ignite.internal.util.worker.GridWorkerIsHangingException;
import org.apache.ignite.internal.util.worker.GridWorkerListener;

/**
 * Workers registry.
 */
public class WorkersRegistry implements GridWorkerListener, GridWorkerIdlenessHandler {
    /** */
    private static final int NO_OF_WORKERS_TO_CHECK_AT_ONCE = 5;

    /** */
    private static final long CHECK_INTERVAL_MS = 3_000;

    /** Registered workers. */
    private final ConcurrentMap<String, GridWorker> registeredWorkers = new ConcurrentHashMap<>();

    /** Whether workers should check peers' health or not. */
    private volatile boolean isPeerCheckEnabled = true;

    /** Points to the next worker to check. */
    private volatile Iterator<Map.Entry<String, GridWorker>> checkIter = registeredWorkers.entrySet().iterator();

    /** */
    private volatile long lastCheckStartTimestamp = System.currentTimeMillis();

    /** Last thread that performed the check. Null reference denotes "checking is in progress". */
    private AtomicReference<Thread> lastChecker = new AtomicReference<>(Thread.currentThread());

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
    public boolean getPeerCheckEnabled() {
        return isPeerCheckEnabled;
    }

    /** */
    public void setPeerCheckEnabled(boolean value) {
        isPeerCheckEnabled = value;
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
    @Override public void onIdle(GridWorker w) throws GridWorkerFailureException {
        if (!isPeerCheckEnabled)
            return;

        Thread prevCheckerThread = lastChecker.get();

        if (prevCheckerThread == null ||
            System.currentTimeMillis() - lastCheckStartTimestamp <= CHECK_INTERVAL_MS ||
            !lastChecker.compareAndSet(prevCheckerThread, null))
            return;

        try {
            lastCheckStartTimestamp = System.currentTimeMillis();

            for (int i = 0; i < NO_OF_WORKERS_TO_CHECK_AT_ONCE; i++) {
                if (!checkIter.hasNext()) {
                    checkIter = registeredWorkers.entrySet().iterator();

                    if (!checkIter.hasNext())
                        return;
                }

                GridWorker worker = checkIter.next().getValue();

                Thread runner = worker.runner();

                if (runner != null && runner != Thread.currentThread()) {
                    if (!runner.isAlive()) {
                        // In normal operation GridWorker implementation guarantees:
                        // worker termination happens before its removal from registeredWorkers.
                        // That is, if worker is dead, but still resides in registeredWorkers
                        // then something went wrong, the only extra thing is to test
                        // whether the iterator refers to actual state of registeredWorkers.
                        GridWorker workerAgain = registeredWorkers.get(worker.runner().getName());

                        if (workerAgain != null && workerAgain == worker)
                            throw new GridWorkerDiedException(worker);

                        checkIter = registeredWorkers.entrySet().iterator();
                    }

                    if (System.currentTimeMillis() - worker.heartbeatTimeMillis() > worker.criticalHeartbeatTimeoutMs()) {
                        GridWorker workerAgain = registeredWorkers.get(worker.runner().getName());

                        if (workerAgain != null && workerAgain == worker)
                            throw new GridWorkerIsHangingException(worker);

                        checkIter = registeredWorkers.entrySet().iterator();
                    }
                }
            }
        }
        finally {
            lastChecker.set(Thread.currentThread());
        }
    }
}
