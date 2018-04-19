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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;

/**
 * Workers registry.
 */
public class WorkersRegistry implements GridWorkerListener {
    /** Registered workers. */
    private final ConcurrentMap<String, GridWorker> registeredWorkers = new ConcurrentHashMap<>();

    /**
     * Adds worker to the registry.
     *
     * @param w Worker.
     */
    public void register(GridWorker w) {
        if (registeredWorkers.putIfAbsent(w.runner().getName(), w) != null)
            throw new IllegalStateException("Worker is already registered [worker=" + w + ']');
    }

    /**
     * Removes worker from the registry.
     *
     * @param name Worker name.
     */
    public void unregister(String name) {
        registeredWorkers.remove(name);
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

    /** {@inheritDoc} */
    @Override public void onStarted(GridWorker w) {
        register(w);
    }

    /** {@inheritDoc} */
    @Override public void onStopped(GridWorker w) {
        unregister(w.runner().getName());
    }
}
