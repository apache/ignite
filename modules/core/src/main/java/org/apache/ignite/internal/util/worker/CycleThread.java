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

package org.apache.ignite.internal.util.worker;

import org.jetbrains.annotations.NotNull;

/**
 * Thread wrapper for standart cycle operations.
 */
public abstract class CycleThread extends Thread {

    /** Sleep interval before each iteration. */
    private final long sleepInterval;

    /**
     * Creates new cycle thread with given parameters.
     *
     * @param name thread name
     * @param sleepInterval sleep interval before each iteration
     */
    protected CycleThread(@NotNull String name, long sleepInterval) {
        super(name);

        this.sleepInterval = sleepInterval;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public final void run() {
        try {
            while (!isInterrupted()) {
                Thread.sleep(sleepInterval);

                iteration();
            }
        } catch (InterruptedException e) {
            // No op
        }
    }

    /**
     * Called on each iteration.
     *
     * @throws InterruptedException Еhrows if no specific handling required.
     */
    public abstract void iteration() throws InterruptedException;
}
