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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Constructs new thread factory for given grid. All threads will belong
     * to the same default thread group.
     *
     * @param igniteInstanceName Ignite instance name.
     */
    public IgniteThreadFactory(String igniteInstanceName) {
        this(igniteInstanceName, "ignite");
    }

    /**
     * Constructs new thread factory for given grid. All threads will belong
     * to the same default thread group.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param threadName Thread name.
     */
    public IgniteThreadFactory(String igniteInstanceName, String threadName) {
        this.igniteInstanceName = igniteInstanceName;
        this.threadName = threadName;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        return new IgniteThread(igniteInstanceName, threadName, r, idxGen.incrementAndGet(), -1);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThreadFactory.class, this, super.toString());
    }
}
