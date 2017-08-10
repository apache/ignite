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

package org.apache.ignite.internal.processors.query.h2.twostep.lazy;

import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory for lazy mapper workers.
 */
public class MapQueryLazyWorkerFactory extends IgniteThreadFactory {
    /** Thread ID generator. */
    private static final AtomicLong THREAD_ID_GEN = new AtomicLong();

    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param threadName Thread name.
     */
    public MapQueryLazyWorkerFactory(String igniteInstanceName, String threadName) {
        super(igniteInstanceName, threadName, GridIoPolicy.QUERY_POOL);
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        return new MapQueryLazyIgniteThread(igniteInstanceName, threadName, r, THREAD_ID_GEN.incrementAndGet());
    }
}
