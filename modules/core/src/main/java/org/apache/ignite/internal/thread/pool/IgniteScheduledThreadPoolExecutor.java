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

package org.apache.ignite.internal.thread.pool;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.concurrent.OperationContextAwareScheduledExecutorService;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UNDEFINED;

/**
 * Ignite specific wrapper over {@link ScheduledThreadPoolExecutor} that supports {@link OperationContext} propagation
 * and automatically uses custom Ignite {@link IgniteThreadFactory}.
 *
 * @see OperationContext
 * @see OperationContextAwareScheduledExecutorService
 */
public class IgniteScheduledThreadPoolExecutor extends OperationContextAwareScheduledExecutorService {
    /**
     * @param threadNamePrefix Pool thread name prefix.
     * @param igniteInstanceName Ignite instance name.
     * @param poolSize Pool size.
     */
    public IgniteScheduledThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int poolSize) {
        super(new ScheduledThreadPoolExecutor(
            poolSize,
            new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, UNDEFINED, null))
        );
    }

    /**
     * @param threadNamePrefix Pool thread name prefix.
     * @param igniteInstanceName Ignite instance name.
     * @return Thread pool instance.
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String threadNamePrefix, @Nullable String igniteInstanceName) {
        return new IgniteScheduledThreadPoolExecutor(threadNamePrefix, igniteInstanceName, 1);
    }
}
