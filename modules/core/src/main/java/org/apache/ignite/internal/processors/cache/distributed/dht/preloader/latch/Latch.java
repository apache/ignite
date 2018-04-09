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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;

/**
 * Simple distributed count down latch interface.
 * Latch supports count down and await logic.
 * Latch functionality is not relied on caches and has own state management {@link ExchangeLatchManager}.
 */
public interface Latch {
    /**
     * Decrements count on current latch.
     * Release all latch waiters on all nodes if count reaches zero.
     *
     * This is idempotent operation. Invoking this method twice or more on the same node doesn't have any effect.
     */
    void countDown();

    /**
     * Awaits current latch completion.
     *
     * @throws IgniteCheckedException If await is failed.
     */
    void await() throws IgniteCheckedException;

    /**
     * Awaits current latch completion with specified timeout.
     *
     * @param timeout Timeout value.
     * @param timeUnit Timeout time unit.
     * @throws IgniteCheckedException If await is failed.
     */
    void await(long timeout, TimeUnit timeUnit) throws IgniteCheckedException;
}
