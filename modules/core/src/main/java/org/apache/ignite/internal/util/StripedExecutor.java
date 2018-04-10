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

package org.apache.ignite.internal.util;

import java.util.concurrent.ExecutorService;

/**
 * Striped executor interface.
 */
public interface StripedExecutor extends ExecutorService {
    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     */
    void checkStarvation();

    /**
     * @return Stripes count.
     */
    int stripes();

    /**
     * Execute command.
     *
     * @param idx Index.
     * @param cmd Command.
     */
    void execute(int idx, Runnable cmd);

    /**
     * Stops executor.
     */
    void stop();

    /**
     * @return Return total queue size of all stripes.
     */
    int queueSize();

    /**
     * @return Completed tasks count.
     */
    long completedTasks();

    /**
     * @return Completed tasks per stripe count.
     */
    long[] stripesCompletedTasks();

    /**
     * @return Number of active tasks per stripe.
     */
    boolean[] stripesActiveStatuses();

    /**
     * @return Number of active tasks.
     */
    int activeStripesCount();

    /**
     * @return Size of queue per stripe.
     */
    int[] stripesQueueSizes();
}
