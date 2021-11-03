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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 *
 */
public interface QueryTaskExecutor extends LifecycleAware {
    /**
     * Executes a query task in a thread, responsible for particular query fragment.
     *
     * @param qryId      Query ID.
     * @param fragmentId Fragment ID.
     * @param qryTask    Query task.
     */
    void execute(UUID qryId, long fragmentId, Runnable qryTask);

    /**
     * Returns a new CompletableFuture that is asynchronously completed by a task running in the given executor after it runs the given
     * action.
     *
     * @param qryId      Id of the query this task created for.
     * @param fragmentId Id of the particular fragment this task created for.
     * @param qryTask    The task to submit.
     * @return the new CompletableFuture
     */
    CompletableFuture<?> submit(UUID qryId, long fragmentId, Runnable qryTask);
}
