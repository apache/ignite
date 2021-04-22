/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask;

import java.io.Serializable;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;

/**
 * Durable task that should be used to do long operations (e.g. index deletion) in background
 * for cases when node with persistence can fail before operation is completed. After start, node reads it's
 * pending background tasks from metastorage and completes them.
 */
public interface DurableBackgroundTask extends Serializable {
    /**
     * Getting a short name for a task to identify it.
     * Also used as part of a key for storage in a MetaStorage.
     *
     * @return Short name of the task.
     */
    String shortName();

    /**
     * Checks if the task has completed or not.
     * If the task is completed, then it will not start and will be deleted from the MetaStorage.
     *
     * @return {@code True} if the task is complete.
     */
    boolean completed();

    /**
     * Checks if the task has started.
     * Avoids running the task twice.
     *
     * @return {@code True} if started.
     */
    boolean started();

    /**
     * Cancel task execution.
     */
    void cancel();

    /**
     * Asynchronous execution of a task.
     *
     * @param ctx Kernal context.
     * @return Future that completes when a task is completed.
     */
    IgniteInternalFuture<?> executeAsync(GridKernalContext ctx);
}
