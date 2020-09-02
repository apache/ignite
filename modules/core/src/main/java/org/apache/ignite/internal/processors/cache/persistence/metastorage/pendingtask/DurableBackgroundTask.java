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

/**
 * Durable task that should be used to do long operations (e.g. index deletion) in background
 * for cases when node with persistence can fail before operation is completed. After start, node reads it's
 * pending background tasks from metastorage and completes them.
 */
public interface DurableBackgroundTask extends Serializable {
    /**
     * Short unique name of the task is used to build metastorage key for saving this task and for logging.
     *
     * @return Short name of this task.
     */
    public String shortName();

    /**
     * Method that executes the task. It is called after node start.
     *
     * @param ctx Grid kernal context.
     */
    public void execute(GridKernalContext ctx);

    /**
     * Method that marks task as complete.
     */
    public void complete();

    /**
     * Method that return completion flag.
     *
     * @return flag that task completed.
     */
    public boolean isCompleted();

    /**
     * Callback for task cancellation.
     */
    public void onCancel();
}
