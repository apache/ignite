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

package org.apache.ignite.internal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.streamer.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Extended streamer context with methods intended for internal use.
 */
public interface IgniteStreamerEx extends IgniteStreamer {
    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext();

    /**
     * Gets streamer default window (the first one in configuration list).
     *
     * @return Streamer window.
     */
    public <E> StreamerWindow<E> window();

    /**
     * Gets streamer window by window name.
     *
     * @param windowName Window name.
     * @return Streamer window.
     */
    @Nullable public <E> StreamerWindow<E> window(String windowName);

    /**
     * Called before execution requests are sent to remote nodes or scheduled for local execution.
     *
     * @param fut Future.
     */
    public void onFutureMapped(GridStreamerStageExecutionFuture fut);

    /**
     * Called when future is completed and parent should be notified, if any.
     *
     * @param fut Future.
     */
    public void onFutureCompleted(GridStreamerStageExecutionFuture fut);

    /**
     * @return Streamer event router.
     */
    public StreamerEventRouter eventRouter();

    /**
     * Schedules batch executions either on local or on remote nodes.
     *
     * @param fut Future.
     * @param execs Executions grouped by node ID.
     * @throws IgniteCheckedException If failed.
     */
    public void scheduleExecutions(GridStreamerStageExecutionFuture fut, Map<UUID, GridStreamerExecutionBatch> execs)
        throws IgniteCheckedException;

    /**
     * Callback for undeployed class loaders. All deployed events will be removed from window and local storage.
     *
     * @param undeployedLdr Undeployed class loader.
     */
    public void onUndeploy(ClassLoader undeployedLdr);

    /**
     * Callback executed when streamer query completes.
     *
     * @param time Consumed time.
     * @param nodes Participating nodes count.
     */
    public void onQueryCompleted(long time, int nodes);
}
