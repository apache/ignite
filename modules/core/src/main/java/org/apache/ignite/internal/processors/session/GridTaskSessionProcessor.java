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

package org.apache.ignite.internal.processors.session;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 */
public class GridTaskSessionProcessor extends GridProcessorAdapter {
    /** Sessions (initialized to 2K number of concurrent sessions). */
    private final ConcurrentMap<IgniteUuid, GridTaskSessionImpl> sesMap =
        new ConcurrentHashMap8<>(2048);

    /**
     * @param ctx Grid kernal context.
     */
    public GridTaskSessionProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Starts session processor.
     */
    @Override public void start() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Session processor started.");
    }

    /**
     * Stops session processor.
     */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Session processor stopped.");
    }

    /**
     * @param sesId Session ID.
     * @param taskNodeId Task node ID.
     * @param taskName Task name.
     * @param dep Deployment.
     * @param taskClsName Task class name.
     * @param top Topology.
     * @param startTime Execution start time.
     * @param endTime Execution end time.
     * @param siblings Collection of siblings.
     * @param attrs Map of attributes.
     * @param fullSup {@code True} to enable distributed session attributes
     *      and checkpoints.
     * @param subjId Subject ID.
     * @return New session if one did not exist, or existing one.
     */
    public GridTaskSessionImpl createTaskSession(
        IgniteUuid sesId,
        UUID taskNodeId,
        String taskName,
        @Nullable GridDeployment dep,
        String taskClsName,
        @Nullable Collection<UUID> top,
        long startTime,
        long endTime,
        Collection<ComputeJobSibling> siblings,
        Map<Object, Object> attrs,
        boolean fullSup,
        UUID subjId) {
        if (!fullSup) {
            return new GridTaskSessionImpl(
                taskNodeId,
                taskName,
                dep,
                taskClsName,
                sesId,
                top,
                startTime,
                endTime,
                siblings,
                attrs,
                ctx,
                false,
                subjId);
        }

        while (true) {
            GridTaskSessionImpl ses = sesMap.get(sesId);

            if (ses == null) {
                GridTaskSessionImpl old = sesMap.putIfAbsent(
                    sesId,
                    ses = new GridTaskSessionImpl(
                        taskNodeId,
                        taskName,
                        dep,
                        taskClsName,
                        sesId,
                        top,
                        startTime,
                        endTime,
                        siblings,
                        attrs,
                        ctx,
                        true,
                        subjId));

                if (old != null)
                    ses = old;
                else
                    // Return without acquire.
                    return ses;
            }

            if (ses.acquire())
                return ses;
            else
                sesMap.remove(sesId, ses);
        }
    }

    /**
     * @param sesId Session ID.
     * @return Session for a given session ID.
     */
    @Nullable public GridTaskSessionImpl getSession(IgniteUuid sesId) {
        return sesMap.get(sesId);
    }

    /**
     * Removes session for a given session ID.
     *
     * @param sesId ID of session to remove.
     * @return {@code True} if session was removed.
     */
    public boolean removeSession(IgniteUuid sesId) {
        GridTaskSessionImpl ses = sesMap.get(sesId);

        assert ses == null || ses.isFullSupport();

        if (ses != null && ses.release()) {
            sesMap.remove(sesId, ses);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Task session processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>  sesMapSize: " + sesMap.size());
    }
}