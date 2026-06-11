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

package org.apache.ignite.internal.classpath;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.fromMetastorage;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CLASSPATH_DEPLOY_TO_ALL;

/** Distributed process to spread {@link IgniteClassPath} files across cluster. */
class DeployToAllProcess {
    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final ClassPathFilesTransmissionHandler icpFilesHnd;

    /** Distribute process that distributes new Ignite class path across all server nodes. */
    private final DistributedProcess<ClassPathDeployToAllRequest, ClassPathDeployToAllResponse> deployToAllProc;

    /** */
    private final Map<UUID, GridFutureAdapter<String>> futs = new ConcurrentHashMap<>();

    /** */
    public DeployToAllProcess(GridKernalContext ctx, ClassPathFilesTransmissionHandler icpFilesHnd) {
        log = ctx.log(DeployToAllProcess.class);

        this.ctx = ctx;
        this.icpFilesHnd = icpFilesHnd;

        deployToAllProc = new DistributedProcess<>(
            ctx,
            CLASSPATH_DEPLOY_TO_ALL,
            this::startDeployToAllProcess,
            this::processDeployToAllResult
        );
    }

    /**
     * @param icpId ClassPath ID.
     * @return Future for deploy process result.
     */
    public IgniteInternalFuture<?> start(UUID icpId) {
        GridFutureAdapter<String> deployRes = new GridFutureAdapter<>();

        synchronized (this) {
            IgniteClassPath icp = fromMetastorage(icpId, ctx);

            ClassPathDeployToAllRequest req = new ClassPathDeployToAllRequest(icpId, ctx.localNodeId());

            if (futs.put(icpId, deployRes) != null)
                return new GridFinishedFuture<>(new IllegalStateException("Deploy to all process started, already: " + icp.name()));

            deployToAllProc.start(icpId, req);
        }

        return deployRes;
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<ClassPathDeployToAllResponse> startDeployToAllProcess(ClassPathDeployToAllRequest req) {
        return icpFilesHnd.downloadLocally(req.icpId, req.uploadNodeId).chain(f -> {
            if (f.error() != null)
                throw new GridClosureException(f.error());

            return new ClassPathDeployToAllResponse(req.icpId);
        });
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processDeployToAllResult(UUID id, Map<UUID, ClassPathDeployToAllResponse> res, Map<UUID, Throwable> err) {
        GridFutureAdapter<String> fut = futs.remove(id);

        // Only upload node manage the process.
        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Unknown distribute process [id=" + id + ']');

            return;
        }

        IgniteClassPath icp = fromMetastorage(id, ctx);

        if (!fut.onDone("OK")) {
            log.warning("Distribute process in wrong state " +
                "[canceled=" + fut.isCancelled() + ", failed=" + fut.isFailed() + ", done=" + fut.isDone() + ']');

            return;
        }

        icp.state(IgniteClassPathState.READY);

        log.info("Deploy to all DONE!");
    }
}
