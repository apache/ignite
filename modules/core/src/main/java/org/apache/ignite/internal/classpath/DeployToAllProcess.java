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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.createRootAndCheckIsEmpty;
import static org.apache.ignite.internal.classpath.ClassPathProcessor.fromMetastorage;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CLASSPATH_DEPLOY_TO_ALL;

/** Distributed process to spread {@link IgniteClassPath} files across cluster. */
class DeployToAllProcess {
    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Files handler. */
    private final ClassPathFilesTransmissionHandler icpFilesHnd;

    /** Distribute process that distributes new Ignite class path across all server nodes. */
    private final DistributedProcess<ClassPathDeployToAllRequest, ClassPathDeployToAllResponse> deployToAllProc;

    /** Future results of started distributed process. */
    private final Map<UUID, GridFutureAdapter<String>> futs = new ConcurrentHashMap<>();

    /** */
    public DeployToAllProcess(GridKernalContext ctx, ClassPathFilesTransmissionHandler icpFilesHnd) {
        log = ctx.log(DeployToAllProcess.class);

        this.ctx = ctx;
        this.icpFilesHnd = icpFilesHnd;
        this.deployToAllProc = new DistributedProcess<>(
            ctx,
            CLASSPATH_DEPLOY_TO_ALL,
            this::downloadLocally,
            this::processDeployToAllResult
        );
    }

    /**
     * @param icpId ClassPath ID.
     * @return Future for deploy process result.
     */
    public IgniteInternalFuture<?> start(UUID icpId) {
        boolean added = false;

        try {
            GridFutureAdapter<String> deployRes = new GridFutureAdapter<>();

            synchronized (this) {
                IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

                ClassPathDeployToAllRequest req = new ClassPathDeployToAllRequest(icpId, ctx.localNodeId());

                added = futs.putIfAbsent(icpId, deployRes) == null;

                if (!added)
                    return new GridFinishedFuture<>(new IllegalStateException("Deploy to all process started, already: " + icp.name()));

                deployToAllProc.start(icpId, req);
            }

            return deployRes;
        }
        catch (Exception e) {
            if (added)
                futs.remove(icpId);

            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<ClassPathDeployToAllResponse> downloadLocally(ClassPathDeployToAllRequest req) {
        try {
            if (req.uploadNodeId.equals(ctx.localNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Skip download ClassPath files for upload node [id=" + req.icpId + ']');

                return new GridFinishedFuture<>();
            }

            IgniteClassPath icp = fromMetastorage(req.icpId, NEW, ctx);

            createRootAndCheckIsEmpty(ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name()));

            return icpFilesHnd.downloadLocally(req.uploadNodeId, icp).chain(f -> {
                if (f.error() == null)
                    return new ClassPathDeployToAllResponse(icp.id());

                NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

                U.delete(ft.classPathRoot(icp.name()));

                throw new GridClosureException(f.error());
            });
        }
        catch (Throwable e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processDeployToAllResult(
        UUID id,
        Map<UUID, ClassPathDeployToAllResponse> res,
        Map<UUID, Throwable> err
    ) {
        GridFutureAdapter<String> fut = futs.remove(id);

        // Only upload node manage the process.
        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Unknown distribute process [id=" + id + ']');

            return;
        }

        try {
            IgniteClassPath icp = fromMetastorage(id, NEW, ctx);

            if (log.isDebugEnabled())
                log.debug("Starting CAS to metastorage: " + icp);

            // Perform CAS async to release discovery thread and let CAS proceed.
            ctx.classPath().casToMetastorageAsync(icp, icp.newState(IgniteClassPathState.READY)).listen(casFut -> {
                log.info("ClassPath is READY. " + res.size() + " of " + (res.size() + err.size()) +
                    " nodes has its files");

                if (!F.isEmpty(res)) {
                    log.info("Node that successfully download ClassPath files:");
                    res.forEach((nodeId, resp) -> log.info("  ^-- " + nodeId));
                }

                if (!F.isEmpty(err)) {
                    log.info("Node that fail to download ClassPath file (will retry on first usage):");
                    err.forEach((nodeId, t) -> log.info("  ^-- " + nodeId + ": " + t.getMessage()));
                }

                boolean metastorageWritten = casFut.error() == null && casFut.result() != null && casFut.result();

                Throwable t = metastorageWritten
                    ?  null
                    : casFut.error() != null
                        ? casFut.error()
                        : new IgniteException("Fail to change ClassPath state. Concurrent removal?");

                if (!fut.onDone(metastorageWritten ? "OK" : null, t)) {
                    log.warning("Distribute process in wrong state " +
                        "[canceled=" + fut.isCancelled() + ", failed=" + fut.isFailed() + ", done=" + fut.isDone() + ']');
                }
            });
        }
        catch (Exception e) {
            fut.onDone(e);
        }
    }
}
