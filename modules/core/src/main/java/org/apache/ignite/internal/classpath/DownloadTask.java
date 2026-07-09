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

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.fromMetastorage;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;

/**
 * Download {@link IgniteClassPath} files to local node from random node that has files, already.
 *
 * @see ClassPathFilesTransmissionHandler
 */
class DownloadTask extends ClassPathProcessor.ClassPathTask<Void> {
    /**
     * @param ctx Kernal context.
     * @param icpId Ignite class path.
     */
    protected DownloadTask(GridKernalContext ctx, UUID icpId) {
        super(ctx, icpId);
    }

    /** {@inheritDoc} */
    @Override public void start0() {
        IgniteClassPath icp = fromMetastorage(icpId, READY, ctx);

        if (icp.deployedOnNodes().contains(ctx.localNodeId())) {
            if (log.isDebugEnabled())
                log.debug("Skip download ClassPath files. Node has files, already [icp=" + icp.name() + ']');

            result().onDone();

            return;
        }

        if (icp.deployedOnNodes().isEmpty()) {
            result().onDone(new IgniteException("Deployed on nodes empty. Can't download files: " + icpId));

            return;
        }

        ctx.classPath().createRootAndCheckIsEmpty(icp);

        UUID rmtNode = F.rand(icp.deployedOnNodes());

        if (stopped) {
            result().onDone(new IgniteException("Download task stopped"));

            return;
        }

        IgniteInternalFuture<Void> downloadRes = ctx.classPath().icpFilesHnd.downloadLocally(rmtNode, icp, this::stopped);

        downloadRes.listen(f -> {
            if (f.error() != null) {
                result().onDone(f.error());

                return;
            }

            if (log.isDebugEnabled())
                log.debug("ClassPath files from remote node has been fully received [icp=" + icp.name() + ']');

            ctx.classPath()
                .modifyInMetastorageAsync(icpId, READY, state -> state.addDeployedOnNode(ctx.localNodeId()), this::stopped)
                .listen(this::finishTaskWithFutureResult);
        });
    }

    /** {@inheritDoc} */
    @Override public void ok() {
        if (log.isDebugEnabled())
            log.debug("ClassPath files downloaded [icpId=" + icpId + ']');
    }

    /** {@inheritDoc} */
    @Override public void fail(Throwable t) {
        try {
            IgniteClassPath icp = fromMetastorage(icpId, READY, ctx);

            log.warning("Failed to download ClassPath files [icp=" + icp.name() + "]", t);

            // Cleanup local files.
            ctx.classPath().addClassPathTask(icp, CleanupTask.removeFiles(ctx, icp));
        }
        catch (Exception e) {
            log.warning("onDowloadFailed", e);
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "DownloadTask";
    }
}
