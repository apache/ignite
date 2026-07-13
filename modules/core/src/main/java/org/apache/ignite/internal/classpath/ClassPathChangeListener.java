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

import java.io.Serializable;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.classpath.ClassPathProcessor.ClassPathTask;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.className;
import static org.apache.ignite.internal.classpath.ClassPathProcessor.isClassPath;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.LOST;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;

/**
 * Listener of {@link DistributedMetaStorage} updates related to {@link IgniteClassPath} instances.
 * Starts {@link ClassPathTask} to handle {@link IgniteClassPath} changes on local node.
 *
 * @see ClassPathProcessor
 */
class ClassPathChangeListener implements DistributedMetaStorageListener<Serializable> {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /**
     * @param ctx Grid kernal context.
     */
    public ClassPathChangeListener(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(ClassPathChangeListener.class);
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@NotNull String key, @Nullable Serializable oldVal, @Nullable Serializable newVal) {
        if (!isClassPath(oldVal) || !isClassPath(newVal)) {
            log.warning("Wrong data in IgniteClassPath metastorage data " +
                "[key=" + key + ", oldVal=" + className(oldVal) + ", newVal=" + className(newVal) + ']');

            return;
        }

        IgniteClassPath oldIcp = (IgniteClassPath)oldVal;
        IgniteClassPath newIcp = (IgniteClassPath)newVal;

        if (newIcp == null) {
            onRemoveFromMetastorage(oldIcp);

            return;
        }

        if (newIcp.deployedOnNodes().isEmpty()) {
            onNoDeployNodes(newIcp);

            return;
        }

        if (newIcp.equalsWithoutNodes(oldIcp)) {
            if (log.isDebugEnabled())
                log.debug("Event ignored. Old and new state equals [old=" + oldIcp + ", new = " + newIcp + ']');

            return;
        }

        onChange(newIcp, oldIcp);
    }

    /**
     * Handles regular change in {@link IgniteClassPath}.
     */
    private void onChange(IgniteClassPath newIcp, IgniteClassPath oldIcp) {
        switch (newIcp.state()) {
            case NEW:
                log.info("IgniteClassPath created. Waiting for READY state to start download file to local node.");

                break;
            case READY:
                if (oldIcp == null || oldIcp.state() == NEW || oldIcp.state() == LOST) {
                    if (newIcp.deployedOnNodes().contains(ctx.localNodeId())) {
                        if (log.isDebugEnabled())
                            log.debug("Event ignored. IgniteClassPath deployed on node, already: " + newIcp);

                        return;
                    }

                    log.info("IgniteClassPath READY. Starting download to local node.");

                    ctx.classPath().addClassPathTask(newIcp, new DownloadTask(ctx, newIcp.id()));
                }
                else
                    log.warning("Wrong state change. Ignore [prev=" + oldIcp.state() + ", new=" + newIcp.state() + ']');

                break;

            case LOST:
                // No-op.

                break;

            case REMOVING:
                // Don't check anything. Download task may be in progress.
                // Cleanup will just raise log warning if local data not exists.
                log.info("IgniteClassPath remove operation started. Local cleanup: " + newIcp);

                ctx.classPath().addClassPathTask(newIcp, CleanupTask.removeFiles(ctx, newIcp));
                ctx.classPath().addClassPathTask(newIcp, ChangeNodesTask.removeNode(ctx, newIcp.id(), ctx.localNodeId()));

                break;

            default:
                throw new IllegalArgumentException("Unknown IgniteClassPath state: " + newIcp.state());
        }
    }

    /**
     * Handle {@link IgniteClassPath} event when it removed from metastorage.
     */
    private void onRemoveFromMetastorage(IgniteClassPath icp) {
        // Maybe remove with force flag or error while uploading.
        // Cancelling all in flight and remove right away.
        ctx.classPath().cancelTasksAndDisallowNew(icp.id(), icp.name(), false);

        if (!ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name()).exists()) {
            log.info("IgniteClassPath removed: " + icp);

            return;
        }

        // Concurrent class path tasks not run. Cleaning up without queue. Mostly harmless (no local data at the moment until force remove).
        CleanupTask t = CleanupTask.removeFiles(ctx, icp);

        t.result().listen(() -> log.info("IgniteClassPath removed: " + icp));

        ctx.classPath().startAsync(t);
    }

    /** Handle {@link IgniteClassPath} event when no nodes that stores ClassPath files in cluster. */
    private void onNoDeployNodes(IgniteClassPath icp) {
        assert icp.deployedOnNodes().isEmpty();

        switch (icp.state()) {
            case READY:
            case NEW:
                log.warning("All nodes that haves IgniteClassPath files left the grid [icp=" + icp.name() + ']');

                ctx.classPath().addClassPathTask(icp, new ChangeStateTask(ctx, icp.id(), LOST));

                break;

            case REMOVING:
                ctx.classPath().cancelTasksAndDisallowNew(icp.id(), icp.name(), true);

                // Remove from metastorage after all nodes cleanup.
                ctx.classPath().startAsync(CleanupTask.removeFromMetastore(ctx, icp));

                break;
            case LOST:
                // No-op.

                break;

            default:
                throw new IllegalStateException("Unknown state: " + icp.state());
        }
    }
}
