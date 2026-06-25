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
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;

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
            log.warning("IgniteClassPath removed. Remove operation not supported at a time");

            return;
        }

        if (newIcp.deployedOnNodes().isEmpty() && (newIcp.state() == NEW || newIcp.state() == READY)) {
            log.warning("All nodes that haves IgniteClassPath files left the grid [icp=" + newIcp.name() + ']');

            ctx.classPath().addClassPathTask(newIcp, new ChangeStateTask(ctx, newIcp.id(), LOST));

            return;
        }

        if (newIcp.equalsWithoutNodes(oldIcp)) {
            if (log.isDebugEnabled())
                log.debug("Event ignored. Old and new state equals [old=" + oldIcp + ", new = " + newIcp + ']');

            return;
        }

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
                log.info("IgniteClassPath lost. Handle of LOST state not supported at a time");

                break;

            case REMOVING:
                log.info("IgniteClassPath removed. Remove operation not supported at a time");

                break;

            default:
                throw new IllegalArgumentException("Unknown IgniteClassPath state: " + newIcp.state());
        }
    }
}
