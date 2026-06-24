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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.fromMetastorage;
import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;

/**
 * Removes {@link IgniteClassPath} metastorage record and deletes its local files.
 */
class CleanupTask extends ClassPathProcessor.ClassPathTask<Void> {
    /** If {@code true} then remove only local data. */
    private final boolean loc;

    /** */
    private CleanupTask(GridKernalContext ctx, UUID icpId, boolean loc) {
        super(ctx, icpId);
        this.loc = loc;
    }

    /** @return Task to clean up local files. */
    public static CleanupTask localCleanup(GridKernalContext ctx, UUID icpId) {
        return new CleanupTask(ctx, icpId, true);
    }

    /** @return Task to clean up both local files and metastorage record. */
    public static CleanupTask clusterWideCleanup(GridKernalContext ctx, UUID icpId) {
        return new CleanupTask(ctx, icpId, false);
    }

    /** {@inheritDoc} */
    @Override void start() {
        IgniteClassPath icp = fromMetastorage(icpId, null, ctx);

        String key = metastorageKey(icp.name());

        if (!loc) {
            boolean rmvd = false;

            while (!rmvd) {
                try {
                    rmvd = ctx.distributedMetastorage().compareAndRemove(key, icp) || ctx.distributedMetastorage().read(key) == null;

                    if (!rmvd)
                        icp = fromMetastorage(icpId, null, ctx);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to remove ClassPath metastorage record [name=" + icp.name() + ']', e);
                }
            }
        }

        ctx.classPath().removeClassPathLocally(ctx.pdsFolderResolver().fileTree().classPathRoot(icp.name()));

        result().onDone();
    }

    /** {@inheritDoc} */
    @Override String name() {
        return (loc ? "local " : "") + "cleanup";
    }

    /** {@inheritDoc} */
    @Override void ok() {
        if (log.isDebugEnabled())
            log.debug("ClassPath cleanup done [icpId=" + icpId + ", loc=" + loc + ']');
    }

    /** {@inheritDoc} */
    @Override void fail(Throwable t) {
        log.warning("Fail to cleanup ClassPath [icpId=" + icpId + ", loc=" + loc + ']', t);
    }
}
