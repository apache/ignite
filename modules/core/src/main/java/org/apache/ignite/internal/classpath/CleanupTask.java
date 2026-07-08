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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Removes {@link IgniteClassPath} metastorage record and deletes its local files.
 */
class CleanupTask extends ClassPathProcessor.ClassPathTask<Void> {
    /** */
    private final IgniteClassPath icp;

    /** If {@code true} then remove from metastore. */
    private final boolean rmvFromMetastore;

    /** */
    private CleanupTask(GridKernalContext ctx, IgniteClassPath icp, boolean rmvFromMetastore) {
        super(ctx, icp.id());
        this.icp = icp;
        this.rmvFromMetastore = rmvFromMetastore;
    }

    /** @return Task to clean up local files. */
    public static CleanupTask localCleanup(GridKernalContext ctx, IgniteClassPath icp) {
        return new CleanupTask(ctx, icp, false);
    }

    /** @return Task to clean up both local files and metastorage record. */
    public static CleanupTask clusterWideCleanup(GridKernalContext ctx, IgniteClassPath icp) {
        return new CleanupTask(ctx, icp, true);
    }

    /** {@inheritDoc} */
    @Override void start0() {
        if (rmvFromMetastore) {
            try {
                ctx.classPath().removeFromMetastorage(icp);
            }
            catch (IgniteCheckedException e) {
                result().onDone(e);
            }
        }

        ctx.classPath().removeClassPathLocally(icp, false);

        result().onDone();
    }

    /** {@inheritDoc} */
    @Override String name() {
        return (rmvFromMetastore ? "local " : "") + "cleanup";
    }

    /** {@inheritDoc} */
    @Override void ok() {
        if (log.isDebugEnabled())
            log.debug("ClassPath cleanup done [icp=" + icp + ", loc=" + rmvFromMetastore + ']');
    }

    /** {@inheritDoc} */
    @Override void fail(Throwable t) {
        log.warning("Fail to cleanup ClassPath [icp=" + icp + ", loc=" + rmvFromMetastore + ']', t);
    }
}
