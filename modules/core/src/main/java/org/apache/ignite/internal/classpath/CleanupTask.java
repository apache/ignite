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
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;

/**
 * Removes {@link IgniteClassPath} metastorage record and deletes its local files.
 */
class CleanupTask extends ClassPathProcessor.ClassPathTask<Void> {
    /** */
    private final IgniteClassPath icp;

    /** If {@code true} then remove only local data. */
    private final boolean loc;

    /** */
    private CleanupTask(GridKernalContext ctx, IgniteClassPath icp, boolean loc) {
        super(ctx, icp.id());
        this.icp = icp;
        this.loc = loc;
    }

    /** @return Task to clean up local files. */
    public static CleanupTask localCleanup(GridKernalContext ctx, IgniteClassPath icp) {
        return new CleanupTask(ctx, icp, true);
    }

    /** @return Task to clean up both local files and metastorage record. */
    public static CleanupTask clusterWideCleanup(GridKernalContext ctx, IgniteClassPath icp) {
        return new CleanupTask(ctx, icp, false);
    }

    /** {@inheritDoc} */
    @Override void start() {
        if (!loc)
            removeFromMetastorage();

        ctx.classPath().removeClassPathLocally(icp, false);

        result().onDone();
    }

    /** */
    private void removeFromMetastorage() {
        try {
            String key = metastorageKey(icp.name());

            int iter = 0;

            while (true) {
                Serializable curData = ctx.distributedMetastorage().read(key);

                if (curData == null || !ClassPathProcessor.isClassPath(curData) || !Objects.equals(((IgniteClassPath)curData).id(), icp.id()))
                    break;

                if (ctx.distributedMetastorage().compareAndRemove(key, curData))
                    break;

                iter++;

                if (iter == 500)
                    throw new IgniteException("Too many iterations");

                if (iter % 100 == 0)
                    log.warning("Remove operation makes too many iterations. Bug? [icp=" + icp + ", curData=" + curData + ']');

            }
        }
        catch (IgniteCheckedException e) {
            result().onDone(e);
        }
    }

    /** {@inheritDoc} */
    @Override String name() {
        return (loc ? "local " : "") + "cleanup";
    }

    /** {@inheritDoc} */
    @Override void ok() {
        if (log.isDebugEnabled())
            log.debug("ClassPath cleanup done [icp=" + icp + ", loc=" + loc + ']');
    }

    /** {@inheritDoc} */
    @Override void fail(Throwable t) {
        log.warning("Fail to cleanup ClassPath [icp=" + icp + ", loc=" + loc + ']', t);
    }
}
