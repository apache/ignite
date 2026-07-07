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

package org.apache.ignite.internal.management.ssl;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.nio.ssl.SslContextReloadable;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** Task that reloads TLS certificates on every mapped node. */
@GridInternal
public class SslReloadTask extends VisorMultiNodeTask<NoArg, String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, String> job(NoArg arg) {
        return new SslReloadJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder res = new StringBuilder();

        for (ComputeJobResult jobRes : results) {
            if (jobRes.getException() != null)
                throw jobRes.getException();

            res.append(jobRes.getData().toString()).append('\n');
        }

        return res.toString();
    }

    /** Job that reloads TLS certificates on the local node. */
    private static class SslReloadJob extends VisorJob<NoArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected SslReloadJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(NoArg arg) throws IgniteException {
            GridKernalContext ctx = ignite.context();

            List<String> reloaded = new ArrayList<>();

            try {
                reload(ctx.config().getCommunicationSpi(), "communication", reloaded);
                reload(ctx.config().getDiscoverySpi(), "discovery", reloaded);
                reload(ctx.clientListener(), "client connector", reloaded);
                reload(ctx.rest(), "REST", reloaded);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to reload SSL certificates on node " +
                    ignite.localNode().id() + ": " + e.getMessage(), e);
            }

            return ignite.localNode().id() + ": " +
                (reloaded.isEmpty() ? "SSL is not configured, nothing to reload" : "reloaded " + reloaded);
        }

        /**
         * Reloads the SSL context of the given component if it supports hot reload.
         *
         * @param comp Component to reload.
         * @param name Human-readable component name for the result message.
         * @param out Collects names of the components that were actually reloaded.
         * @throws IgniteCheckedException If the reload failed.
         */
        private static void reload(Object comp, String name, List<String> out) throws IgniteCheckedException {
            if (comp instanceof SslContextReloadable && ((SslContextReloadable)comp).reloadSslContext())
                out.add(name);
        }
    }
}
