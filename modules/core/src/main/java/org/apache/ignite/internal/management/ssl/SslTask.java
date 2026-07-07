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
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

/** Base of the {@code --ssl} tasks: every mapped node contributes one line, and a node that failed fails the task. */
public abstract class SslTask extends VisorMultiNodeTask<NoArg, String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder res = new StringBuilder();

        boolean failed = false;

        for (ComputeJobResult jobRes : results) {
            if (jobRes.getException() != null) {
                failed = true;

                res.append(jobRes.getException().getMessage());
            }
            else
                res.append(jobRes.getData().toString());

            res.append('\n');
        }

        // Every node is listed before the failure is raised: a partial reload leaves the cluster with mixed
        // certificates, so the operator has to see which nodes ended up on which side.
        if (failed)
            throw new IgniteException(res.toString());

        return res.toString();
    }

    /** Walks the SSL-enabled components of a node, either reloading their certificates or only checking them. */
    protected static class SslJob extends VisorJob<NoArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Whether the rebuilt certificates are put in use. */
        private final boolean apply;

        /**
         * @param arg Task argument.
         * @param debug Debug flag.
         * @param apply Whether the rebuilt certificates are put in use.
         */
        protected SslJob(NoArg arg, boolean debug, boolean apply) {
            super(arg, debug);

            this.apply = apply;
        }

        /** {@inheritDoc} */
        @Override protected String run(NoArg arg) throws IgniteException {
            // Sorted, so that the report of a node does not depend on the order the components started in.
            Map<String, SslContextReloadable> comps =
                new TreeMap<>(ignite.context().internalSubscriptionProcessor().getSslContextReloadables());

            List<String> rebuilt = new ArrayList<>();
            List<String> unchanged = new ArrayList<>();
            List<String> failed = new ArrayList<>();

            for (Map.Entry<String, SslContextReloadable> e : comps.entrySet()) {
                try {
                    SslContextReloadable comp = e.getValue();

                    boolean newCtx = apply ? comp.reloadSslContext() : comp.checkSslContext();

                    (newCtx ? rebuilt : unchanged).add(e.getKey());
                }
                catch (Exception ex) {
                    // Every component is attempted, so that one broken transport does not hide the state of the rest.
                    // Anything may be thrown here: the SSL context comes from a user-supplied factory.
                    failed.add(e.getKey() + " (" + ex.getMessage() + ')');
                }
            }

            String res = ignite.localNode().id() + ": " + report(rebuilt, unchanged, failed);

            if (!failed.isEmpty())
                throw new IgniteException(res);

            return res;
        }

        /**
         * @param rebuilt Components that produced new certificates.
         * @param unchanged Components whose factory returned the context already in use.
         * @param failed Components that could not be rebuilt, with the reason.
         * @return Outcome for this node.
         */
        private String report(List<String> rebuilt, List<String> unchanged, List<String> failed) {
            if (rebuilt.isEmpty() && unchanged.isEmpty() && failed.isEmpty())
                return "SSL is not configured, nothing to do";

            List<String> parts = new ArrayList<>();

            if (!rebuilt.isEmpty())
                parts.add((apply ? "reloaded " : "can be reloaded ") + rebuilt);

            if (!unchanged.isEmpty()) {
                parts.add((apply ? "NOT reloaded " : "cannot be reloaded ") + unchanged +
                    " - the configured SSL context factory returned the context already in use; hot reload requires " +
                    "a factory that rebuilds it, for example a subclass of " + AbstractSslContextFactory.class.getName());
            }

            if (!failed.isEmpty())
                parts.add((apply ? "failed to reload " : "would fail to reload ") + failed);

            return String.join("; ", parts);
        }
    }
}
