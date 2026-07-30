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

import java.security.cert.X509Certificate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** Reloads TLS certificates on every mapped node, or only reports whether they can be reloaded. */
@GridInternal
public class SslReloadTask extends VisorMultiNodeTask<SslReloadCommandArg, String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SslReloadCommandArg, String> job(SslReloadCommandArg arg) {
        return new SslReloadJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder res = new StringBuilder();

        boolean failed = false;

        for (ComputeJobResult jobRes : results) {
            IgniteException e = jobRes.getException();

            if (e == null)
                res.append(jobRes.getData().toString());
            else if (X.hasCause(e, ClusterTopologyCheckedException.class))
                res.append(jobRes.getNode().id()).append(": left the cluster, nothing to reload");
            else {
                failed = true;

                res.append(e.getMessage());
            }

            res.append('\n');
        }

        // Every node is listed before the failure is raised: a partial reload leaves the cluster with mixed
        // certificates, so the operator has to see which nodes ended up on which side.
        if (failed)
            throw new IgniteException(res.toString());

        return res.toString();
    }

    /** */
    private static class SslReloadJob extends VisorJob<SslReloadCommandArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected SslReloadJob(SslReloadCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(SslReloadCommandArg arg) throws IgniteException {
            boolean apply = !arg.dryRun();

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

            String res = ignite.localNode().id() + ": " +
                report(apply, rebuilt, unchanged, failed, servedCertificate(comps.values()));

            if (!failed.isEmpty())
                throw new IgniteException(res);

            return res;
        }

        /**
         * @param comps Components of this node.
         * @return Certificate the node presents to other nodes, or {@code null} if none could tell.
         */
        private static @Nullable X509Certificate servedCertificate(Collection<SslContextReloadable> comps) {
            for (SslContextReloadable comp : comps) {
                X509Certificate cert = comp.servedCertificate();

                if (cert != null)
                    return cert;
            }

            return null;
        }

        /**
         * @param apply Whether the rebuilt certificates were put in use.
         * @param rebuilt Components that produced new certificates.
         * @param unchanged Components that were handed a ready-made context and have nothing to read again.
         * @param failed Components that could not be rebuilt, with the reason.
         * @param served Certificate the node presents to other nodes, may be {@code null}.
         * @return Outcome for this node.
         */
        private static String report(boolean apply, List<String> rebuilt, List<String> unchanged,
            List<String> failed, @Nullable X509Certificate served) {
            if (rebuilt.isEmpty() && unchanged.isEmpty() && failed.isEmpty())
                return "SSL is not configured";

            List<String> parts = new ArrayList<>();

            if (!rebuilt.isEmpty())
                parts.add((apply ? "reloaded " : "can be reloaded ") + String.join(", ", rebuilt));

            if (!unchanged.isEmpty()) {
                parts.add((apply ? "not reloaded " : "cannot be reloaded ") + String.join(", ", unchanged) +
                    " - the SSL context is handed over ready-made, so there is nothing to read again");
            }

            if (!failed.isEmpty())
                parts.add((apply ? "failed on " : "would fail on ") + String.join(", ", failed));

            if (served != null) {
                parts.add("serving " + served.getSubjectX500Principal() + " until " +
                    served.getNotAfter().toInstant().atOffset(ZoneOffset.UTC).toLocalDate());
            }

            return String.join("; ", parts);
        }
    }
}
