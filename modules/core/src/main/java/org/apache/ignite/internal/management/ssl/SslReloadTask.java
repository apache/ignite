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
import java.util.Comparator;
import java.util.List;
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

                String msg = e.getMessage() != null ? e.getMessage() : e.toString();

                // The job reports every node with its id; anything else that failed has to be attributed too.
                res.append(msg.startsWith(jobRes.getNode().id().toString())
                    ? msg
                    : jobRes.getNode().id() + ": " + msg);
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
            boolean commit = arg.commit();

            Collection<SslContextReloadable> comps =
                ignite.context().internalSubscriptionProcessor().getSslContextReloadables();

            if (comps.isEmpty())
                return ignite.localNode().id() + ": SSL is not configured";

            // Sorted, so that the report of a node does not depend on the order the components started in.
            // A provider whose transport never started serves nothing, so it has nothing to report either.
            List<SslContextReloadable> sorted = new ArrayList<>();

            for (SslContextReloadable comp : comps) {
                if (!comp.users().isEmpty())
                    sorted.add(comp);
            }

            // Configured but serving nothing is a different answer from not configured at all: it means a
            // transport did not start, which the operator would otherwise have to find out some other way.
            if (sorted.isEmpty())
                return ignite.localNode().id() + ": SSL is configured, but no transport is serving it";

            sorted.sort(Comparator.comparing(comp -> String.join(", ", comp.users())));

            List<String> lines = new ArrayList<>();

            boolean failed = false;

            for (SslContextReloadable comp : sorted) {
                String users = String.join(", ", comp.users());

                String outcome;

                if (commit) {
                    switch (comp.commit(arg.token())) {
                        case APPLIED:
                            outcome = "reloaded " + users + served(comp);

                            break;

                        case NOTHING_TO_APPLY:
                            outcome = "not reloaded " + users +
                                " - the SSL context is handed over ready-made, so there is nothing to read again" +
                                served(comp);

                            break;

                        default:
                            outcome = "not reloaded " + users + " - nothing was prepared here for this run, " +
                                "which is what a node that joined while the operator was being asked looks like";
                    }
                }
                else {
                    try {
                        outcome = comp.prepare(arg.token())
                            ? "can be reloaded " + users + served(comp)
                            : "cannot be reloaded " + users +
                                " - the SSL context is handed over ready-made, so there is nothing to read again";
                    }
                    catch (Exception e) {
                        // Every provider is attempted, so that one broken transport does not hide the state of the
                        // rest. Anything may be thrown here: the context comes from a user-supplied factory.
                        failed = true;

                        outcome = "would fail on " + users + " (" + reason(e) + ')';
                    }
                }

                lines.add(ignite.localNode().id() + ": " + outcome);
            }

            // Nothing may be left committable on a node that could not prepare everything, and a dry run keeps
            // nothing at all: it is a rehearsal, not a first phase the operator can later confirm.
            if (failed || arg.dryRun()) {
                for (SslContextReloadable comp : sorted)
                    comp.discard();
            }

            String res = String.join("\n", lines);

            if (failed)
                throw new IgniteException(res);

            return res;
        }

        /**
         * @param e Failure to describe.
         * @return Its message, or its type when it carries none, which is what an unexpected failure out of a
         *      user-supplied factory tends to look like.
         */
        private static String reason(Exception e) {
            return e.getMessage() != null ? e.getMessage() : e.toString();
        }

        /**
         * @param comp Provider to ask.
         * @return Certificate this provider presents, ready to append to its line, or an empty string if it cannot
         *      be told without a peer, which is the case for the transports a client connects to.
         */
        private static String served(SslContextReloadable comp) {
            X509Certificate cert = comp.servedCertificate();

            return cert == null ? "" : "; serving " + cert.getSubjectX500Principal() + " until " +
                cert.getNotAfter().toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
        }
    }
}
