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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.api.CommandWarningException;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.ssl.SslContextValidator;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.TaskSessionResource;
import org.jetbrains.annotations.Nullable;

/** Reloads TLS certificates on every mapped node, or only reports whether they can be reloaded. */
@GridInternal
public class SslReloadTask extends VisorMultiNodeTask<SslReloadCommandArg, String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String READY_MADE =
        " - the SSL context is handed over ready-made, so there is nothing to read again";

    /** {@inheritDoc} */
    @Override protected VisorJob<SslReloadCommandArg, String> job(SslReloadCommandArg arg) {
        return new SslReloadJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder res = new StringBuilder();

        boolean failed = false;

        boolean warned = false;

        for (ComputeJobResult jobRes : results) {
            IgniteException e = jobRes.getException();

            if (e == null)
                res.append(jobRes.getData().toString());
            else if (X.hasCause(e, ClusterTopologyException.class, ClusterTopologyCheckedException.class)) {
                // The node may have reloaded before it left, so nothing is claimed about its certificates.
                res.append(jobRes.getNode().id()).append(": left the cluster, outcome unknown");
            }
            else {
                if (X.hasCause(e, CommandWarningException.class))
                    warned = true;
                else
                    failed = true;

                String msg = e.getMessage() != null ? e.getMessage() : e.toString();

                // The job reports every node with its id; anything else that failed has to be attributed too.
                res.append(msg.startsWith(jobRes.getNode().id().toString())
                    ? msg
                    : jobRes.getNode().id() + ": " + msg);
            }

            res.append('\n');
        }

        // Every node is listed before the failure is raised: nodes reload independently, so the operator has to see
        // which of them moved to the new certificates and which did not.
        if (failed)
            throw new IgniteException(res.toString());

        if (warned)
            throw warning(res.toString());

        return res.toString();
    }

    /**
     * @param res Report to return.
     * @return Exception that makes the command end with warnings and print the report.
     */
    private static IgniteException warning(String res) {
        return new IgniteException(res, new CommandWarningException(new IgniteException(res)));
    }

    /** */
    private static class SslReloadJob extends VisorJob<SslReloadCommandArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Session of the task, which names the node the command came through. */
        @TaskSessionResource
        private transient ComputeTaskSession ses;

        /** */
        protected SslReloadJob(SslReloadCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(SslReloadCommandArg arg) throws IgniteException {
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

            IgniteLogger log = ignite.log();

            String initiator = initiator();

            List<String> lines = new ArrayList<>();

            boolean failed = false;

            boolean readyMade = false;

            for (SslContextReloadable comp : sorted) {
                String users = String.join(", ", comp.users());

                boolean rebuilt;

                try {
                    rebuilt = arg.dryRun() ? comp.check() : comp.reload();
                }
                catch (Exception e) {
                    // Every provider is attempted, so that one broken transport neither hides the state of the rest
                    // nor keeps them from being reloaded. Anything may be thrown here: the context comes from a
                    // user-supplied factory.
                    failed = true;

                    String reason = reason(e);

                    lines.add(ignite.localNode().id() + ": " + (arg.dryRun() ? "would fail on " : "failed on ") +
                        users + " (" + reason + ')');

                    log.warning((arg.dryRun()
                        ? "TLS certificates on disk cannot be used, the ones in use stay"
                        : "Failed to reload TLS certificates, the ones in use stay") +
                        " [transports=" + users + ", initiator=" + initiator + ", reason=" + reason + ']', e);

                    continue;
                }

                if (!rebuilt) {
                    readyMade = true;

                    lines.add(ignite.localNode().id() + ": " + (arg.dryRun() ? "cannot be reloaded " : "not reloaded ") +
                        users + READY_MADE);

                    U.warn(log, "TLS certificates cannot be reloaded, the SSL context is handed over ready-made " +
                        "[transports=" + users + ", initiator=" + initiator + ']');

                    continue;
                }

                if (arg.dryRun()) {
                    lines.add(ignite.localNode().id() + ": can be reloaded " + users);

                    continue;
                }

                // The certificates are in use by now. Whatever goes wrong while describing them must not turn the
                // reload into a reported failure.
                X509Certificate cert = null;

                try {
                    cert = comp.servedCertificate();
                }
                catch (Exception ignored) {
                    // Described as unknown.
                }

                lines.add(ignite.localNode().id() + ": reloaded " + users + served(cert));

                if (log.isInfoEnabled()) {
                    String desc = SslContextValidator.describe(cert);

                    log.info("TLS certificates reloaded [transports=" + users + (desc.isEmpty() ? "" : ", " + desc) +
                        ", initiator=" + initiator + ']');
                }
            }

            String res = String.join("\n", lines);

            if (failed)
                throw new IgniteException(res);

            if (readyMade)
                throw warning(res);

            return res;
        }

        /**
         * @return Who asked for the reload, as the node log names it.
         */
        private String initiator() {
            IgniteSecurity security = ignite.context().security();

            String res = "management command, originNodeId=" + ses.getTaskNodeId();

            if (security.enabled()) {
                SecuritySubject subj = security.securityContext().subject();

                res += ", login=" + subj.login() + ", address=" + subj.address();
            }

            return res;
        }

        /**
         * @param e Failure to describe.
         * @return Messages along its chain of causes, each once. A failure out of a user-supplied factory may carry
         *      no message at all, and is then named by its type.
         */
        private static String reason(Throwable e) {
            StringBuilder sb = new StringBuilder();

            int depth = 0;

            for (Throwable t = e; t != null && depth < 10; t = t.getCause(), depth++) {
                String msg = t.getMessage();

                // A wrapper made out of its cause alone carries nothing but the cause's own description.
                if (msg == null || msg.isEmpty() || (t.getCause() != null && msg.equals(t.getCause().toString())))
                    continue;

                if (sb.indexOf(msg) >= 0)
                    continue;

                if (sb.length() > 0)
                    sb.append(": ");

                sb.append(msg);
            }

            return sb.length() > 0 ? sb.toString() : e.toString();
        }

        /**
         * @param cert Certificate the transports serve, {@code null} if it cannot be told without a peer.
         * @return The certificate, ready to append to a report line.
         */
        private static String served(@Nullable X509Certificate cert) {
            return cert == null ? "" : "; serving " + cert.getSubjectX500Principal() + " until " +
                cert.getNotAfter().toInstant().atOffset(ZoneOffset.UTC).toLocalDate() +
                ", issued by " + cert.getIssuerX500Principal();
        }
    }
}
