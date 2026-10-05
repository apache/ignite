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
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.ssl.SslCertificates;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.ssl.SslReloadState;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.TaskSessionResource;
import org.jetbrains.annotations.Nullable;

/** Reloads TLS certificates on every mapped node, or only reports whether they can be reloaded. */
@GridInternal
public class SslReloadTask extends SslTask<SslReloadCommandArg> {
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
    @Override protected String leftCluster() {
        // The node may have reloaded before it left, so nothing is claimed about its certificates.
        return "left the cluster, outcome unknown";
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
            String nothing = nothingServed(ignite);

            if (nothing != null)
                return nothing;

            List<SslContextReloadable> sorted = serving(ignite);

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

                    String reason = SslReloadState.reason(e);

                    if (!arg.dryRun())
                        comp.reloadState().onFailure(reason);

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

                comp.reloadState().onSuccess();

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
                    String desc = SslCertificates.describe(cert);

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
