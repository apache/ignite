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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.ssl.SslCertificates;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.ssl.SslReloadState;
import org.apache.ignite.internal.visor.VisorJob;

/**
 * Reports the TLS certificates every mapped node serves, which authorities it trusts, and how its last reload went.
 * A node whose certificate is no longer, or not yet, valid fails the command; a node whose last reload failed makes
 * it end with a warning.
 */
@GridInternal
public class SslStatusTask extends SslTask<NoArg> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, String> job(NoArg arg) {
        return new SslStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected String leftCluster() {
        return "left the cluster";
    }

    /** */
    private static class SslStatusJob extends VisorJob<NoArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected SslStatusJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(NoArg arg) throws IgniteException {
            String nothing = nothingServed(ignite);

            if (nothing != null)
                return nothing;

            long now = System.currentTimeMillis();

            List<String> lines = new ArrayList<>();

            boolean invalid = false;

            boolean reloadFailed = false;

            for (SslContextReloadable comp : serving(ignite)) {
                lines.add(ignite.localNode().id() + ": " + String.join(", ", comp.users()));

                X509Certificate[] chain = comp.servedChain();

                if (chain == null)
                    lines.add("    serving: unknown");
                else {
                    X509Certificate cert = chain[0];

                    long chainNotAfter = SslCertificates.chainNotAfter(chain);

                    String san = SslCertificates.subjectAlternativeNames(cert);

                    lines.add("    serving " + cert.getSubjectX500Principal() +
                        ", issued by " + cert.getIssuerX500Principal() +
                        ", serial " + SslCertificates.serial(cert) +
                        ", valid from " + Instant.ofEpochMilli(cert.getNotBefore().getTime()) +
                        " until " + Instant.ofEpochMilli(cert.getNotAfter().getTime()) +
                        (chainNotAfter < cert.getNotAfter().getTime()
                            ? ", chain valid until " + Instant.ofEpochMilli(chainNotAfter)
                            : "") +
                        (san.isEmpty() ? "" : ", SAN " + san));

                    if (now > chainNotAfter || now < cert.getNotBefore().getTime()) {
                        invalid = true;

                        lines.add("    PROBLEM: the certificate is not valid now, peers refuse it");
                    }
                }

                List<X509Certificate> trusted = comp.trustedAuthorities();

                lines.add("    trusts " + (trusted.isEmpty() ? "unknown" : SslCertificates.authorities(trusted)));

                SslReloadState state = comp.reloadState();

                if (state.failures() > 0) {
                    reloadFailed = true;

                    lines.add("    last reload failed " + state.failures() + " time(s) in a row, the last at " +
                        Instant.ofEpochMilli(state.lastFailureTime()) + ": " + state.lastFailure());
                }
                else if (state.lastSuccessTime() > 0)
                    lines.add("    last reload succeeded at " + Instant.ofEpochMilli(state.lastSuccessTime()));
                else
                    lines.add("    not reloaded since the node started");
            }

            String res = String.join("\n", lines);

            if (invalid)
                throw new IgniteException(res);

            if (reloadFailed)
                throw warning(res);

            return res;
        }
    }
}
