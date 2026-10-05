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
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.ssl.SslCertificates;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.visor.VisorJob;

import static org.apache.ignite.internal.ssl.SslCertificates.chainNotAfter;
import static org.apache.ignite.internal.ssl.SslCertificates.describe;

/** Reports the TLS certificates of every mapped node; a node serving a certificate that is not valid now fails the command. */
@GridInternal
public class SslStatusTask extends SslTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, String> job(NoArg arg) {
        return new SslStatusJob(arg, debug);
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
            String id = ignite.localNode().id().toString();

            Collection<SslContextReloadable> comps = reloadables(ignite);

            if (comps.isEmpty())
                return id + ": SSL is not configured";

            long now = System.currentTimeMillis();

            List<String> lines = new ArrayList<>();

            boolean invalid = false;

            for (SslContextReloadable comp : comps) {
                lines.add(id + ": " + String.join(", ", comp.transports()));

                X509Certificate[] chain = comp.servedChain();

                if (chain == null)
                    lines.add("    serving unknown");
                else {
                    long notAfter = chainNotAfter(chain);

                    lines.add("    serving " + describe(chain[0]) +
                        (notAfter < chain[0].getNotAfter().getTime() ? ", chainNotAfter=" + Instant.ofEpochMilli(notAfter) : ""));

                    if (SslCertificates.invalidAt(chain, now) != null) {
                        invalid = true;

                        lines.add("    PROBLEM: the certificate is not valid now, peers refuse it");
                    }
                }

                if (comp.failures() > 0) {
                    lines.add("    last reload failed " + comp.failures() + " time(s) in a row, the last at " +
                        Instant.ofEpochMilli(comp.lastFailureTime()) + ": " + comp.lastFailure());
                }
                else if (comp.lastSuccessTime() > 0)
                    lines.add("    last reload succeeded at " + Instant.ofEpochMilli(comp.lastSuccessTime()));

                if (comp.nextRenewalTime() > 0)
                    lines.add("    next automatic renewal at " + Instant.ofEpochMilli(comp.nextRenewalTime()));
            }

            String res = String.join("\n", lines);

            if (invalid)
                throw new IgniteException(res);

            return res;
        }
    }
}
