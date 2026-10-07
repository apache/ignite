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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.internal.ssl.SslCertificates.describe;
import static org.apache.ignite.internal.ssl.SslCertificates.reason;

/** Reloads TLS certificates on every mapped node. */
@GridInternal
public class SslReloadTask extends SslTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, IgniteBiTuple<Boolean, String>> job(NoArg arg) {
        return new SslReloadJob(arg, debug, ignite.localNode().id());
    }

    /** */
    private static class SslReloadJob extends VisorJob<NoArg, IgniteBiTuple<Boolean, String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Node the command came through. */
        private final UUID originNodeId;

        /**
         * @param arg Argument.
         * @param debug Debug flag.
         * @param originNodeId Node the command came through.
         */
        private SslReloadJob(NoArg arg, boolean debug, UUID originNodeId) {
            super(arg, debug);

            this.originNodeId = originNodeId;
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Boolean, String> run(NoArg arg) throws IgniteException {
            String id = ignite.localNode().id().toString();

            Collection<SslContextReloadable> comps = ignite.context().internalSubscriptionProcessor().sslContexts().reloadables();

            if (comps.isEmpty())
                return new IgniteBiTuple<>(true, id + ": SSL is not configured");

            String initiator = initiator();

            List<String> lines = new ArrayList<>();

            boolean failed = false;

            for (SslContextReloadable comp : comps) {
                String transports = String.join(", ", comp.transports());

                try {
                    comp.reload();

                    String desc = describe(comp.servedCertificate());

                    if (ignite.log().isInfoEnabled()) {
                        ignite.log().info("TLS certificates reloaded [transports=" + transports + (desc.isEmpty() ? "" : ", " + desc) +
                            ", initiator=" + initiator + ']');
                    }

                    lines.add(id + ": reloaded " + transports + (desc.isEmpty() ? "" : "; serving " + desc));
                }
                catch (Exception e) {
                    failed = true;

                    String reason = reason(e);

                    lines.add(id + ": failed on " + transports + " (" + reason + ')');

                    ignite.log().warning("Failed to reload TLS certificates, the ones in use stay [transports=" + transports +
                        ", initiator=" + initiator + ", reason=" + reason + ']', e);
                }
            }

            return new IgniteBiTuple<>(!failed, String.join("\n", lines));
        }

        /** @return Who asked for the reload, as the node log names it. */
        private String initiator() {
            IgniteSecurity security = ignite.context().security();

            String res = "management command, originNodeId=" + originNodeId;

            if (security.enabled()) {
                SecuritySubject subj = security.securityContext().subject();

                res += ", login=" + subj.login() + ", address=" + subj.address();
            }

            return res;
        }
    }
}
