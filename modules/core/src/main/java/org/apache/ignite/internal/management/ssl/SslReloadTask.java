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
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.ssl.AbstractSslContextFactory;

import static java.util.stream.Collectors.joining;

/** */
@GridInternal
public class SslReloadTask extends VisorMultiNodeTask<NoArg, String, SslReloadTask.SslReloadJobResult> {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private Collection<UUID> targetNodes;

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid, VisorTaskArgument<NoArg> arg) {
        targetNodes = arg.getNodes();

        Map<? extends ComputeJob, ClusterNode> jobs = super.map0(subgrid, arg);

        if (jobs.isEmpty())
            throw new IgniteException(targetNodes.stream().map(id -> id + ": left the topology").collect(joining("\n")));

        return jobs;
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, SslReloadJobResult> job(NoArg arg) {
        return new SslReloadJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected String reduce0(List<ComputeJobResult> results) {
        Map<UUID, ComputeJobResult> nodeResults = new HashMap<>();

        for (ComputeJobResult res : results)
            nodeResults.put(res.getNode().id(), res);

        List<String> reports = new ArrayList<>();
        boolean failed = false;

        for (UUID id : targetNodes) {
            ComputeJobResult res = nodeResults.get(id);

            if (res == null || X.hasCause(res.getException(), ClusterTopologyException.class)) {
                reports.add(id + ": left the topology");
                failed = true;
            }
            else if (res.getException() != null) {
                reports.add(id + ": failed: " + errorMessage(res.getException()));
                failed = true;
            }
            else {
                SslReloadJobResult nodeRes = res.getData();

                reports.add(nodeRes.report);
                failed |= nodeRes.failed;
            }
        }

        String report = String.join("\n", reports);

        if (failed)
            throw new IgniteException(report);

        return report;
    }

    /** */
    private static String errorMessage(Throwable err) {
        return err.getMessage() == null ? err.getClass().getName() : err.getMessage();
    }

    /** */
    private static class SslReloadJob extends VisorJob<NoArg, SslReloadJobResult> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private SslReloadJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SslReloadJobResult run(NoArg arg) {
            Map<String, Factory<SSLContext>> factories = ignite.context().ssl().factories();
            UUID nodeId = ignite.localNode().id();
            String initiator = "";

            if (ignite.context().security().enabled()) {
                SecuritySubject subj = ignite.context().security().securityContext().subject();

                initiator = " [initiator=" + subj.login() + ", subjectId=" + subj.id() + ']';
            }

            if (factories.isEmpty()) {
                String report = nodeId + ": no TCP transports started with TLS";

                ignite.log().info("TLS reload: " + report + initiator);

                return new SslReloadJobResult(report, false);
            }

            Set<Factory<SSLContext>> processed = Collections.newSetFromMap(new IdentityHashMap<>());
            List<String> reports = new ArrayList<>();
            boolean failed = false;

            for (Factory<SSLContext> factory : factories.values()) {
                if (!processed.add(factory))
                    continue;

                String transports = factories.entrySet().stream()
                    .filter(entry -> entry.getValue() == factory)
                    .map(Map.Entry::getKey)
                    .collect(joining(", "));

                String outcome;
                boolean factoryFailed = false;
                Exception err = null;

                if (!(factory instanceof AbstractSslContextFactory)) {
                    outcome = "unsupported: " + factory.getClass().getName();
                    factoryFailed = true;
                }
                else {
                    try {
                        ((AbstractSslContextFactory)factory).init();

                        outcome = "reloaded";
                    }
                    catch (Exception e) {
                        outcome = "failed: " + errorMessage(e);
                        factoryFailed = true;
                        err = e;
                    }
                }

                String report = nodeId + ": " + transports + ": " + outcome;

                reports.add(report);
                failed |= factoryFailed;

                if (factoryFailed)
                    ignite.log().warning("TLS reload: " + report + initiator, err);
                else
                    ignite.log().info("TLS reload: " + report + initiator);
            }

            return new SslReloadJobResult(String.join("\n", reports), failed);
        }
    }

    /** */
    public static class SslReloadJobResult extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        @Order(0)
        String report;

        /** */
        @Order(1)
        boolean failed;

        /** */
        public SslReloadJobResult() {
            // No-op.
        }

        /** */
        public SslReloadJobResult(String report, boolean failed) {
            this.report = report;
            this.failed = failed;
        }
    }
}
