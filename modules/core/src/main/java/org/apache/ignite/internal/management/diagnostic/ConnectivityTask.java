/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.diagnostic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Visor task that checks connectivity status between nodes.
 */
@GridInternal
public class ConnectivityTask
    extends VisorMultiNodeTask<DiagnosticConnectivityCommandArg, Map<ClusterNode, ConnectivityResult>, ConnectivityResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<DiagnosticConnectivityCommandArg, ConnectivityResult> job(DiagnosticConnectivityCommandArg arg) {
        return new ConnectivityJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<ClusterNode, ConnectivityResult> reduce0(
        List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, ConnectivityResult> map = new HashMap<>();

        results.forEach(result -> {
            if (result.getException() != null)
                return;

            final ClusterNode node = result.getNode();
            final ConnectivityResult data = result.getData();
            map.put(node, data);
        });

        return map;
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<DiagnosticConnectivityCommandArg> arg) {
        return Arrays.asList(arg.getArgument().nodes());
    }

    /**
     * This job is sent to every node in cluster. It then use compute on every other node just to check
     * that there is a connection between nodes.
     */
    private static class ConnectivityJob extends VisorJob<DiagnosticConnectivityCommandArg, ConnectivityResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg   Formal job argument.
         * @param debug Debug flag.
         */
        private ConnectivityJob(DiagnosticConnectivityCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ConnectivityResult run(DiagnosticConnectivityCommandArg arg) {
            List<UUID> ids = ignite.cluster().nodes().stream()
                .map(ClusterNode::id)
                .filter(uuid -> !Objects.equals(ignite.localNode().id(), uuid))
                .collect(Collectors.toList());

            List<ClusterNode> nodes = new ArrayList<>(ignite.cluster().forNodeIds(ids).nodes());

            CommunicationSpi spi = ignite.configuration().getCommunicationSpi();

            Map<ClusterNode, Boolean> statuses = new HashMap<>();

            if (spi instanceof TcpCommunicationSpi) {
                BitSet set = ((TcpCommunicationSpi)spi).checkConnection(nodes).get();

                for (int i = 0; i < nodes.size(); i++) {
                    ClusterNode node = nodes.get(i);
                    boolean success = set.get(i);

                    statuses.put(node, success);
                }
            }

            return new ConnectivityResult(statuses);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ConnectivityJob.class, this);
        }
    }

}
