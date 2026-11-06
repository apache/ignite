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

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.ssl.SslCertificates.reason;

/**
 * Collects {@code --ssl} command results from all selected nodes, listing failures first.
 * Each job returns a success flag and a report. If a selected node is unavailable, or a job fails, the task fails with a report
 * containing both failed and successful results.
 */
public abstract class AbstractSslTask extends VisorMultiNodeTask<NoArg, String, IgniteBiTuple<Boolean, String>> {
    /** */
    @Serial
    private static final long serialVersionUID = 0L;

    /** Nodes selected by the command, including those that leave before jobs are mapped. */
    private Collection<UUID> nodeIds = List.of();

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<NoArg> arg) {
        nodeIds = List.copyOf(super.jobNodes(arg));

        return nodeIds;
    }

    /**
     * @param node Node.
     * @return Node UUID and consistent ID for command output.
     */
    static String node(ClusterNode node) {
        return node.id() + " [consistentId=" + node.consistentId() + ']';
    }

    /** {@inheritDoc} */
    @Override protected String reduce0(List<ComputeJobResult> results) throws IgniteException {
        List<String> failed = new ArrayList<>();
        List<String> succeeded = new ArrayList<>();
        Set<UUID> missing = new LinkedHashSet<>(nodeIds);

        for (ComputeJobResult jobRes : results) {
            missing.remove(jobRes.getNode().id());

            IgniteException e = jobRes.getException();

            if (e != null)
                failed.add(node(jobRes.getNode()) + ": " + reason(e));
            else {
                IgniteBiTuple<Boolean, String> rep = jobRes.getData();

                (rep.get1() ? succeeded : failed).add(rep.get2());
            }
        }

        for (UUID id : missing)
            failed.add(id + ": node was unavailable to run the command");

        if (failed.isEmpty())
            return String.join("\n", succeeded) + '\n';

        throw new IgniteException("Failed on " + failed.size() + " node(s):\n" + String.join("\n", failed) + '\n' +
            (succeeded.isEmpty() ? "" : "\nSucceeded on " + succeeded.size() + " node(s):\n" + String.join("\n", succeeded) + '\n'));
    }
}
