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

package org.apache.ignite.internal.commandline;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/**
 * Visor task executor.
 */
public class TaskExecutor {
    /** */
    public static final String DFLT_HOST = "127.0.0.1";

    /** */
    public static final String DFLT_PORT = "11211";

    /** Broadcast uuid. */
    public static final UUID BROADCAST_UUID = UUID.randomUUID();

    /**
     * @param client Client
     * @param taskClsName Task class name.
     * @param taskArgs Task args.
     * @param nodeId Node ID to execute task at (if null, random node will be chosen by balancer).
     * @param clientCfg Client configuration.
     * @return Task result.
     * @throws ClientException If failed to execute task.
     */
    public static <R> R executeTaskByNameOnNode(
        IgniteClient client,
        String taskClsName,
        Object taskArgs,
        UUID nodeId,
        ClientConfiguration clientCfg
    ) throws ClientException, InterruptedException {
        if (nodeId == BROADCAST_UUID) {
            Collection<ClusterNode> nodes = client.cluster().nodes();

            if (F.isEmpty(nodes))
                throw new ClientException("Connectable nodes not found", null);

            List<UUID> nodeIds = nodes.stream()
                .map(ClusterNode::id)
                .collect(Collectors.toList());

            return client.compute().execute(taskClsName, new VisorTaskArgument<>(nodeIds, taskArgs, false));
        }

        ClusterNode node = null;

        if (nodeId == null) {
            node = getBalancedNode(client);
        }
        else {
            for (ClusterNode n : client.cluster().nodes()) {
                if (nodeId.equals(n.id())) {
                    node = n;

                    break;
                }
            }

            if (node == null)
                throw new IllegalArgumentException("Node with id=" + nodeId + " not found");
        }

        return client.compute(client.cluster().forNode(node)).execute(taskClsName, new VisorTaskArgument<>(node.id(), taskArgs, false));
    }

    /**
     * @param client Client.
     * @param taskCls Task class.
     * @param taskArgs Task arguments.
     * @param clientCfg Client configuration.
     * @return Task result.
     */
    public static <R> R executeTask(
        IgniteClient client,
        Class<? extends ComputeTask<?, R>> taskCls,
        Object taskArgs,
        ClientConfiguration clientCfg
    ) throws InterruptedException {
        return executeTaskByNameOnNode(client, taskCls.getName(), taskArgs, null, clientCfg);
    }

    /**
     * @param client Client instance.
     * @return balanced node.
     */
    public static ClusterNode getBalancedNode(IgniteClient client) {
        return client.cluster().forOldest().node();
    }
}
