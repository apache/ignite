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

package org.apache.ignite.internal.management;

import java.util.Collections;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ClientClusterImpl;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.NativeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.api.PreparableCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTask;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTaskResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/** */
@Deprecated
public class DeactivateCommand implements NativeCommand<DeactivateCommandArg, NoArg>, PreparableCommand<DeactivateCommandArg, NoArg> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Deactivate cluster (deprecated. Use --set-state instead)";
    }

    /** {@inheritDoc} */
    @Override public String deprecationMessage(DeactivateCommandArg arg) {
        return "Command deprecated. Use --set-state instead.";
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(DeactivateCommandArg arg) {
        return "Warning: the command will deactivate a cluster \"" + arg.clusterName() + "\".";
    }

    /** {@inheritDoc} */
    @Override public NoArg execute(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        DeactivateCommandArg arg,
        Consumer<String> printer
    ) {
        if (client != null)
            ((ClientClusterImpl)client.cluster()).state(INACTIVE, arg.force());
        else
            ((IgniteClusterEx)ignite.cluster()).state(INACTIVE, arg.force());

        printer.accept("Cluster deactivated");

        return null;
    }

    /** {@inheritDoc} */
    @Override public Class<DeactivateCommandArg> argClass() {
        return DeactivateCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public boolean prepare(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        DeactivateCommandArg arg,
        Consumer<String> printer
    ) throws Exception {
        String clusterName;

        if (ignite != null)
            clusterName = ((IgniteEx)ignite).context().cluster().clusterName();
        else {
            ClusterNode node = F.first(CommandUtils.nodes(client, ignite));

            VisorIdAndTagViewTaskResult idAndTag = CommandUtils.execute(client, ignite,
                VisorIdAndTagViewTask.class, null, Collections.singleton(node));

            clusterName = idAndTag.clusterName();
        }

        arg.clusterName(clusterName);

        return true;
    }
}
