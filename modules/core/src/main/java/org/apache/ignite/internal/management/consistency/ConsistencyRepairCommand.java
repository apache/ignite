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

package org.apache.ignite.internal.management.consistency;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.management.api.CommandUtils.nodes;

/** */
@IgniteExperimental
public class ConsistencyRepairCommand implements LocalCommand<ConsistencyRepairCommandArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Check/Repair cache consistency using Read Repair approach";
    }

    /** {@inheritDoc} */
    @Override public Class<ConsistencyRepairCommandArg> argClass() {
        return ConsistencyRepairCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public String execute(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        ConsistencyRepairCommandArg arg,
        Consumer<String> printer
    ) throws Exception {
        StringBuilder sb = new StringBuilder();
        boolean failed = false;

        if (arg.parallel())
            failed = execute(client, ignite, arg, nodes(client, ignite), sb);
        else {
            Set<ClusterNode> nodes = nodes(client, ignite).stream()
                .filter(node -> !node.isClient())
                .collect(toSet());

            for (ClusterNode node : nodes) {
                failed = execute(client, ignite, arg, Collections.singleton(node), sb);

                if (failed)
                    break;
            }
        }

        String res = sb.toString();

        if (failed)
            throw new IgniteException(res);

        printer.accept(res);

        return res;
    }

    /** */
    private boolean execute(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        ConsistencyRepairCommandArg arg,
        Collection<ClusterNode> nodes,
        StringBuilder sb
    ) throws Exception {
        boolean failed = false;

        ConsistencyTaskResult res = CommandUtils.execute(
            client,
            ignite,
            ConsistencyRepairTask.class,
            arg,
            nodes
        );

        if (res.cancelled()) {
            sb.append("Operation execution cancelled.\n\n");

            failed = true;
        }

        if (res.failed()) {
            sb.append("Operation execution failed.\n\n");

            failed = true;
        }

        if (failed)
            sb.append("[EXECUTION FAILED OR CANCELLED, RESULTS MAY BE INCOMPLETE OR INCONSISTENT]\n\n");

        if (res.message() != null)
            sb.append(res.message());
        else
            assert !arg.parallel();

        return failed;
    }
}
