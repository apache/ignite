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
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;
import static java.util.stream.Collectors.toSet;

/** */
public class ConsistencyRepairCommand implements
    ExperimentalCommand<ConsistencyRepairCommandArg, String>,
    LocalCommand<ConsistencyRepairCommandArg, String> {
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
        GridClient cli,
        ConsistencyRepairCommandArg arg,
        Consumer<String> printer
    ) throws Exception {
        StringBuilder sb = new StringBuilder();
        boolean failed = false;

        if (arg.parallel())
            failed = execute(cli, arg, cli.compute().nodes(GridClientNode::connectable), sb);
        else {
            Set<GridClientNode> nodes = cli.compute().nodes().stream()
                .filter(node -> !node.isClient())
                .collect(toSet());

            for (GridClientNode node : nodes) {
                failed = execute(cli, arg, Collections.singleton(node), sb);

                if (failed)
                    break;
            }
        }

        String res = sb.toString();

        if (failed)
            throw new IgniteCheckedException(res);

        printer.accept(res);

        return res;
    }

    /** */
    private boolean execute(
        GridClient cli,
        ConsistencyRepairCommandArg arg,
        Collection<GridClientNode> nodes,
        StringBuilder sb
    ) throws GridClientException {
        boolean failed = false;

        VisorConsistencyTaskResult res = cli.compute().projection(nodes).execute(
            VisorConsistencyRepairTask.class.getName(),
            new VisorTaskArgument<>(nodes.stream().map(GridClientNode::nodeId).collect(Collectors.toList()), arg, false)
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
