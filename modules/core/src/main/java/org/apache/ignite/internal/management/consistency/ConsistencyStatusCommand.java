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
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyStatusTask;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.servers;

/** */
@IgniteExperimental
public class ConsistencyStatusCommand implements ComputeCommand<NoArg, VisorConsistencyTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Cache consistency check/repair operations status";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorConsistencyStatusTask> taskClass() {
        return VisorConsistencyStatusTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, NoArg arg) {
        return servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(NoArg arg, VisorConsistencyTaskResult res, Consumer<String> printer) {
        if (res.cancelled())
            printer.accept("Operation execution cancelled.\n\n");

        if (res.failed())
            printer.accept("Operation execution failed.\n\n");

        if (res.cancelled() || res.failed())
            printer.accept("[EXECUTION FAILED OR CANCELLED, RESULTS MAY BE INCOMPLETE OR INCONSISTENT]\n\n");

        printer.accept(res.message());
    }
}
