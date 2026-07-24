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

package org.apache.ignite.internal.management.io;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.api.CommandUtils.node;

/** */
public class IoTestCommunicationCommand implements ComputeCommand<IoTestCommunicationCommandArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Tests Communication SPI latency to all remote server nodes.";
    }

    /** {@inheritDoc} */
    @Override public Class<IoTestCommunicationCommandArg> argClass() {
        return IoTestCommunicationCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IoTestCommunicationTask> taskClass() {
        return IoTestCommunicationTask.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(
        Collection<ClusterNode> nodes,
        IoTestCommunicationCommandArg arg
    ) {
        return node(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(IoTestCommunicationCommandArg arg, String res, Consumer<String> printer) {
        if (F.isEmpty(res))
            printer.accept("Failed to run test. See logs for details.");
        else
            printer.accept(res);
    }
}
