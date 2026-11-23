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

package org.apache.ignite.internal.management.cdc;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;

import static org.apache.ignite.internal.management.api.CommandUtils.node;
import static org.apache.ignite.internal.management.api.CommandUtils.servers;

/** Command to clear CDC directory. */
public class CdcClearCommand implements ComputeCommand<CdcClearCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Clear CDC directory: delete WAL segment links and CDC application state";
    }

    /** {@inheritDoc} */
    @Override public Class<CdcClearCommandArg> argClass() {
        return CdcClearCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<CdcClearTask> taskClass() {
        return CdcClearTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, CdcClearCommandArg arg) {
        if (arg.nodeId() == null)
            return servers(nodes);

        List<ClusterNode> node = node(arg.nodeId(), nodes);

        if (node.get(0).isClient())
            throw new IllegalArgumentException("You specified a client node id. " +
                "Client nodes do not support CDC clear command.");

        return node;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CdcClearCommandArg arg, Void res, Consumer<String> printer) {
        printer.accept("CDC directories were successfully cleared.");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(CdcClearCommandArg arg) {
        String prompt = """
            Warning: The command will delete all WAL segment links and CDC application state from the CDC directory.
            The CDC application must be shut down.
            All changes in deleted segment links will be lost!
            To synchronize data after clearing, use the '--cdc resend' command, a snapshot or other methods.""";

        if (arg.resetMode()) {
            prompt += """

                CDC mode state will be deleted too. The CDC application will not capture changes \
                until Ignite nodes are restarted.""";
        }

        return prompt;
    }
}
