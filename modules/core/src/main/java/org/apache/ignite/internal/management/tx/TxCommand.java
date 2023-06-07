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

package org.apache.ignite.internal.management.tx;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.CliSubcommandsWithPrefix;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.tx.TxCommand.AbstractTxCommandArg;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;

/** */
@CliSubcommandsWithPrefix
public class TxCommand extends CommandRegistryImpl<AbstractTxCommandArg, Map<ClusterNode, VisorTxTaskResult>>
    implements ComputeCommand<AbstractTxCommandArg, Map<ClusterNode, VisorTxTaskResult>> {
    /** */
    public TxCommand() {
        super(new TxInfoCommand());
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "List or kill transactions";
    }

    /** {@inheritDoc} */
    @Override public Class<TxCommandArg> argClass() {
        return TxCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorTxTask> taskClass() {
        return VisorTxTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(AbstractTxCommandArg arg0, Map<ClusterNode, VisorTxTaskResult> res, Consumer<String> printer) {
        TxCommandArg arg = (TxCommandArg)arg0;

        if (res.isEmpty())
            printer.accept("Nothing found.");
        else if (arg.kill())
            printer.accept("Killed transactions:");
        else
            printer.accept("Matching transactions:");

        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            if (entry.getValue().getInfos().isEmpty())
                continue;

            printer.accept(nodeDescription(entry.getKey()));

            for (VisorTxInfo info : entry.getValue().getInfos())
                printer.accept(info.toUserString());
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(AbstractTxCommandArg arg) {
        if (((TxCommandArg)arg).kill())
            return "Warning: the command will kill some transactions.";

        return null;
    }

    /** */
    public abstract static class AbstractTxCommandArg extends IgniteDataTransferObject {
        // No-op.
    }

    /**
     * Provides text descrition of a cluster node.
     *
     * @param node Node.
     */
    static String nodeDescription(ClusterNode node) {
        return node.getClass().getSimpleName() + " [id=" + node.id() +
            ", addrs=" + node.addresses() +
            ", order=" + node.order() +
            ", ver=" + node.version() +
            ", isClient=" + node.isClient() +
            ", consistentId=" + node.consistentId() +
            "]";
    }
}
