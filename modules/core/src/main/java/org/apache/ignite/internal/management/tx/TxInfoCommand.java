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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.tx.TxCommand.AbstractTxCommandArg;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.jetbrains.annotations.Nullable;

/** */
public class TxInfoCommand implements ComputeCommand<AbstractTxCommandArg, Map<ClusterNode, VisorTxTaskResult>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print detailed information (topology and key lock ownership) about specific transaction";
    }

    /** {@inheritDoc} */
    @Override public Class<TxInfoCommandArg> argClass() {
        return TxInfoCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorTxTask> taskClass() {
        return VisorTxTask.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, AbstractTxCommandArg arg) {
        //TODO: implement me
        return ComputeCommand.super.nodes(nodes, arg);
    }

    /** {@inheritDoc} */
    @Override public void printResult(AbstractTxCommandArg arg, Map<ClusterNode, VisorTxTaskResult> res, Consumer<String> printer) {
        ComputeCommand.super.printResult(arg, res, printer);
    }
}
