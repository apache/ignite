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

package org.apache.ignite.internal.management.kill;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;

/**
 * Kill explicit lock command.
 */
public class KillLockCommand implements ComputeCommand<KillLockCommandArg, UUID> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Kill explicit lock by xid";
    }

    /** {@inheritDoc} */
    @Override public Class<KillLockCommandArg> argClass() {
        return KillLockCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<KillLockTask> taskClass() {
        return KillLockTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, KillLockCommandArg arg) {
        return CommandUtils.nodeOrAll(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(KillLockCommandArg arg, UUID res, Consumer<String> printer) {
        if (res == null) {
            printer.accept("Lock with xid " + arg.xid() + " is not found" +
                (arg.nodeId() == null ? "" : " on node " + arg.nodeId()) + '.');
        }
        else
            printer.accept("Lock with xid " + arg.xid() + " has been killed on node " + res + '.');
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(KillLockCommandArg arg) {
        return "Warning: the command will kill explicit lock with xid " + arg.xid()
            + ". Killing a lock may affect the execution of application code that relies on it.";
    }
}
