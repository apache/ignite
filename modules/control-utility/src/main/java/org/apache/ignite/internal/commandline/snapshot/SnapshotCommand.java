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

package org.apache.ignite.internal.commandline.snapshot;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.mxbean.SnapshotMXBean;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CANCEL;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CHECK;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CREATE;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.of;

/**
 * control.sh Snapshot command.
 *
 * @see SnapshotMXBean
 * @see IgniteSnapshotManager
 */
public class SnapshotCommand extends AbstractCommand<Object> {
    /** Command argument. */
    private String snpName;

    /** Snapshot sub-command to execute. */
    private SnapshotSubcommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                cmd.taskName(),
                snpName,
                null,
                clientCfg
            );

            if (cmd == CHECK)
                ((IdleVerifyResultV2)res).print(log::info, true);

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return snpName;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = of(argIter.nextArg("Expected snapshot action."));
        snpName = argIter.nextArg("Expected snapshot name.");

        if (F.isEmpty(snpName))
            throw new IllegalArgumentException("Expected snapshot name.");
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Create cluster snapshot:", SNAPSHOT, singletonMap("snapshot_name", "Snapshot name."),
            CREATE.toString(), "snapshot_name");

        Command.usage(log, "Cancel running snapshot:", SNAPSHOT, singletonMap("snapshot_name", "Snapshot name."),
            CANCEL.toString(), "snapshot_name");

        Command.usage(log, "Check snapshot:", SNAPSHOT, singletonMap("snapshot_name", "Snapshot name."),
            CHECK.toString(), "snapshot_name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SNAPSHOT.toCommandName();
    }
}
