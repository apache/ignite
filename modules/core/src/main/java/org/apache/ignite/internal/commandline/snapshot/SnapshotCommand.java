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
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCancelTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTask;
import org.apache.ignite.mxbean.SnapshotMXBean;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CANCEL;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CREATE;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.of;

/**
 * control.sh Snapshot command.
 *
 * @see SnapshotMXBean
 * @see IgniteSnapshotManager
 */
public class SnapshotCommand implements Command<Object> {
    /** Command argument. */
    private Object taskArgs;

    /** Task name. */
    private String taskName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            return executeTaskByNameOnNode(
                client,
                taskName,
                taskArgs,
                null,
                clientCfg
            );
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArgs;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        SnapshotSubcommand cmd = of(argIter.nextArg("Expected snapshot action."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct action.");

        switch (cmd) {
            case CREATE:
                taskName = VisorSnapshotCreateTask.class.getName();
                taskArgs = argIter.nextArg("Expected snapshot name.");

                break;

            case CANCEL:
                taskName = VisorSnapshotCancelTask.class.getName();
                taskArgs = argIter.nextArg("Expected snapshot name.");

                break;

            default:
                throw new IllegalArgumentException("Unknown snapshot sub-command: " + cmd);
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Create cluster snapshot:", SNAPSHOT, singletonMap("snapshot_name", "Snapshot name."),
            CREATE.toString(), "snapshot_name");

        Command.usage(log, "Cancel running snapshot:", SNAPSHOT, singletonMap("snapshot_name", "Snapshot name."),
            CANCEL.toString(), "snapshot_name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SNAPSHOT.toCommandName();
    }
}
