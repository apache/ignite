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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskAction;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskArg;
import org.apache.ignite.mxbean.SnapshotMXBean;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CANCEL;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CHECK;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.CREATE;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.RESTORE;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.of;

/**
 * control.sh Snapshot command.
 *
 * @see SnapshotMXBean
 * @see IgniteSnapshotManager
 */
public class SnapshotCommand extends AbstractCommand<Object> {
    /** Command argument. */
    private Object cmdArg;

    /** Snapshot sub-command to execute. */
    private SnapshotSubcommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                cmd.taskName(),
                arg(),
                null,
                clientCfg
            );

            if (cmd == CHECK)
                ((IdleVerifyResultV2)res).print(log::info, true);

            if (cmd == RESTORE)
                log.info(String.valueOf(res));

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
        return cmdArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = of(argIter.nextArg("Expected snapshot action."));
        String snpName = argIter.nextArg("Expected snapshot name.");

        if (cmd != RESTORE) {
            cmdArg = snpName;

            return;
        }

        VisorSnapshotRestoreTaskAction cmdAction =
            VisorSnapshotRestoreTaskAction.fromCmdArg(argIter.nextArg("Restore action expected."));

        Set<String> grpNames = null;

        if (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("");

            if (cmdAction != VisorSnapshotRestoreTaskAction.START)
                throw new IllegalArgumentException("Invalid argument \"" + arg + "\", no more arguments expected.");

            grpNames = argIter.parseStringSet(arg);
        }

        cmdArg = new VisorSnapshotRestoreTaskArg(cmdAction, snpName, grpNames);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> commonParams = Collections.singletonMap("snapshot_name", "Snapshot name.");

        Command.usage(log, "Create cluster snapshot:", SNAPSHOT, commonParams, CREATE.toString(), "snapshot_name");
        Command.usage(log, "Cancel running snapshot:", SNAPSHOT, commonParams, CANCEL.toString(), "snapshot_name");
        Command.usage(log, "Check snapshot:", SNAPSHOT, commonParams, CHECK.toString(), "snapshot_name");

        Map<String, String> startParams = new LinkedHashMap<>(commonParams);

        startParams.put("group1,...groupN", "Cache group names.");

        Command.usage(log, "Restore snapshot:", SNAPSHOT, startParams, RESTORE.toString(),
            VisorSnapshotRestoreTaskAction.START.cmdName(), "snapshot_name", optional("group1,...groupN"));

        Command.usage(log, "Snapshot restore operation status:", SNAPSHOT, commonParams, RESTORE.toString(),
            VisorSnapshotRestoreTaskAction.STATUS.cmdName(), "snapshot_name");

        Command.usage(log, "Cancel snapshot restore operation:", SNAPSHOT, commonParams, RESTORE.toString(),
            VisorSnapshotRestoreTaskAction.CANCEL.cmdName(), "snapshot_name");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (cmd != RESTORE)
            return null;

        VisorSnapshotRestoreTaskArg arg = (VisorSnapshotRestoreTaskArg)cmdArg;

        return arg.jobAction() == VisorSnapshotRestoreTaskAction.START && arg.groupNames() != null ? null :
            "Warning: command will restore ALL PUBLIC CACHE GROUPS from the snapshot " + arg.snapshotName() + '.';
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SNAPSHOT.toCommandName();
    }
}
