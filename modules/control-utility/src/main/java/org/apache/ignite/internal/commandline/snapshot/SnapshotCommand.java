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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTaskArg;
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

        VisorSnapshotRestoreTaskAction cmdAction = VisorSnapshotRestoreTaskAction.START;

        Set<String> grpNames = null;
        boolean waitComplete = false;

        if (cmd == CANCEL || cmd == CHECK) {
            if (argIter.hasNextSubArg()) {
                throw new IllegalArgumentException(
                    "Argument \"" + argIter.nextArg("") + "\" is not expected for the \"" + cmd + "\" command.");
            }

            cmdArg = snpName;

            return;
        }

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("");

            if ("--wait".equals(arg)) {
                if (cmdAction != VisorSnapshotRestoreTaskAction.START)
                    throw new IllegalArgumentException("Operation \"" + cmdAction + "\" executes synchronously by default.");

                waitComplete = true;

                continue;
            }
            else if (cmd != RESTORE)
                throw new IllegalArgumentException("Command \"" + cmd + "\" doesn't support option \"" + arg + "\".");

            switch (arg) {
                case "--groups":
                    String argDesc = "comma-separated list of cache group names";

                    grpNames = argIter.nextStringSet(argDesc);

                    if (F.isEmpty(grpNames))
                        throw new IllegalArgumentException("A " + argDesc + " is expected.");

                    break;
                case "--cancel":
                case "--status":
                    cmdAction = VisorSnapshotRestoreTaskAction.fromCmdArg(arg);

                    if (waitComplete)
                        throw new IllegalArgumentException("Operation \"" + cmdAction + "\" executes synchronously by default.");

                    break;
                case "--start":
                    throw new IllegalArgumentException("Option \"" + arg + "\" is deprecated, " +
                        "use \"--groups\" instead. Check the command syntax for more details.");
                default:
                    throw new IllegalArgumentException("Command \"" + cmd + "\" doesn't support option \"" + arg + "\".");
            }
        }

        cmdArg = cmd == CREATE ?
            new VisorSnapshotCreateTaskArg(snpName, waitComplete) :
            new VisorSnapshotRestoreTaskArg(snpName, waitComplete, cmdAction, grpNames);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> commonParams = Collections.singletonMap("snapshot_name", "Snapshot name.");
        Map<String, String> createParams = new LinkedHashMap<>(commonParams);

        createParams.put("wait", "Wait for the entire operation to complete. Otherwise, the operation will run in the background and the command will return immediately.");

        usage(log, "Create cluster snapshot:", SNAPSHOT, createParams, CREATE.toString(), "snapshot_name", optional("--wait"));
        usage(log, "Cancel running snapshot:", SNAPSHOT, commonParams, CANCEL.toString(), "snapshot_name");
        usage(log, "Check snapshot:", SNAPSHOT, commonParams, CHECK.toString(), "snapshot_name");

        Map<String, String> restoreParams = new LinkedHashMap<>(createParams);

        restoreParams.put("group1,...groupN", "Cache group names.");


        usage(log, "Restore snapshot:", SNAPSHOT, restoreParams, RESTORE.toString(),
            "snapshot_name", optional("--wait"), optional("--groups", "group1,...groupN"));

        usage(log, "Snapshot restore operation status:", SNAPSHOT, commonParams, RESTORE.toString(),
            "snapshot_name", VisorSnapshotRestoreTaskAction.STATUS.cmdName());

        usage(log, "Cancel snapshot restore operation:", SNAPSHOT, commonParams, RESTORE.toString(),
            "snapshot_name", VisorSnapshotRestoreTaskAction.CANCEL.cmdName());
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (cmd != RESTORE)
            return null;

        VisorSnapshotRestoreTaskArg arg = (VisorSnapshotRestoreTaskArg)cmdArg;

        return arg.jobAction() == VisorSnapshotRestoreTaskAction.START && arg.groupNames() != null ? null :
            "Warning: command will restore ALL USER CREATED CACHE GROUPS from the snapshot " + arg.snapshotName() + '.';
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SNAPSHOT.toCommandName();
    }
}
