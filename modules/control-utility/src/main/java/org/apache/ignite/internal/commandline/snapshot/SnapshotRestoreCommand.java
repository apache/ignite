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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskAction;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.GROUPS;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.SOURCE;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.SYNC;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommands.RESTORE;
import static org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskAction.START;
import static org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskAction.STATUS;

/**
 * Sub-command to restore snapshot.
 */
public class SnapshotRestoreCommand extends SnapshotSubcommand {
    /** Default constructor. */
    protected SnapshotRestoreCommand() {
        super("restore", VisorSnapshotRestoreTask.class);
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        if (cmdArg instanceof VisorSnapshotRestoreTaskArg && ((VisorSnapshotRestoreTaskArg)cmdArg).jobAction() == STATUS)
            log.warning("Command deprecated. Use '" + SNAPSHOT + SnapshotSubcommands.STATUS + "' instead.");

        Object res = super.execute(clientCfg, log);

        log.info(String.valueOf(res));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        VisorSnapshotRestoreTaskAction restoreAction = parseAction(argIter);
        String snpPath = null;
        Set<String> grpNames = null;
        boolean sync = false;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg(null);

            if (restoreAction != START) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". " +
                    "Action \"--" + restoreAction.name().toLowerCase() + "\" does not support specified option.");
            }

            SnapshotRestoreCommandOption option = CommandArgUtils.of(arg, SnapshotRestoreCommandOption.class);

            if (option == null) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". " +
                    "Possible options: " + F.concat(F.asList(SnapshotRestoreCommandOption.values()), ", ") + '.');
            }
            else if (option == GROUPS) {
                if (grpNames != null)
                    throw new IllegalArgumentException(GROUPS.argName() + " arg specified twice.");

                String argDesc = "a comma-separated list of cache group names.";

                grpNames = argIter.nextStringSet(argDesc);

                if (grpNames.isEmpty())
                    throw new IllegalArgumentException("Expected " + argDesc);
            }
            else if (option == SOURCE) {
                if (snpPath != null)
                    throw new IllegalArgumentException(SOURCE.argName() + " arg specified twice.");

                String errMsg = "Expected path to the snapshot directory.";

                if (CommandArgIterator.isCommandOrOption(argIter.peekNextArg()))
                    throw new IllegalArgumentException(errMsg);

                snpPath = argIter.nextArg(errMsg);
            }
            else if (option == SYNC) {
                if (sync)
                    throw new IllegalArgumentException(SYNC.argName() + " arg specified twice.");

                sync = true;
            }
        }

        cmdArg = new VisorSnapshotRestoreTaskArg(snpName, snpPath, sync, restoreAction, grpNames);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = generalUsageOptions();
        Map<String, String> startParams = new LinkedHashMap<>(params);

        startParams.put(GROUPS.argName() + " " + GROUPS.arg(), GROUPS.description());
        startParams.put(SOURCE.argName() + " " + SOURCE.arg(), SOURCE.description());
        startParams.put(SYNC.argName(), SYNC.description());

        usage(log, "Restore snapshot:", SNAPSHOT, startParams, RESTORE.toString(), SNAPSHOT_NAME_ARG, "--start",
            optional(GROUPS.argName(), GROUPS.arg()), optional(SOURCE.argName(), SOURCE.arg()), optional(SYNC.argName()));
        usage(log, "Snapshot restore operation status (Command deprecated. Use '" + SNAPSHOT + SnapshotSubcommands.STATUS
            + "' instead.):", SNAPSHOT, params, RESTORE.toString(), SNAPSHOT_NAME_ARG, "--status");
        usage(log, "Cancel snapshot restore operation:", SNAPSHOT, params, RESTORE.toString(), SNAPSHOT_NAME_ARG, "--cancel");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        VisorSnapshotRestoreTaskArg arg = (VisorSnapshotRestoreTaskArg)cmdArg;

        return arg.jobAction() != START || arg.groupNames() != null ? null :
            "Warning: command will restore ALL USER-CREATED CACHE GROUPS from the snapshot " + arg.snapshotName() + '.';
    }

    /**
     * @param argIter Argument iterator.
     * @return Snapshot restore operation management action.
     */
    private VisorSnapshotRestoreTaskAction parseAction(CommandArgIterator argIter) {
        Collection<String> cmdNames =
            F.viewReadOnly(F.asList(VisorSnapshotRestoreTaskAction.values()), v -> "--" + v.toString().toLowerCase());

        String actionErrMsg = "One of " + cmdNames + " is expected.";

        String action = argIter.nextArg(actionErrMsg);

        for (VisorSnapshotRestoreTaskAction val : VisorSnapshotRestoreTaskAction.values()) {
            if (action.toLowerCase().equals("--" + val.name().toLowerCase()))
                return val;
        }

        throw new IllegalArgumentException("Invalid argument: " + action + ". " + actionErrMsg);
    }
}
