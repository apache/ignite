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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskAction;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.CHECK_CRC;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.GROUPS;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.INCREMENT;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.SOURCE;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotRestoreCommandOption.SYNC;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommands.RESTORE;
import static org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskAction.START;

/**
 * Sub-command to restore snapshot.
 */
public class SnapshotRestoreCommand extends SnapshotSubcommand {
    /** Default constructor. */
    protected SnapshotRestoreCommand() {
        super("restore", VisorSnapshotRestoreTask.class);
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        explainDeprecatedOptions(cmdArg, log);

        return super.execute(clientCfg, log);
    }

    /**
     * @param cmdArg Command argument.
     * @param log Logger.
     */
    private void explainDeprecatedOptions(Object cmdArg, IgniteLogger log) {
        VisorSnapshotRestoreTaskAction action = ((VisorSnapshotRestoreTaskArg)cmdArg).jobAction();

        if (action == null)
            return;

        switch (action) {
            case START:
                log.warning("Command option '--" + START.toString().toLowerCase() + "' is redundant and must be avoided.");

                break;

            case CANCEL:
                log.warning("Command deprecated. Use `" + SNAPSHOT + ' ' + SnapshotSubcommands.CANCEL + "' instead.");

                break;

            case STATUS:
                log.warning("Command deprecated. Use '" + SNAPSHOT + ' ' + SnapshotSubcommands.STATUS + "' instead.");

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        VisorSnapshotRestoreTaskAction restoreAction = parseAction(argIter.peekNextArg());
        String snpPath = null;
        Integer incIdx = null;
        Set<String> grpNames = null;
        boolean sync = false;
        boolean checkCRC = false;

        if (restoreAction != null)
            argIter.nextArg(null);

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg(null);

            if (restoreAction != null && restoreAction != START) {
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
            else if (option == INCREMENT) {
                if (incIdx != null)
                    throw new IllegalArgumentException(INCREMENT.argName() + " arg specified twice.");

                String errMsg = "incremental snapshot index";

                if (CommandArgIterator.isCommandOrOption(argIter.peekNextArg()))
                    throw new IllegalArgumentException("Expected " + errMsg + '.');

                incIdx = argIter.nextIntArg(errMsg);
            }
            else if (option == SYNC) {
                if (sync)
                    throw new IllegalArgumentException(SYNC.argName() + " arg specified twice.");

                sync = true;
            }
            else if (option == CHECK_CRC) {
                if (checkCRC)
                    throw new IllegalArgumentException(CHECK_CRC.argName() + " arg specified twice.");

                checkCRC = true;
            }
        }

        cmdArg = new VisorSnapshotRestoreTaskArg(snpName, snpPath, incIdx, sync, restoreAction, grpNames, checkCRC);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = generalUsageOptions();
        Map<String, String> startParams = new LinkedHashMap<>(params);

        startParams.put(GROUPS.argName() + " " + GROUPS.arg(), GROUPS.description());
        startParams.put(SOURCE.argName() + " " + SOURCE.arg(), SOURCE.description());
        startParams.put(INCREMENT.argName() + " " + INCREMENT.arg(), INCREMENT.description());
        startParams.put(SYNC.argName(), SYNC.description());
        startParams.put(CHECK_CRC.argName(), CHECK_CRC.description());

        usage(log, "Restore snapshot:", SNAPSHOT, startParams, RESTORE.toString(), SNAPSHOT_NAME_ARG,
            optional(INCREMENT.argName(), INCREMENT.arg()),
            optional(GROUPS.argName(), GROUPS.arg()),
            optional(SOURCE.argName(), SOURCE.arg()),
            optional(SYNC.argName()),
            optional(CHECK_CRC.argName()));
        usage(log, "Snapshot restore operation status (Command deprecated. Use '" + SNAPSHOT + ' '
            + SnapshotSubcommands.STATUS + "' instead):", SNAPSHOT, params, RESTORE.toString(), SNAPSHOT_NAME_ARG, "--status");
        usage(log, "Cancel snapshot restore operation (Command deprecated. Use '" + SNAPSHOT + ' '
            + SnapshotSubcommands.CANCEL + "' instead):", SNAPSHOT, params, RESTORE.toString(), SNAPSHOT_NAME_ARG, "--cancel");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        VisorSnapshotRestoreTaskArg arg = (VisorSnapshotRestoreTaskArg)cmdArg;

        return (arg.jobAction() != null && arg.jobAction() != START) || arg.groupNames() != null ? null :
            "Warning: command will restore ALL USER-CREATED CACHE GROUPS from the snapshot " + arg.snapshotName() + '.';
    }

    /**
     * @param arg Argument.
     * @return Snapshot restore operation management action.
     */
    private VisorSnapshotRestoreTaskAction parseAction(String arg) {
        if (arg == null)
            return null;

        for (VisorSnapshotRestoreTaskAction val : VisorSnapshotRestoreTaskAction.values()) {
            if (arg.toLowerCase().equals("--" + val.name().toLowerCase()))
                return val;
        }

        return null;
    }
}
