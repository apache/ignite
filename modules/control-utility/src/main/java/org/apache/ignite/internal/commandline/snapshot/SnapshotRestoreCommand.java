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
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommands.RESTORE;

/**
 * Snapshot restore sub-command.
 */
public class SnapshotRestoreCommand extends SnapshotSubcommand {
    /** Default contructor. */
    protected SnapshotRestoreCommand() {
        super("restore", VisorSnapshotRestoreTask.class);
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        Object res = super.execute(clientCfg, log);

        log.info(String.valueOf(res));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        VisorSnapshotRestoreTaskAction restoreAction = parseAction(argIter);
        Set<String> grpNames = null;
        boolean sync = false;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg(null);

            if (restoreAction != VisorSnapshotRestoreTaskAction.START)
                throw new IllegalArgumentException("Invalid argument: " + arg + '.');

            SnapshotRestoreCommandOption option = CommandArgUtils.of(arg, SnapshotRestoreCommandOption.class);

            if (option == null) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". " +
                    "One of " + F.asList(SnapshotRestoreCommandOption.values()) + " is expected.");
            }
            else if (option == SnapshotRestoreCommandOption.GROUPS) {
                String argDesc = "a comma-separated list of cache group names.";

                grpNames = argIter.nextStringSet(argDesc);

                if (grpNames.isEmpty())
                    throw new IllegalArgumentException("Expected " + argDesc);
            }
            else if (option == SnapshotRestoreCommandOption.SYNC)
                sync = true;
        }

        cmdArg = new VisorSnapshotRestoreTaskArg(snpName, sync, restoreAction, grpNames);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = F.asMap("snapshot_name", "Snapshot name.");
        Map<String, String> startParams = new LinkedHashMap<String, String>(params) {{
            put("group1,...groupN", "Cache group names.");
            put("sync", "Run the operation synchronously, the command will wait for the entire operation to complete. " +
                    "Otherwise, the it will be performed in the background, and the command will immediately return control.");
        }};

        usage(log, "Restore snapshot:", SNAPSHOT, startParams, RESTORE.toString(),
            "snapshot_name", "--start", optional("--sync"), optional("--groups", "group1,...groupN"));
        usage(log, "Snapshot restore operation status:", SNAPSHOT, params, RESTORE.toString(), "snapshot_name", "--status");
        usage(log, "Cancel snapshot restore operation:", SNAPSHOT, params, RESTORE.toString(), "snapshot_name", "--cancel");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        VisorSnapshotRestoreTaskArg arg = (VisorSnapshotRestoreTaskArg)cmdArg;

        return arg.jobAction() == VisorSnapshotRestoreTaskAction.START && arg.groupNames() != null ? null :
            "Warning: command will restore ALL USER CREATED CACHE GROUPS from the snapshot " + arg.snapshotName() + '.';
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
