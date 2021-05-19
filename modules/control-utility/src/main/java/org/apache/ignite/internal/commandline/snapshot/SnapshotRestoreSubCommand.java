package org.apache.ignite.internal.commandline.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommand.RESTORE;

/**
 * Sub-command to restore cache group from snapshot.
 */
public class SnapshotRestoreSubCommand extends AbstractCommand<VisorSnapshotRestoreTaskArg> {
    /** Command name. */
    private final String cmdName;

    /** Task argument. */
    private VisorSnapshotRestoreTaskArg taskArg;

    /**
     * @param cmdName Command name.
     */
    public SnapshotRestoreSubCommand(String cmdName) {
        this.cmdName = cmdName;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                VisorSnapshotRestoreTask.class.getName(),
                arg(),
                null,
                clientCfg
            );

            log.info(String.valueOf(res));

            return null;
        }
        catch (Throwable e) {
            log.severe("Failed to restore snapshot [snapshot=" + taskArg.snapshotName() +
                    (taskArg.groupName() == null ? "" : ", grp=" + taskArg.groupName()) + ']');

            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public VisorSnapshotRestoreTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = null;
        String grpName = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            SnapshotRestoreCommandArg cmdArg = CommandArgUtils.of(arg, SnapshotRestoreCommandArg.class);

            if (cmdArg == SnapshotRestoreCommandArg.GROUP_NAME)
                grpName = argIter.nextArg("Expected cache group name.");
            else {
                if (snpName != null)
                    throw new IllegalArgumentException("Multiple snapshot names are not supported.");

                snpName = arg;
            }
        }

        if (snpName == null)
            throw new IllegalArgumentException("Expected snapshot name.");

        taskArg = new VisorSnapshotRestoreTaskArg(snpName, grpName);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return taskArg.groupName() != null ? null :
            "Warning: command will restore ALL PUBLIC CACHE GROUPS from the snapshot " + taskArg.snapshotName() + '.';
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> restoreParams = new LinkedHashMap<>();

        restoreParams.put("snapshot_name", "Snapshot name.");
        restoreParams.put("group_name", "Cache group name.");

        Command.usage(log, "Restore snapshot:", SNAPSHOT, restoreParams, RESTORE.toString(),
            optional(SnapshotRestoreCommandArg.GROUP_NAME, "group_name"));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cmdName;
    }

    /** */
    private enum SnapshotRestoreCommandArg implements CommandArg {
        /** Id of the node to get metric values from. */
        GROUP_NAME("--group");

        /** Name of the argument. */
        private final String name;

        /** @param name Name of the argument. */
        SnapshotRestoreCommandArg(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String argName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return name;
        }
    }
}
