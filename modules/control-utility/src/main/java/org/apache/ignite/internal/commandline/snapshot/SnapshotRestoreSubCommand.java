package org.apache.ignite.internal.commandline.snapshot;

import java.util.Arrays;
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
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreCancelTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotRestoreStatusTask;
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

    /** Command action. */
    private SnapshotRestoreAction cmdAction;

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
                cmdAction.taskCls.getName(),
                arg(),
                null,
                clientCfg
            );

            log.info(String.valueOf(res));

            return null;
        }
        catch (Throwable e) {
            log.severe("Failed to execute snapshot restore command [snapshot=" + taskArg.snapshotName() +
                    (taskArg.groupNames() == null ? "" : ", grp=" + taskArg.groupNames()) + ']');

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
        String snpName = argIter.nextArg("Snapshot name expected");
        String arg = argIter.nextArg("Restore action expected.");
        Set<String> grpNames = null;

        cmdAction = CommandArgUtils.of(arg, SnapshotRestoreAction.class);

        if (cmdAction == null) {
            throw new IllegalArgumentException("Invalid argument \"" + arg + "\" one of " +
                Arrays.toString(SnapshotRestoreAction.values()) + " is expected.");
        }

        if (argIter.hasNextSubArg()) {
            arg = argIter.nextArg("");

            if (cmdAction != SnapshotRestoreAction.START)
                throw new IllegalArgumentException("Invalid argument \"" + arg + "\", no more arguments expected.");

            grpNames = argIter.parseStringSet(arg);
        }

        taskArg = new VisorSnapshotRestoreTaskArg(snpName, grpNames);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return taskArg.groupNames() != null ? null :
            "Warning: command will restore ALL PUBLIC CACHE GROUPS from the snapshot " + taskArg.snapshotName() + '.';
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> restoreParams = new LinkedHashMap<>();

        restoreParams.put("snapshot_name", "Snapshot name.");
        restoreParams.put("group_name", "Cache group name.");

        Command.usage(log, "Restore snapshot:", SNAPSHOT, restoreParams, RESTORE.toString(),
            optional(SnapshotRestoreAction.START, "group_name"));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cmdName;
    }

    private enum SnapshotRestoreAction implements CommandArg {
        START("--start", VisorSnapshotRestoreTask.class),

        STOP("--stop", VisorSnapshotRestoreCancelTask.class),

        STATUS("--status", VisorSnapshotRestoreStatusTask.class);

        /** Name of the argument. */
        private final String name;

        /** Visro task class. */
        private final Class<?> taskCls;

        /**
         * @param taskCls
         * @param name Name of the argument.
         */
        SnapshotRestoreAction(String name, Class<?> taskCls) {
            this.name = name;
            this.taskCls = taskCls;
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
