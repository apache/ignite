package org.apache.ignite.internal.commandline.snapshot;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.F;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/**
 * Snapshot basic command. To perform the task, a single argument is used - the name of the snapshot.
 */
public class SnapshotBasicSubCommand extends AbstractCommand<String> {
    /** Command name. */
    private final String cmdName;

    /** Command description. */
    private final String description;

    /** Task class. */
    private final Class<?> taskCls;

    /** Snapshot name. */
    private String snpName;

    /**
     * @param cmdName Command name.
     * @param description Description.
     * @param taskCls Task class.
     */
    public SnapshotBasicSubCommand(String cmdName, String description, Class<?> taskCls) {
        this.cmdName = cmdName;
        this.description = description;
        this.taskCls = taskCls;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                taskCls.getName(),
                snpName,
                null,
                clientCfg
            );

            if (res instanceof IdleVerifyResultV2)
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
    @Override public void parseArguments(CommandArgIterator argIter) {
        snpName = argIter.nextArg("Expected snapshot name.");

        assert !F.isEmpty(snpName);
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return snpName;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, description + ':', SNAPSHOT, singletonMap("snapshot_name", "Snapshot name."),
            cmdName, "snapshot_name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cmdName;
    }
}
