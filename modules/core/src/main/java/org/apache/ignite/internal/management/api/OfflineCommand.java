package org.apache.ignite.internal.management.api;

import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** Represents commands that can be executed without connecting to the cluster. */
public interface OfflineCommand<A extends IgniteDataTransferObject, R> extends Command<A, R> {
    /**
     * Executes the command in offline mode without cluster connection.
     *
     * @param arg Command argument.
     * @param printer Consumer for command output printing.
     * @return Command execution result.
     */
    public R execute(
        A arg,
        Consumer<String> printer
    );
}
