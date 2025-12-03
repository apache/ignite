package org.apache.ignite.internal.management.checkpoint;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.jetbrains.annotations.Nullable;

public class CheckpointCommand implements ComputeCommand<CheckpointCommandArg, String> {
    /** {@inheritDoc} */
    @Override public Class<CheckpointTask> taskClass() {
        return CheckpointTask.class;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Trigger checkpoint with optional parameters";
    }

    /** {@inheritDoc} */
    @Override public Class<CheckpointCommandArg> argClass() {
        return CheckpointCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(
            Collection<ClusterNode> nodes,
            CheckpointCommandArg arg
    ) {
        return CommandUtils.servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(CheckpointCommandArg arg, String res, Consumer<String> printer) {
        printer.accept(res);
    }
}