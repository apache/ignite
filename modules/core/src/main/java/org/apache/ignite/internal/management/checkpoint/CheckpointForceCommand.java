package org.apache.ignite.internal.management.checkpoint;

import java.util.Collection;
import java.util.function.Consumer;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.jetbrains.annotations.Nullable;

public class CheckpointForceCommand implements ComputeCommand<CheckpointForceCommandArg, String> {
    /** {@inheritDoc} */
    @Override public Class<CheckpointForceTask> taskClass() {
        return CheckpointForceTask.class;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Force checkpoint with optional parameters";
    }

    /** {@inheritDoc} */
    @Override public Class<CheckpointForceCommandArg> argClass() {
        return CheckpointForceCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Collection<ClusterNode> nodes(
            Collection<ClusterNode> nodes,
            CheckpointForceCommandArg arg
    ) {
        return CommandUtils.servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(CheckpointForceCommandArg arg, String res, Consumer<String> printer) {
        printer.accept(res);
    }
}