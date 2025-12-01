package org.apache.ignite.internal.management.checkpoint;

import org.apache.ignite.internal.management.api.CommandRegistryImpl;

public class CheckpointCommand extends CommandRegistryImpl {
    /** */
    public CheckpointCommand() {
        super(
                new CheckpointForceCommand()
        );
    }
}