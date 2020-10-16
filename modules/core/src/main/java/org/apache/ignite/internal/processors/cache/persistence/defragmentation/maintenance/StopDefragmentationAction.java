package org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance;

import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Action which allows to stop the defragmentation at any time from maintenance mode processor.
 */
class StopDefragmentationAction implements MaintenanceAction<Boolean> {
    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defragmentationManager;

    /**
     * @param defragmentationManager Defragmentation manager.
     */
    public StopDefragmentationAction(CachePartitionDefragmentationManager defragmentationManager) {
        this.defragmentationManager = defragmentationManager;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        defragmentationManager.cancel();

        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return "stop";
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Stopping the defragmentation process immediately";
    }
}
