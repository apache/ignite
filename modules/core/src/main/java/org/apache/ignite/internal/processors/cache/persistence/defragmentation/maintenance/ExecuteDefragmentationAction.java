package org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance;

import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Action which allows to start the defragmentation process.
 */
class ExecuteDefragmentationAction implements MaintenanceAction<Boolean> {
    /** Logger. */
    private final IgniteLogger log;

    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defrgMgr;

    /**
     * @param logFunction Logger provider.
     * @param defrgMgr Defragmentation manager.
     */
    public ExecuteDefragmentationAction(
        Function<Class<?>, IgniteLogger> logFunction,
        CachePartitionDefragmentationManager defrgMgr
    ) {
        this.log = logFunction.apply(ExecuteDefragmentationAction.class);
        this.defrgMgr = defrgMgr;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        try {
            defrgMgr.executeDefragmentation();
        }
        catch (IgniteCheckedException e) {
            log.error("Defragmentation is failed", e);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return "execute";
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Starting the process of defragmentation.";
    }
}
