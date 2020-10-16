package org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defragmentation specific callback for maintenance mode.
 */
public class DefragmentationWorkflowCallback implements MaintenanceWorkflowCallback {
    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defragmentationManager;

    /** Logger provider. */
    private final Function<Class, IgniteLogger> loggerProvider;

    /**
     * @param loggerProvider Logger provider.
     * @param manager Defragmentation manager.
     */
    public DefragmentationWorkflowCallback(
        Function<Class, IgniteLogger> loggerProvider,
        CachePartitionDefragmentationManager manager
    ) {
        defragmentationManager = manager;
        this.loggerProvider = loggerProvider;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        return defragmentationManager.groupIdsForDefragmentation().length > 0;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<MaintenanceAction<?>> allActions() {
        return Collections.singletonList(new StopDefragmentationAction(defragmentationManager));
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<Boolean> automaticAction() {
        return new ExecuteDefragmentationAction(loggerProvider, defragmentationManager);
    }
}
