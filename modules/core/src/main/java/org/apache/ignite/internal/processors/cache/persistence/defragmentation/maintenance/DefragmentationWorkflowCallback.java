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
    private final CachePartitionDefragmentationManager defrgMgr;

    /** Logger provider. */
    private final Function<Class<?>, IgniteLogger> logProvider;

    /**
     * @param logProvider Logger provider.
     * @param defrgMgr Defragmentation manager.
     */
    public DefragmentationWorkflowCallback(
        Function<Class<?>, IgniteLogger> logProvider,
        CachePartitionDefragmentationManager defrgMgr
    ) {
        this.defrgMgr = defrgMgr;
        this.logProvider = logProvider;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<MaintenanceAction<?>> allActions() {
        return Collections.singletonList(new StopDefragmentationAction(defrgMgr));
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<Boolean> automaticAction() {
        return new ExecuteDefragmentationAction(logProvider, defrgMgr);
    }
}
