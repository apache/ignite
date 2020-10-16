package org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.maintenance.MaintenanceTask;

import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.DEFRAGMENTATION_MNTC_TASK_NAME;

/**
 * Maintenance task for defragmentation.
 */
public class DefragmentationParameters {

    public static final String CACHE_GROUP_ID_SEPARATOR = ",";
    private final List<Integer> cacheGroupIds;

    /**
     * @param cacheGroupIds Id of cache group for defragmentations.
     */
    private DefragmentationParameters(List<Integer> cacheGroupIds) {
        this.cacheGroupIds = cacheGroupIds;
    }

    /**
     * Convert parameter to maintenance storage.
     *
     * @param cacheGroupIds Cache group ids for defragmentation.
     * @return Maintenance task.
     */
    public static MaintenanceTask toStore(List<Integer> cacheGroupIds) {
        return new MaintenanceTask(
            DEFRAGMENTATION_MNTC_TASK_NAME,
            "Cache group defragmentation",
            cacheGroupIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(CACHE_GROUP_ID_SEPARATOR))
        );
    }

    /**
     * @param rawTask Task from maintenance storage.
     * @return Defragmentation parameters.
     */
    public static DefragmentationParameters fromStore(MaintenanceTask rawTask) {
        return new DefragmentationParameters(Arrays.asList(rawTask.parameters()
            .split(CACHE_GROUP_ID_SEPARATOR))
            .stream()
            .map(Integer::valueOf)
            .collect(Collectors.toList())
        );
    }

    /**
     * @return Cache groups ids.
     */
    public List<Integer> cacheGroupIds() {
        return cacheGroupIds;
    }
}
