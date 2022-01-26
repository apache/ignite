package org.apache.ignite.internal.ducktest.utils.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.List;

/**
 *
 */
public class MemoryUsageMetrics {
    /**
     *
     */
    public static long getPeakHeapMemory() {
        long totalPeakHeapBytes = 0;
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean pool : memoryPoolMXBeans) {
            if (pool.getType() == MemoryType.HEAP)
                totalPeakHeapBytes += pool.getPeakUsage().getUsed();
        }
        return totalPeakHeapBytes;
    }
}
