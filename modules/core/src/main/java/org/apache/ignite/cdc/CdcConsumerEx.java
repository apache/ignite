package org.apache.ignite.cdc;

import java.nio.file.Path;
import org.apache.ignite.metric.MetricRegistry;

/**
 * Extended CdcConsumer interface which provides some additional methods required for CDC regex filters.
 */
public interface CdcConsumerEx extends CdcConsumer {
    /**
     * Starts the consumer.
     * @param mreg Metric registry for consumer specific metrics.
     * @param cdcDir Path to Change Data Capture Directory.
     */
    void start(MetricRegistry mreg, Path cdcDir);
}
