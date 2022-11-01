package org.apache.ignite.internal.logger;

import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Internal extension of {@link IgniteLogger}.
 */
public interface IgniteLoggerEx extends IgniteLogger {
    /** Adds console appender to the logger. */
    public void addConsoleAppender();

    /** Flush any buffered output. */
    public void flush();

    /**
     * Sets application name and node ID.
     *
     * @param application Application.
     * @param nodeId Node ID.
     */
    public void setApplicationAndNode(@Nullable String application, @Nullable UUID nodeId);
}
