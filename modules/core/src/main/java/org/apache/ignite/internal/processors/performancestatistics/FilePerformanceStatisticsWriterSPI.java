package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;

public interface FilePerformanceStatisticsWriterSPI {
    /** Starts collecting performance statistics. */
    void start();

    /** Stops collecting performance statistics. */
    void stop();

    /**
     * @param cacheId Cache id.
     * @param name    Cache name.
     */
    void cacheStart(int cacheId, String name);

    /**
     * @param type      Operation type.
     * @param cacheId   Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration  Duration in nanoseconds.
     */
    void cacheOperation(OperationType type, int cacheId, long startTime, long duration);

    /**
     * @param cacheIds  Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration  Duration in nanoseconds.
     * @param commited  {@code True} if commited.
     */
    void transaction(GridIntList cacheIds, long startTime, long duration, boolean commited);

    /**
     * @param type      Cache query type.
     * @param text      Query text in case of SQL query. Cache name in case of SCAN query.
     * @param id        Query id.
     * @param startTime Start time in milliseconds.
     * @param duration  Duration in nanoseconds.
     * @param success   Success flag.
     */
    void query(GridCacheQueryType type, String text, long id, long startTime, long duration,
        boolean success);

    /**
     * @param viewName Name of system view.
     * @param view     System view.
     */
    void systemView(String viewName, SystemView<?> view);

    /**
     * @param type          Cache query type.
     * @param queryNodeId   Originating node id.
     * @param id            Query id.
     * @param logicalReads  Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads);

    /**
     * @param type      Cache query type.
     * @param qryNodeId Originating node id.
     * @param id        Query id.
     * @param action    Action with rows.
     * @param rows      Number of rows.
     */
    void queryRows(GridCacheQueryType type, UUID qryNodeId, long id, String action, long rows);

    /**
     * @param type      Cache query type.
     * @param qryNodeId Originating node id.
     * @param id        Query id.
     * @param name      Query property name.
     * @param val       Query property value.
     */
    void queryProperty(GridCacheQueryType type, UUID qryNodeId, long id, String name, String val);

    /**
     * @param sesId     Session id.
     * @param taskName  Task name.
     * @param startTime Start time in milliseconds.
     * @param duration  Duration.
     * @param affPartId Affinity partition id.
     */
    void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId);

    /**
     * @param sesId      Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime  Start time in milliseconds.
     * @param duration   Job execution time.
     * @param timedOut   {@code True} if job is timed out.
     */
    void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut);

    /**
     * @param beforeLockDuration          Before lock duration.
     * @param lockWaitDuration            Lock wait duration.
     * @param listenersExecDuration       Listeners execute duration.
     * @param markDuration                Mark duration.
     * @param lockHoldDuration            Lock hold duration.
     * @param pagesWriteDuration          Pages write duration.
     * @param fsyncDuration               Fsync duration.
     * @param walCpRecordFsyncDuration    Wal cp record fsync duration.
     * @param writeCpEntryDuration        Write checkpoint entry duration.
     * @param splitAndSortCpPagesDuration Split and sort cp pages duration.
     * @param recoveryDataWriteDuration   Recovery data write duration in milliseconds.
     * @param totalDuration               Total duration in milliseconds.
     * @param cpStartTime                 Checkpoint start time in milliseconds.
     * @param pagesSize                   Pages size.
     * @param dataPagesWritten            Data pages written.
     * @param cowPagesWritten             Cow pages written.
     */
    void checkpoint(
        long beforeLockDuration,
        long lockWaitDuration,
        long listenersExecDuration,
        long markDuration,
        long lockHoldDuration,
        long pagesWriteDuration,
        long fsyncDuration,
        long walCpRecordFsyncDuration,
        long writeCpEntryDuration,
        long splitAndSortCpPagesDuration,
        long recoveryDataWriteDuration,
        long totalDuration,
        long cpStartTime,
        int pagesSize,
        int dataPagesWritten,
        int cowPagesWritten
    );

    /**
     * @param endTime  End time in milliseconds.
     * @param duration Duration in milliseconds.
     */
    void pagesWriteThrottle(long endTime, long duration);

    /**
     * @param sysView System view.
     */
    void systemView(SystemView<?> sysView);

    /** */
    File file();
}
