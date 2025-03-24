package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FLUSH_SIZE;

abstract class AbstractFilePerformanceStatisticsWriter implements FilePerformanceStatisticsWriterSPI {
    /** Directory to store performance statistics files. Placed under Ignite work directory. */
    public static final String PERF_STAT_DIR = "perf_stat";

    /** Default maximum file size in bytes. Performance statistics will be stopped when the size exceeded. */
    public static final long DFLT_FILE_MAX_SIZE = 32 * U.GB;

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = (int)(32 * U.MB);

    /** Default minimal batch size to flush in bytes. */
    public static final int DFLT_FLUSH_SIZE = (int)(8 * U.MB);

    /** Default maximum cached strings threshold. String caching will stop on threshold excess. */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 10 * 1024;

    /**
     * File format version. This version should be incremented each time when format of existing events are
     * changed (fields added/removed) to avoid unexpected non-informative errors on deserialization.
     */
    public static final short FILE_FORMAT_VERSION = 1;

    /** File writer thread name. */
    static final String WRITER_THREAD_NAME = "performance-statistics-writer";

    /** Hashcodes of cached strings. */
    protected final Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** Minimal batch size to flush in bytes. */
    protected final int flushSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_FLUSH_SIZE, DFLT_FLUSH_SIZE);

    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    protected final int cachedStrsThreshold = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, DFLT_CACHED_STRINGS_THRESHOLD);

    /** Count of cached strings. */
    protected volatile int knownStrsSz;

    /** @return Performance statistics file. */
    protected static File resolveStatisticsFile(GridKernalContext ctx) throws IgniteCheckedException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File fileDir = U.resolveWorkDirectory(igniteWorkDir, PERF_STAT_DIR, false);

        File file = new File(fileDir, "node-" + ctx.localNodeId() + ".prf");

        int idx = 0;

        while (file.exists()) {
            idx++;

            file = new File(fileDir, "node-" + ctx.localNodeId() + '-' + idx + ".prf");
        }

        return file;
    }

    /** Writes {@link UUID} to buffer. */
    protected static void writeUuid(ByteBuffer buf, UUID uuid) {
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
    }

    /** Writes {@link IgniteUuid} to buffer. */
    static void writeIgniteUuid(ByteBuffer buf, IgniteUuid uuid) {
        buf.putLong(uuid.globalId().getMostSignificantBits());
        buf.putLong(uuid.globalId().getLeastSignificantBits());
        buf.putLong(uuid.localId());
    }

    /**
     * @param buf    Buffer to write to.
     * @param str    String to write.
     * @param cached {@code True} if string cached.
     */
    static void writeString(ByteBuffer buf, String str, boolean cached) {
        buf.put(cached ? (byte)1 : 0);

        if (cached)
            buf.putInt(str.hashCode());
        else {
            byte[] bytes = str.getBytes();

            buf.putInt(bytes.length);
            buf.put(bytes);
        }
    }

    /** @return {@code True} if string was cached and can be written as hashcode. */
    protected boolean cacheIfPossible(String str) {
        if (knownStrsSz >= cachedStrsThreshold)
            return false;

        int hash = str.hashCode();

        // We can cache slightly more strings then threshold value.
        // Don't implement solution with synchronization here, because our primary goal is avoid any contention.
        if (knownStrs.contains(hash) || !knownStrs.add(hash))
            return true;

        knownStrsSz = knownStrs.size();

        return false;
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(int cacheId, String name) {

    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(OperationType type, int cacheId, long startTime, long duration) {

    }

    /** {@inheritDoc} */
    @Override public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commited) {

    }

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, long id, long startTime, long duration,
        boolean success) {

    }

    /** {@inheritDoc} */
    @Override public void systemView(String viewName, SystemView<?> view) {

    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID qryNodeId, long id, long logicalReads,
        long physicalReads) {

    }

    /** {@inheritDoc} */
    @Override public void queryRows(GridCacheQueryType type, UUID qryNodeId, long id, String action, long rows) {

    }

    /** {@inheritDoc} */
    @Override public void queryProperty(GridCacheQueryType type, UUID qryNodeId, long id, String name, String val) {

    }

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {

    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {

    }

    /** {@inheritDoc} */
    @Override public void checkpoint(long beforeLockDuration, long lockWaitDuration, long listenersExecDuration,
        long markDuration, long lockHoldDuration, long pagesWriteDuration, long fsyncDuration,
        long walCpRecordFsyncDuration, long writeCpEntryDuration, long splitAndSortCpPagesDuration,
        long recoveryDataWriteDuration, long totalDuration, long cpStartTime, int pagesSize, int dataPagesWritten,
        int cowPagesWritten) {

    }

    /** {@inheritDoc} */
    @Override public void pagesWriteThrottle(long endTime, long duration) {

    }

    /** {@inheritDoc} */
    @Override public void systemView(SystemView<?> sysView) {

    }
}
