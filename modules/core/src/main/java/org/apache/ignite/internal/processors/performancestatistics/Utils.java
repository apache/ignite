package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;

/** */
public class Utils {
    /**
     * File format version. This version should be incremented each time when format of existing events are
     * changed (fields added/removed) to avoid unexpected non-informative errors on deserialization.
     */
    public static final short FILE_FORMAT_VERSION = 1;

    /** Writes {@link UUID} to buffer. */
    static void writeUuid(ByteBuffer buf, UUID uuid) {
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
    /** @return Performance statistics file. */
    static File resolveStatisticsFile(GridKernalContext ctx, String fileName) throws IgniteCheckedException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File fileDir = U.resolveWorkDirectory(igniteWorkDir, PERF_STAT_DIR, false);

        File file = new File(fileDir, fileName + ".prf");

        int idx = 0;

        while (file.exists()) {
            idx++;

            file = new File(fileDir, fileName + '-' + idx + ".prf");
        }

        return file;
    }
}
