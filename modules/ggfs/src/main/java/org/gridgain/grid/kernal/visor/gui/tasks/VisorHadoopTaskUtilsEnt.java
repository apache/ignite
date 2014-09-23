/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import static org.gridgain.grid.ggfs.GridGgfsConfiguration.*;
import static org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopLogger.*;

/**
 * Contains utility methods for Visor tasks and jobs.
 */
@SuppressWarnings("ExtendsUtilityClass")
public class VisorHadoopTaskUtilsEnt extends VisorTaskUtils {
    // Named column indexes in log file.
    public static final int LOG_COL_TIMESTAMP = 0;
    public static final int LOG_COL_THREAD_ID = 1;
    public static final int LOG_COL_ENTRY_TYPE = 3;
    public static final int LOG_COL_PATH = 4;
    public static final int LOG_COL_GGFS_MODE = 5;
    public static final int LOG_COL_STREAM_ID = 6;
    public static final int LOG_COL_DATA_LEN = 8;
    public static final int LOG_COL_OVERWRITE = 10;
    public static final int LOG_COL_POS = 13;
    public static final int LOG_COL_USER_TIME = 17;
    public static final int LOG_COL_SYSTEM_TIME = 18;
    public static final int LOG_COL_TOTAL_BYTES = 19;

    /** List of log entries that should be parsed. */
    public static final Set<Integer> LOG_TYPES = F.asSet(
            GridGgfsHadoopLogger.TYPE_OPEN_IN,
            GridGgfsHadoopLogger.TYPE_OPEN_OUT,
            GridGgfsHadoopLogger.TYPE_RANDOM_READ,
            GridGgfsHadoopLogger.TYPE_CLOSE_IN,
            GridGgfsHadoopLogger.TYPE_CLOSE_OUT
    );

    /**
     * Resolve GGFS profiler logs directory.
     *
     * @param ggfs GGFS instance to resolve logs dir for.
     * @return {@link Path} to log dir or {@code null} if not found.
     * @throws GridException if failed to resolve.
     */
    public static Path resolveGgfsProfilerLogsDir(GridGgfs ggfs) throws GridException {
        String logsDir;

        if (ggfs instanceof GridGgfsEx)
            logsDir = ((GridGgfsEx) ggfs).clientLogDirectory();
        else if (ggfs == null)
            throw new GridException("Failed to get profiler log folder (GGFS instance not found)");
        else
            throw new GridException("Failed to get profiler log folder (unexpected GGFS instance type)");

        URL logsDirUrl = U.resolveGridGainUrl(logsDir != null ? logsDir : DFLT_GGFS_LOG_DIR);

        return logsDirUrl != null ? new File(logsDirUrl.getPath()).toPath() : null;
    }
}
