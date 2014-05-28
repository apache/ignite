/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.ggfs.GridGgfs;
import org.gridgain.grid.kernal.processors.ggfs.GridGgfsEx;
import org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils;
import org.gridgain.grid.util.GridUtils;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;

import static org.gridgain.grid.ggfs.GridGgfsConfiguration.DFLT_GGFS_LOG_DIR;

/**
 * Contains utility methods for Visor tasks and jobs.
 */
@SuppressWarnings("ExtendsUtilityClass")
public class VisorHadoopTaskUtilsEnt extends VisorTaskUtils {
    /**
     * Resolve GGFS profiler logs directory.
     *
     * @param ggfs GGFS instance to resolve logs dir for.
     * @return `Some(Path)` to log dir or `None` if not found.
     * @throws org.gridgain.grid.GridException if failed to resolve.
     */
    public static Path resolveGgfsProfilerLogsDir(GridGgfs ggfs) throws GridException {
        String logsDir;

        if (ggfs instanceof GridGgfsEx)
            logsDir = ((GridGgfsEx) ggfs).clientLogDirectory();
        else if (ggfs == null)
            throw new GridException("Failed to get profiler log folder (GGFS instance not found)");
        else
            throw new GridException("Failed to get profiler log folder (unexpected GGFS instance type)");

        URL logsDirUrl = GridUtils.resolveGridGainUrl(logsDir != null ? logsDir : DFLT_GGFS_LOG_DIR);

        if (logsDirUrl != null)
            return new File(logsDirUrl.getPath()).toPath();
        else
            return null;
    }
}
