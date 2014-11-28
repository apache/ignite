/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Statistic writer implementation that writes info into any Hadoop file system.
 */
public class GridHadoopFSCounterWriter implements GridHadoopCounterWriter {
    /** */
    public static final String PERFORMANCE_COUNTER_FILE_NAME = "statistics";

    /** */
    private static final String DEFAULT_USER_NAME = "anonymous";

    /** */
    private static final String COUNTER_WRITER_DIR_PROPERTY = "gridgain.hadoop.fsStatWriter.directory";

    /** */
    private static final String USER_MACRO = "${USER}";

    /** */
    private static final String DEFAULT_COUNTER_WRITER_DIR = "/users/" + USER_MACRO;

    /** {@inheritDoc} */
    @Override public void write(GridHadoopJobInfo jobInfo, GridHadoopJobId jobId, GridHadoopCounters cntrs)
        throws GridException {

        Configuration hadoopCfg = new Configuration();

        for (Map.Entry<String, String> e : ((GridHadoopDefaultJobInfo)jobInfo).properties().entrySet())
            hadoopCfg.set(e.getKey(), e.getValue());

        String user = jobInfo.user();

        if (F.isEmpty(user))
            user = DEFAULT_USER_NAME;

        String dir = jobInfo.property(COUNTER_WRITER_DIR_PROPERTY);

        if (dir == null)
            dir = DEFAULT_COUNTER_WRITER_DIR;

        Path jobStatPath = new Path(new Path(dir.replace(USER_MACRO, user)), jobId.toString());

        GridHadoopPerformanceCounter perfCntr = GridHadoopPerformanceCounter.getCounter(cntrs, null);

        try {
            FileSystem fs = jobStatPath.getFileSystem(hadoopCfg);

            fs.mkdirs(jobStatPath);

            try (PrintStream out = new PrintStream(fs.create(new Path(jobStatPath, PERFORMANCE_COUNTER_FILE_NAME)))) {
                for (T2<String, Long> evt : perfCntr.evts()) {
                    out.print(evt.get1());
                    out.print(':');
                    out.println(evt.get2().toString());
                }

                out.flush();
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
