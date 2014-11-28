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
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.Map;

/**
 * Statistic writer implementation that writes info into any Hadoop file system.
 */
public class GridHadoopFSStatWriter implements GridHadoopStatWriter {
    /** {@inheritDoc} */
    @Override public void write(GridHadoopJobInfo jobInfo, GridHadoopJobId jobId, GridHadoopCounters cntrs)
        throws IOException {

        Configuration hadoopCfg = new Configuration();

        for (Map.Entry<String, String> e : ((GridHadoopDefaultJobInfo)jobInfo).properties().entrySet())
            hadoopCfg.set(e.getKey(), e.getValue());

        String user = jobInfo.user();

        if (F.isEmpty(user))
            user = "anonymous";

        String dir = jobInfo.property("gridgain.hadoop.fsStatWriter.directory");

        if (dir == null)
            dir = "/users/${USER}";

        Path jobStatPath = new Path(new Path(dir.replace("${USER}", user)), jobId.toString());

        GridHadoopStatCounter cntr = cntrs.counter(GridHadoopStatCounter.GROUP_NAME, GridHadoopStatCounter.COUNTER_NAME,
            GridHadoopStatCounter.class);

        FileSystem fs = jobStatPath.getFileSystem(hadoopCfg);

        fs.mkdirs(jobStatPath);

        try (PrintStream out = new PrintStream(fs.create(new Path(jobStatPath, "statistics")))) {
            for (T2<String, Long> evt : cntr.evts()) {
                out.print(evt.get1());
                out.print(':');
                out.println(evt.get2().toString());
            }

            out.flush();
        }
    }
}
