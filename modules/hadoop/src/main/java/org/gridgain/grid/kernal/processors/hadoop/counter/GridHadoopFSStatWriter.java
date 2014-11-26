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

/**
 * Statistic writer implementation that writes info into any Hadoop file system.
 */
public class GridHadoopFSStatWriter implements GridHadoopStatWriter {
    /** */
    private String statDir = ".gridgain-hadoop/jobs";

    /** {@inheritDoc} */
    @Override public void write(GridHadoopJobId jobId, GridHadoopCounters cntrs) throws IOException {
        Configuration cfg = new Configuration();

        Path jobStatPath = new Path(new Path(statDir), jobId.toString());

        GridHadoopStatCounter cntr = cntrs.counter(GridHadoopStatCounter.GROUP_NAME, GridHadoopStatCounter.COUNTER_NAME,
            GridHadoopStatCounter.class);

        FileSystem fs = jobStatPath.getFileSystem(cfg);

        fs.mkdirs(jobStatPath);

        try (FSDataOutputStream out = fs.create(new Path(jobStatPath, "statistics"))) {
            for (T2<String, Long> evt : cntr.evts()) {
                out.writeUTF(evt.get1());
                out.writeChar(':');
                out.writeUTF(evt.get2().toString());
                out.writeChar('\n');
            }

            out.flush();
        }
    }

    /**
     * Gets directory for job statistics.
     *
     * @return Directory path.
     */
    public String getStatDir() {
        return statDir;
    }

    /**
     * Sets directory for job statistics.
     *
     * @param statDir Directory path.
     */
    public void setStatDir(String statDir) {
        this.statDir = statDir;
    }
}
