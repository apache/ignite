/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.fs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.net.*;

import static org.apache.ignite.fs.IgniteFsConfiguration.*;

/**
 * Wrapper of HDFS for support of separated working directory.
 */
public class GridHadoopDistributedFileSystem extends DistributedFileSystem {
    /** User name for each thread. */
    private final ThreadLocal<String> userName = new ThreadLocal<String>() {
        /** {@inheritDoc} */
        @Override protected String initialValue() {
            return DFLT_USER_NAME;
        }
    };

    /** Working directory for each thread. */
    private final ThreadLocal<Path> workingDir = new ThreadLocal<Path>() {
        /** {@inheritDoc} */
        @Override protected Path initialValue() {
            return getHomeDirectory();
        }
    };

    /** {@inheritDoc} */
    @Override public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);

        setUser(conf.get(MRJobConfig.USER_NAME, DFLT_USER_NAME));
    }

    /**
     * Set user name and default working directory for current thread.
     *
     * @param userName User name.
     */
    public void setUser(String userName) {
        this.userName.set(userName);

        setWorkingDirectory(getHomeDirectory());
    }

    /** {@inheritDoc} */
    @Override public Path getHomeDirectory() {
        Path path = new Path("/user/" + userName.get());

        return path.makeQualified(getUri(), null);
    }

    /** {@inheritDoc} */
    @Override public void setWorkingDirectory(Path dir) {
        Path fixedDir = fixRelativePart(dir);

        String res = fixedDir.toUri().getPath();

        if (!DFSUtil.isValidName(res))
            throw new IllegalArgumentException("Invalid DFS directory name " + res);

        workingDir.set(fixedDir);
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() {
        return workingDir.get();
    }
}
