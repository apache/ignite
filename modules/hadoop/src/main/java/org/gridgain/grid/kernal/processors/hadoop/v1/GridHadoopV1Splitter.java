/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop API v1 splitter.
 */
public class GridHadoopV1Splitter {
    /** */
    private static final String[] EMPTY_HOSTS = {};

    /**
     * @param jobConf Job configuration.
     * @return Collection of mapped splits.
     * @throws GridException If mapping failed.
     */
    public static Collection<GridHadoopInputSplit> splitJob(JobConf jobConf) throws GridException {
        try {
            InputFormat<?, ?> format = jobConf.getInputFormat();

            assert format != null;

            InputSplit[] splits = format.getSplits(jobConf, 0);

            Collection<GridHadoopInputSplit> res = new ArrayList<>(splits.length);

            for (int i = 0; i < splits.length; i++) {
                InputSplit nativeSplit = splits[i];

                if (nativeSplit instanceof FileSplit) {
                    FileSplit s = (FileSplit)nativeSplit;

                    res.add(new GridHadoopFileBlock(s.getLocations(), s.getPath().toUri(), s.getStart(), s.getLength()));
                }
                else
                    res.add(GridHadoopUtils.wrapSplit(i, nativeSplit, nativeSplit.getLocations()));
            }

            return res;
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /**
     * @param clsName Input split class name.
     * @param in Input stream.
     * @param hosts Optional hosts.
     * @return File block or {@code null} if it is not a {@link FileSplit} instance.
     * @throws GridException If failed.
     */
    @Nullable public static GridHadoopFileBlock readFileBlock(String clsName, FSDataInputStream in,
        @Nullable String[] hosts) throws GridException {
        if (!FileSplit.class.getName().equals(clsName))
            return null;

        FileSplit split = U.newInstance(FileSplit.class);

        try {
            split.readFields(in);
        }
        catch (IOException e) {
            throw new GridException(e);
        }

        if (hosts == null)
            hosts = EMPTY_HOSTS;

        return new GridHadoopFileBlock(hosts, split.getPath().toUri(), split.getStart(), split.getLength());
    }
}
