/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.task.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop API v2 splitter.
 */
public class GridHadoopV2Splitter {
    /**
     * @param jobId Job ID.
     * @param info Job info.
     * @return Collection of mapped blocks.
     * @throws GridException If mapping failed.
     */
    public static Collection<GridHadoopFileBlock> splitJob(GridHadoopJobId jobId, GridHadoopDefaultJobInfo info)
        throws GridException {
        Configuration cfg = info.configuration();

        InputFormat<?, ?> format = (InputFormat<?, ?>)U.newInstance(cfg.getClass(
            MRJobConfig.INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class));

        assert format != null;

        try {
            JobContext jobCtx = new JobContextImpl(cfg,
                new JobID(jobId.globalId().toString(), jobId.localId()));

            List<InputSplit> splits = format.getSplits(jobCtx);

            Collection<GridHadoopFileBlock> res = new ArrayList<>(splits.size());

            for (InputSplit s0 : splits) {
                FileSplit s = (FileSplit)s0;

                GridHadoopFileBlock block = new GridHadoopFileBlock(s.getLocations(), s.getPath().toUri(),
                    s.getStart(), s.getLength());

                res.add(block);
            }

            return res;
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridException(e);
        }
    }
}
