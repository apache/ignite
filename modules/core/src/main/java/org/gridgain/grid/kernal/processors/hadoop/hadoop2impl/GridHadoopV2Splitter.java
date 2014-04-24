/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop API v2 splitter.
 */
public class GridHadoopV2Splitter {
    /**
     * @param ctx Job context.
     * @return Collection of mapped blocks.
     * @throws GridException If mapping failed.
     */
    public static Collection<GridHadoopFileBlock> splitJob(JobContext ctx) throws GridException {
        try {
            InputFormat<?, ?> format = U.newInstance(ctx.getInputFormatClass());

            assert format != null;

            List<InputSplit> splits = format.getSplits(ctx);

            Collection<GridHadoopFileBlock> res = new ArrayList<>(splits.size());

            for (InputSplit s0 : splits) {
                FileSplit s = (FileSplit)s0;

                GridHadoopFileBlock block = new GridHadoopFileBlock(s.getLocations(), s.getPath().toUri(),
                    s.getStart(), s.getLength());

                res.add(block);
            }

            return res;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }
}
