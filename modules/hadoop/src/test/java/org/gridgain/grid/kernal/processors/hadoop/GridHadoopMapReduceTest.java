/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;

import java.util.*;

/**
 * Test of whole cycle of map-reduce processing via Job tracker.
 */
public class GridHadoopMapReduceTest extends GridHadoopAbstractWordCountTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    @Override
    public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * Custom serialization class that inherits behaviour of native {@link WritableSerialization}.
     */
    private static class CustomSerialization extends WritableSerialization { }

    /**
     * Tests whole job execution with all phases in all combination of new and old versions of API.
     * @throws Exception If fails.
     */
    public void testWholeMapReduceExecution() throws Exception {
        GridGgfsPath inDir = new GridGgfsPath(PATH_INPUT);

        ggfs.mkdirs(inDir);

        GridGgfsPath inFile = new GridGgfsPath(inDir, GridHadoopWordCount2.class.getSimpleName() + "-input");

        generateTestFile(inFile.toString(), "aaaa", 1000, "bbbb", 2000, "cccc", 1500, "dddd", 700 );

        for (int i = 0; i < 1; i++) {
            ggfs.delete(new GridGgfsPath(PATH_OUTPUT), true);

            boolean useNewMapper = (i & 1) == 0;
            boolean useNewCombiner = (i & 2) == 0;
            boolean useNewReducer = (i & 4) == 0;
            boolean useCustomSerializer = (i & 8) == 0;

            JobConf jobConf = new JobConf();

            if (useCustomSerializer)
                jobConf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, CustomSerialization.class.getName());

            //To split into about 40 items for v2
            jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

            //For v1
            jobConf.setInt("fs.local.block.size", 65000);

            // File system coordinates.
            jobConf.set("fs.default.name", GGFS_SCHEME);
            jobConf.set("fs.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem");
            jobConf.set("fs.AbstractFileSystem.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem");

            GridHadoopWordCount1.setTasksClasses(jobConf, !useNewMapper, !useNewCombiner, !useNewReducer);

            Job job = Job.getInstance(jobConf);

            GridHadoopWordCount2.setTasksClasses(job, useNewMapper, useNewCombiner, useNewReducer);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(GGFS_SCHEME + inFile.toString()));
            FileOutputFormat.setOutputPath(job, new Path(GGFS_SCHEME + PATH_OUTPUT));

            job.setJarByClass(GridHadoopWordCount2.class);

            GridHadoopProcessorAdapter hadoop = ((GridKernal) grid(0)).context().hadoop();

            GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 1),
                    new GridHadoopDefaultJobInfo(job.getConfiguration()));

            fut.get();

            System.out.println("Future completed.");

            assertEquals("Use new mapper = " + useNewMapper + ", combiner = " + useNewCombiner + "reducer = " +
                    useNewReducer,
                "aaaa\t1000\n" +
                "bbbb\t2000\n" +
                "cccc\t1500\n" +
                "dddd\t700\n",
                readAndSortFile(PATH_OUTPUT + "/" + (useNewReducer ? "part-r-" : "part-") +
                    "00000")
            );
        }
    }
}
