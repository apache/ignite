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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;

import java.util.*;

/**
 * Test of whole cycle of map-reduce processing via Job tracker.
 */
public class GridHadoopMapReduceTest extends GridHadoopAbstractWordCountTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * Tests whole job execution with all phases in all combination of new and old versions of API.
     * @throws Exception If fails.
     */
    public void testWholeMapReduceExecution() throws Exception {
        GridGgfsPath inDir = new GridGgfsPath(PATH_INPUT);

        ggfs.mkdirs(inDir);

        GridGgfsPath inFile = new GridGgfsPath(inDir, GridHadoopWordCount2.class.getSimpleName() + "-input");

        generateTestFile(inFile.toString(), "red", 100000, "blue", 200000, "green", 150000, "yellow", 70000 );

        for (int i = 0; i < 8; i++) {
            ggfs.delete(new GridGgfsPath(PATH_OUTPUT), true);

            boolean useNewMapper = (i & 1) == 0;
            boolean useNewCombiner = (i & 2) == 0;
            boolean useNewReducer = (i & 4) == 0;

            JobConf jobConf = new JobConf();

            //To split into about 40 items for v2
            jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

            //For v1
            jobConf.setInt("fs.local.block.size", 65000);

            // File system coordinates.
            setupFileSystems(jobConf);

            GridHadoopWordCount1.setTasksClasses(jobConf, !useNewMapper, !useNewCombiner, !useNewReducer);

            Job job = Job.getInstance(jobConf);

            GridHadoopWordCount2.setTasksClasses(job, useNewMapper, useNewCombiner, useNewReducer);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(ggfsScheme() + inFile.toString()));
            FileOutputFormat.setOutputPath(job, new Path(ggfsScheme() + PATH_OUTPUT));

            job.setJarByClass(GridHadoopWordCount2.class);

            GridFuture<?> fut = grid(0).hadoop().submit(new GridHadoopJobId(UUID.randomUUID(), 1),
                new GridHadoopDefaultJobInfo(job.getConfiguration()));

            fut.get();

            assertEquals("Use new mapper = " + useNewMapper + ", combiner = " + useNewCombiner + "reducer = " +
                useNewReducer,
                "blue\t200000\n" +
                "green\t150000\n" +
                "red\t100000\n" +
                "yellow\t70000\n",
                readAndSortFile(PATH_OUTPUT + "/" + (useNewReducer ? "part-r-" : "part-") + "00000")
            );
        }
    }
}
