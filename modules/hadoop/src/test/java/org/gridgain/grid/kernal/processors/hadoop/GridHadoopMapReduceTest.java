/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.commons.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * Test of whole cycle of map-reduce processing via Job tracker.
 */
public class GridHadoopMapReduceTest extends GridHadoopAbstractWordCountTest {
    /** {@inheritDoc} */
    @Override protected boolean ggfsEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());
    }

    /**
     * Tests whole job execution with all phases in all combination of new and old versions of API.
     * @throws Exception If fails.
     */
    public void testWholeMapReduceExecution() throws Exception {
        File testInputFile = File.createTempFile(GridHadoopWordCount2.class.getSimpleName(), "-input");

        testInputFile.deleteOnExit();

        generateTestFile(testInputFile, "red", 100000, "blue", 200000, "green", 150000, "yellow", 70000);

        File testOutputDir = Files.createTempDirectory("job-output").toFile();

        for (int i = 0; i < 8; i++) {
            boolean useNewMapper = (i & 1) == 0;
            boolean useNewCombiner = (i & 2) == 0;
            boolean useNewReducer = (i & 4) == 0;

            JobConf jobConf = new JobConf();

            //To split into about 40 items
            jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

            GridHadoopWordCount1.setTasksClasses(jobConf, !useNewMapper, !useNewCombiner, !useNewReducer);

            Job job = Job.getInstance(jobConf);

            GridHadoopWordCount2.setTasksClasses(job, useNewMapper, useNewCombiner, useNewReducer);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(testInputFile.getAbsolutePath()));
            FileOutputFormat.setOutputPath(job, new Path(testOutputDir.getAbsolutePath()));

            job.setJarByClass(GridHadoopWordCount2.class);

            try {
                GridHadoopProcessorAdapter hadoop = ((GridKernal) grid(0)).context().hadoop();

                GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 1),
                        new GridHadoopDefaultJobInfo(job.getConfiguration()));

                fut.get();

                assertEquals("Use new mapper = " + useNewMapper + ", combiner = " + useNewCombiner + "reducer = " + useNewReducer,
                    "green\t150000\n" +
                    "blue\t200000\n" +
                    "red\t100000\n" +
                    "yellow\t70000\n",
                    readFile(testOutputDir.getAbsolutePath() + "/" + (useNewReducer ? "part-r-" : "part-") + "00000")
                );
            }
            finally {
                FileUtils.deleteDirectory(testOutputDir);
            }
        }
    }
}
