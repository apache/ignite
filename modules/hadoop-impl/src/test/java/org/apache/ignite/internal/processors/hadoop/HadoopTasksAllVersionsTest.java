/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.hadoop.examples.HadoopWordCount2;

/**
 * Tests of Map, Combine and Reduce task executions of any version of hadoop API.
 */
abstract class HadoopTasksAllVersionsTest extends HadoopAbstractWordCountTest {
    /** Empty hosts array. */
    private static final String[] HOSTS = new String[0];

    /**
     * Creates some grid hadoop job. Override this method to create tests for any job implementation.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws IOException If fails.
     */
    public abstract HadoopJob getHadoopJob(String inFile, String outFile) throws Exception;

    /**
     * @return prefix of reducer output file name. It's "part-" for v1 and "part-r-" for v2 API
     */
    public abstract String getOutputFileNamePrefix();

    /**
     * Tests map task execution.
     *
     * @throws Exception If fails.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMapTask() throws Exception {
        IgfsPath inDir = new IgfsPath(PATH_INPUT);

        igfs.mkdirs(inDir);

        IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

        URI inFileUri = URI.create(igfsScheme() + inFile.toString());

        try (PrintWriter pw = new PrintWriter(igfs.create(inFile, true))) {
            pw.println("hello0 world0");
            pw.println("world1 hello1");
        }

        HadoopFileBlock fileBlock1 = new HadoopFileBlock(HOSTS, inFileUri, 0, igfs.info(inFile).length() - 1);

        try (PrintWriter pw = new PrintWriter(igfs.append(inFile, false))) {
            pw.println("hello2 world2");
            pw.println("world3 hello3");
        }
        HadoopFileBlock fileBlock2 = new HadoopFileBlock(HOSTS, inFileUri, fileBlock1.length(),
                igfs.info(inFile).length() - fileBlock1.length());

        HadoopJob gridJob = getHadoopJob(igfsScheme() + inFile.toString(), igfsScheme() + PATH_OUTPUT);

        HadoopTaskInfo taskInfo = new HadoopTaskInfo(HadoopTaskType.MAP, gridJob.id(), 0, 0, fileBlock1);

        HadoopTestTaskContext ctx = new HadoopTestTaskContext(taskInfo, gridJob);

        ctx.mockOutput().clear();

        ctx.run();

        assertEquals("hello0,1; world0,1; world1,1; hello1,1", Joiner.on("; ").join(ctx.mockOutput()));

        ctx.mockOutput().clear();

        ctx.taskInfo(new HadoopTaskInfo(HadoopTaskType.MAP, gridJob.id(), 0, 0, fileBlock2));

        ctx.run();

        assertEquals("hello2,1; world2,1; world3,1; hello3,1", Joiner.on("; ").join(ctx.mockOutput()));
    }

    /**
     * Generates input data for reduce-like operation into mock context input and runs the operation.
     *
     * @param gridJob Job is to create reduce task from.
     * @param taskType Type of task - combine or reduce.
     * @param taskNum Number of task in job.
     * @param words Pairs of words and its counts.
     * @return Context with mock output.
     * @throws IgniteCheckedException If fails.
     */
    private HadoopTestTaskContext runTaskWithInput(HadoopJob gridJob, HadoopTaskType taskType,
        int taskNum, String... words) throws IgniteCheckedException {
        HadoopTaskInfo taskInfo = new HadoopTaskInfo(taskType, gridJob.id(), taskNum, 0, null);

        HadoopTestTaskContext ctx = new HadoopTestTaskContext(taskInfo, gridJob);

        for (int i = 0; i < words.length; i+=2) {
            List<IntWritable> valList = new ArrayList<>();

            for (int j = 0; j < Integer.parseInt(words[i + 1]); j++)
                valList.add(new IntWritable(1));

            ctx.mockInput().put(new Text(words[i]), valList);
        }

        ctx.run();

        return ctx;
    }

    /**
     * Tests reduce task execution.
     *
     * @throws Exception If fails.
     */
    public void testReduceTask() throws Exception {
        HadoopJob gridJob = getHadoopJob(igfsScheme() + PATH_INPUT, igfsScheme() + PATH_OUTPUT);

        runTaskWithInput(gridJob, HadoopTaskType.REDUCE, 0, "word1", "5", "word2", "10");
        runTaskWithInput(gridJob, HadoopTaskType.REDUCE, 1, "word3", "7", "word4", "15");

        assertEquals(
            "word1\t5\n" +
            "word2\t10\n",
            readAndSortFile(PATH_OUTPUT + "/_temporary/0/task_00000000-0000-0000-0000-000000000000_0000_r_000000/" +
                getOutputFileNamePrefix() + "00000")
        );

        assertEquals(
            "word3\t7\n" +
            "word4\t15\n",
            readAndSortFile(PATH_OUTPUT + "/_temporary/0/task_00000000-0000-0000-0000-000000000000_0000_r_000001/" +
                getOutputFileNamePrefix() + "00001")
        );
    }

    /**
     * Tests combine task execution.
     *
     * @throws Exception If fails.
     */
    public void testCombinerTask() throws Exception {
        HadoopJob gridJob = getHadoopJob("/", "/");

        HadoopTestTaskContext ctx =
            runTaskWithInput(gridJob, HadoopTaskType.COMBINE, 0, "word1", "5", "word2", "10");

        assertEquals("word1,5; word2,10", Joiner.on("; ").join(ctx.mockOutput()));

        ctx = runTaskWithInput(gridJob, HadoopTaskType.COMBINE, 1, "word3", "7", "word4", "15");

        assertEquals("word3,7; word4,15", Joiner.on("; ").join(ctx.mockOutput()));
    }

    /**
     * Runs chain of map-combine task on file block.
     *
     * @param fileBlock block of input file to be processed.
     * @param gridJob Hadoop job implementation.
     * @return Context of combine task with mock output.
     * @throws IgniteCheckedException If fails.
     */
    private HadoopTestTaskContext runMapCombineTask(HadoopFileBlock fileBlock, HadoopJob gridJob)
        throws IgniteCheckedException {
        HadoopTaskInfo taskInfo = new HadoopTaskInfo(HadoopTaskType.MAP, gridJob.id(), 0, 0, fileBlock);

        HadoopTestTaskContext mapCtx = new HadoopTestTaskContext(taskInfo, gridJob);

        mapCtx.run();

        //Prepare input for combine
        taskInfo = new HadoopTaskInfo(HadoopTaskType.COMBINE, gridJob.id(), 0, 0, null);

        HadoopTestTaskContext combineCtx = new HadoopTestTaskContext(taskInfo, gridJob);

        combineCtx.makeTreeOfWritables(mapCtx.mockOutput());

        combineCtx.run();

        return combineCtx;
    }

    /**
     * Tests all job in complex.
     * Runs 2 chains of map-combine tasks and sends result into one reduce task.
     *
     * @throws Exception If fails.
     */
    @SuppressWarnings("ConstantConditions")
    public void testAllTasks() throws Exception {
        IgfsPath inDir = new IgfsPath(PATH_INPUT);

        igfs.mkdirs(inDir);

        IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

        URI inFileUri = URI.create(igfsScheme() + inFile.toString());

        generateTestFile(inFile.toString(), "red", 100, "blue", 200, "green", 150, "yellow", 70);

        //Split file into two blocks
        long fileLen = igfs.info(inFile).length();

        Long l = fileLen / 2;

        HadoopFileBlock fileBlock1 = new HadoopFileBlock(HOSTS, inFileUri, 0, l);
        HadoopFileBlock fileBlock2 = new HadoopFileBlock(HOSTS, inFileUri, l, fileLen - l);

        HadoopJob gridJob = getHadoopJob(inFileUri.toString(), igfsScheme() + PATH_OUTPUT);

        HadoopTestTaskContext combine1Ctx = runMapCombineTask(fileBlock1, gridJob);

        HadoopTestTaskContext combine2Ctx = runMapCombineTask(fileBlock2, gridJob);

        //Prepare input for combine
        HadoopTaskInfo taskInfo = new HadoopTaskInfo(HadoopTaskType.REDUCE, gridJob.id(), 0, 0, null);

        HadoopTestTaskContext reduceCtx = new HadoopTestTaskContext(taskInfo, gridJob);

        reduceCtx.makeTreeOfWritables(combine1Ctx.mockOutput());
        reduceCtx.makeTreeOfWritables(combine2Ctx.mockOutput());

        reduceCtx.run();

        reduceCtx.taskInfo(new HadoopTaskInfo(HadoopTaskType.COMMIT, gridJob.id(), 0, 0, null));

        reduceCtx.run();

        assertEquals(
            "blue\t200\n" +
            "green\t150\n" +
            "red\t100\n" +
            "yellow\t70\n",
            readAndSortFile(PATH_OUTPUT + "/" + getOutputFileNamePrefix() + "00000")
        );
    }
}