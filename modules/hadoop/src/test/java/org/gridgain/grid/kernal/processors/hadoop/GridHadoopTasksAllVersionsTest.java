/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import com.google.common.base.*;
import org.apache.commons.io.*;
import org.apache.hadoop.io.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

/**
 * Tests of Map, Combine and Reduce task executions of any version of hadoop API.
 */
abstract class GridHadoopTasksAllVersionsTest extends GridCommonAbstractTest {
    /**
     * Creates some grid hadoop job. Override this method to create tests for any job implementation.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws IOException If fails.
     */
    public abstract GridHadoopJob getHadoopJob(String inFile, String outFile) throws Exception;

    /**
     * @return prefix of reducer output file name. It's "part-" for v1 and "part-r-" for v2 API
     */
    public abstract String getOutputFileNamePrefix();

    /**
     * Tests map task execution.
     *
     * @throws Exception If fails.
     */
    public void testMapTask() throws Exception {
        File testInputFile = File.createTempFile(GridHadoopWordCount2.class.getSimpleName(), "-input");

        testInputFile.deleteOnExit();

        URI testInputFileURI = URI.create(testInputFile.getAbsolutePath());

        PrintWriter testInputFileWriter = new PrintWriter(testInputFile);

        testInputFileWriter.println("hello0 world0");
        testInputFileWriter.println("world1 hello1");
        testInputFileWriter.flush();

        GridHadoopFileBlock fileBlock1 = new GridHadoopFileBlock(null, testInputFileURI, 0, testInputFile.length() - 1);

        testInputFileWriter.println("hello2 world2");
        testInputFileWriter.println("world3 hello3");
        testInputFileWriter.close();

        GridHadoopFileBlock fileBlock2 =
                new GridHadoopFileBlock(null, testInputFileURI, fileBlock1.length(), testInputFile.length() - fileBlock1.length());

        GridHadoopJob gridJob = getHadoopJob(testInputFileURI.toString(), "/");

        GridHadoopTestTaskContext ctx = new GridHadoopTestTaskContext(gridJob);

        ctx.mockOutput().clear();
        GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.MAP, gridJob.id(), 0, 0, fileBlock1);
        GridHadoopTask task = gridJob.createTask(taskInfo);
        task.run(ctx);

        assertEquals("hello0,1; world0,1; world1,1; hello1,1", Joiner.on("; ").join(ctx.mockOutput()));

        ctx.mockOutput().clear();
        taskInfo = new GridHadoopTaskInfo (null, GridHadoopTaskType.MAP, gridJob.id(), 0, 0, fileBlock2);
        task = gridJob.createTask(taskInfo);
        task.run(ctx);

        assertEquals("hello2,1; world2,1; world3,1; hello3,1", Joiner.on("; ").join(ctx.mockOutput()));
    }

    /**
     * Reads whole text file into String.
     *
     * @param fileName Name of the file to read.
     * @return Content of the file as String value.
     * @throws IOException If could not read the file.
     */
    private String readFile(String fileName) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        String line;
        while ((line = reader.readLine()) != null)
            sb.append(line).append("\n");

        return sb.toString();
    }

    /**
     * Generates input data for reduce-like operation into mock context input and runs the operation.
     *
     * @param gridJob Job is to create reduce task from.
     * @param taskType Type of task - combine or reduce.
     * @param taskNum Number of task in job.
     * @param words Pairs of words and its counts.
     * @return Context with mock output.
     * @throws GridException If fails.
     */
    private GridHadoopTestTaskContext runTaskWithInput(GridHadoopJob gridJob, GridHadoopTaskType taskType,
                                                       int taskNum, String... words) throws GridException {
        GridHadoopTestTaskContext ctx = new GridHadoopTestTaskContext(gridJob);

        for (int i = 0; i < words.length; i+=2) {
            List<IntWritable> valList = new ArrayList<>();

            for (int j = 0; j < Integer.parseInt(words[i + 1]); j++)
                valList.add(new IntWritable(1));

            ctx.mockInput().put(new Text(words[i]), valList);
        }

        GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, taskType, gridJob.id(), taskNum, 0, null);
        GridHadoopTask task = gridJob.createTask(taskInfo);

        task.run(ctx);

        return ctx;
    }

    /**
     * Tests reduce task execution.
     *
     * @throws Exception If fails.
     */
    public void testReduceTask() throws Exception {
        Path outputDir = Files.createTempDirectory(GridHadoopWordCount2.class.getSimpleName() + "-output");

        try {
            URI testOutputDirURI = URI.create(outputDir.toString());

            GridHadoopJob gridJob = getHadoopJob("/", testOutputDirURI.toString());

            runTaskWithInput(gridJob, GridHadoopTaskType.REDUCE, 0, "word1", "5", "word2", "10");
            runTaskWithInput(gridJob, GridHadoopTaskType.REDUCE, 1, "word3", "7", "word4", "15");

            assertEquals(
                "word1\t5\n" +
                "word2\t10\n",
                readFile(outputDir + "/_temporary/0/task_00000000-0000-0000-0000-000000000000_0000_r_000000/" +
                        getOutputFileNamePrefix() + "00000")
            );

            assertEquals(
                "word3\t7\n" +
                "word4\t15\n",
                readFile(outputDir + "/_temporary/0/task_00000000-0000-0000-0000-000000000000_0000_r_000001/" +
                        getOutputFileNamePrefix() + "00001")
            );
        }
        finally {
            FileUtils.deleteDirectory(outputDir.toFile());
        }
    }

    /**
     * Tests combine task execution.
     *
     * @throws Exception If fails.
     */
    public void testCombinerTask() throws Exception {
        GridHadoopJob gridJob = getHadoopJob("/", "/");

        GridHadoopTestTaskContext ctx =
                runTaskWithInput(gridJob, GridHadoopTaskType.COMBINE, 0, "word1", "5", "word2", "10");

        assertEquals("word1,5; word2,10", Joiner.on("; ").join(ctx.mockOutput()));

        ctx = runTaskWithInput(gridJob, GridHadoopTaskType.COMBINE, 1, "word3", "7", "word4", "15");

        assertEquals("word3,7; word4,15", Joiner.on("; ").join(ctx.mockOutput()));
    }

    /**
     * Runs chain of map-combine task on file block.
     *
     * @param fileBlock block of input file to be processed.
     * @param gridJob Hadoop job implementation.
     * @return Context of combine task with mock output.
     * @throws GridException If fails.
     */
    private GridHadoopTestTaskContext runMapCombineTask(GridHadoopFileBlock fileBlock, GridHadoopJob gridJob) throws GridException {
        GridHadoopTestTaskContext mapCtx = new GridHadoopTestTaskContext(gridJob);

        GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.MAP, gridJob.id(), 0, 0, fileBlock);
        GridHadoopTask task = gridJob.createTask(taskInfo);

        task.run(mapCtx);

        //Prepare input for combine
        GridHadoopTestTaskContext combineCtx = new GridHadoopTestTaskContext(gridJob);
        combineCtx.makeTreeOfWritables(mapCtx.mockOutput());

        taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.COMBINE, gridJob.id(), 0, 0, null);
        task = gridJob.createTask(taskInfo);

        task.run(combineCtx);

        return combineCtx;
    }

    /**
     * Tests all job in complex.
     * Runs 2 chains of map-combine tasks and sends result into one reduce task.
     *
     * @throws Exception If fails.
     */
    public void testAllTasks() throws Exception {
        Path outputDir = Files.createTempDirectory(GridHadoopWordCount2.class.getSimpleName() + "-output");

        try {
            URI testOutputDirURI = URI.create(outputDir.toString());

            File testInputFile = File.createTempFile(GridHadoopWordCount2.class.getSimpleName(), "-input");
            testInputFile.deleteOnExit();

            URI testInputFileURI = URI.create(testInputFile.getAbsolutePath());

            generateTestFile(testInputFile, "red", 100, "blue", 200, "green", 150, "yellow", 70);

            //Split file into two blocks
            Long l = testInputFile.length() / 2;
            GridHadoopFileBlock fileBlock1 = new GridHadoopFileBlock(null, testInputFileURI, 0, l);
            GridHadoopFileBlock fileBlock2 = new GridHadoopFileBlock(null, testInputFileURI, l, testInputFile.length() - l);

            GridHadoopJob gridJob = getHadoopJob(testInputFileURI.toString(), testOutputDirURI.toString());

            GridHadoopTestTaskContext combine1Ctx = runMapCombineTask(fileBlock1, gridJob);

            GridHadoopTestTaskContext combine2Ctx = runMapCombineTask(fileBlock2, gridJob);

            //Prepare input for combine
            GridHadoopTestTaskContext reduceCtx = new GridHadoopTestTaskContext(gridJob);
            reduceCtx.makeTreeOfWritables(combine1Ctx.mockOutput());
            reduceCtx.makeTreeOfWritables(combine2Ctx.mockOutput());

            GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.REDUCE, gridJob.id(), 0, 0, null);
            GridHadoopTask task = gridJob.createTask(taskInfo);

            task.run(reduceCtx);

            taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.COMMIT, gridJob.id(), 0, 0, null);
            task = gridJob.createTask(taskInfo);

            task.run(reduceCtx);

            assertEquals(
                "blue\t200\n" +
                "green\t150\n" +
                "red\t100\n" +
                "yellow\t70\n",
                readFile(outputDir + "/" + getOutputFileNamePrefix() + "00000")
            );
        }
        finally {
            FileUtils.deleteDirectory(outputDir.toFile());
        }
    }

    /**
     * Generates text file with words. In one line there are from 5 to 9 words.
     *
     * @param file File that there is generation for.
     * @param wordCounts Pair word and count, i.e "hello", 2, "world", 3, etc.
     * @throws FileNotFoundException If could not create the file.
     */
    private void generateTestFile(File file, Object... wordCounts) throws FileNotFoundException {
        List<String> wordsArr = new ArrayList<>();

        //Generating
        for (int i = 0; i < wordCounts.length; i += 2) {
            String word = (String) wordCounts[i];
            int cnt = (Integer) wordCounts[i + 1];

            while (cnt-- > 0)
                wordsArr.add(word);
        }

        //Shuffling
        for (int i = 0; i < wordsArr.size(); i++) {
            int j = (int)(Math.random() * wordsArr.size());

            Collections.swap(wordsArr, i, j);
        }

        //Input file preparing
        PrintWriter testInputFileWriter = new PrintWriter(file);

        int j = 0;

        while (j < wordsArr.size()) {
            int i = 5 + (int)(Math.random() * 5);

            List<String> subList = wordsArr.subList(j, Math.min(j + i, wordsArr.size()));
            j += i;

            testInputFileWriter.println(Joiner.on(' ').join(subList));
        }

        testInputFileWriter.close();
    }
}
