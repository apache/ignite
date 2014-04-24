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
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.hadoop2impl.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

/**
 * Tests of Map, Combine and Reduce task executions.
 */
public class GridHadoopTasks2Test extends GridCommonAbstractTest {
    /**
     * Tests map task execution.
     * @throws GridException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    public void testMapTask() throws GridException, ClassNotFoundException, IOException, InterruptedException {
        File testInputFile = File.createTempFile(GridGainWordCount2.class.getSimpleName(), "-input");
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

        Job hadoopJob = GridGainWordCount2.getJob(testInputFileURI.toString(), "/");
        hadoopJob.setJobID(new JobID());

        GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);
        GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(hadoopJob.getConfiguration());

        GridHadoopV2JobImpl gridJob = new GridHadoopV2JobImpl(jobId, jobInfo);

        GridHadoopTestTaskContext ctx = new GridHadoopTestTaskContext(gridJob);

        ctx.mockOutput().clear();
        GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.MAP, jobId, 0, 0, fileBlock1);
        GridHadoopTask task = gridJob.createTask(taskInfo);
        task.run(ctx);

        assertEquals("hello0,1; world0,1; world1,1; hello1,1", Joiner.on("; ").join(ctx.mockOutput()));

        ctx.mockOutput().clear();
        taskInfo = new GridHadoopTaskInfo (null, GridHadoopTaskType.MAP, jobId, 0, 0, fileBlock2);
        task = gridJob.createTask(taskInfo);
        task.run(ctx);

        assertEquals("hello2,1; world2,1; world3,1; hello3,1", Joiner.on("; ").join(ctx.mockOutput()));
    }

    /**
     * Reads whole text file into String.
     * @param fileName name of file to read.
     * @return String value.
     * @throws IOException
     */
    private String readFile(String fileName) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }

        return sb.toString();
    }

    /**
     * Generates input data for reduce-like operation into mock context input and runs the operation.
     * @param gridJob to create reduce task from.
     * @param jobId Job ID.
     * @param taskType Type of task - combine or reduce.
     * @param taskNum Number of task in job.
     * @param words Pairs of words and its counts.
     * @return Context with mock output.
     * @throws GridException
     */
    private GridHadoopTestTaskContext runTaskWithInput(GridHadoopV2JobImpl gridJob, GridHadoopJobId jobId, GridHadoopTaskType taskType,
                                                       int taskNum, String... words) throws GridException {
        GridHadoopTestTaskContext ctx = new GridHadoopTestTaskContext(gridJob);

        for (int i = 0; i < words.length; i+=2) {
            List<IntWritable> valList = new ArrayList<>();

            for (int j = 0; j < Integer.parseInt(words[i + 1]); j++) {
                valList.add(new IntWritable(1));
            }

            ctx.mockInput().put(new Text(words[i]), valList);
        }

        GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, taskType, jobId, taskNum, 0, null);
        GridHadoopTask task = gridJob.createTask(taskInfo);
        task.run(ctx);
        return ctx;
    }

    /**
     * Tests reduce task execution.
     * @throws IOException
     * @throws GridException
     */
    public void testReduceTask() throws IOException, GridException {
        Path outputDir = Files.createTempDirectory(GridGainWordCount2.class.getSimpleName() + "-output");

        try {
            URI testOutputDirURI = URI.create(outputDir.toString());

            Job hadoopJob = GridGainWordCount2.getJob("/", testOutputDirURI.toString());
            hadoopJob.setJobID(new JobID());

            GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);
            GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(hadoopJob.getConfiguration());

            GridHadoopV2JobImpl gridJob = new GridHadoopV2JobImpl(jobId, jobInfo);

            runTaskWithInput(gridJob, jobId, GridHadoopTaskType.REDUCE, 0, "word1", "5", "word2", "10");
            runTaskWithInput(gridJob, jobId, GridHadoopTaskType.REDUCE, 1, "word3", "7", "word4", "15");

            assertEquals(
                "word1\t5\n" +
                "word2\t10\n",
                readFile(outputDir + "/part-r-00000")
            );

            assertEquals(
                "word3\t7\n" +
                "word4\t15\n",
                readFile(outputDir + "/part-r-00001")
            );

        } finally {
            FileUtils.deleteDirectory(outputDir.toFile());
        }
    }

    /**
     * Tests combine task execution.
     * @throws IOException
     * @throws GridException
     */
    public void testCombinerTask() throws IOException, GridException {
        Job hadoopJob = GridGainWordCount2.getJob("/", "/");
        hadoopJob.setJobID(new JobID());

        GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);
        GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(hadoopJob.getConfiguration());

        GridHadoopV2JobImpl gridJob = new GridHadoopV2JobImpl(jobId, jobInfo);

        GridHadoopTestTaskContext ctx =
                runTaskWithInput(gridJob, jobId, GridHadoopTaskType.COMBINE, 0, "word1", "5", "word2", "10");

        assertEquals("word1,5; word2,10", Joiner.on("; ").join(ctx.mockOutput()));

        ctx = runTaskWithInput(gridJob, jobId, GridHadoopTaskType.COMBINE, 1, "word3", "7", "word4", "15");

        assertEquals("word3,7; word4,15", Joiner.on("; ").join(ctx.mockOutput()));
    }

    /**
     * Runs chain of map-combine task on file block.
     * @param fileBlock block of input file to be processed.
     * @param jobId Job ID.
     * @param gridJob Hadoop job inmplementation.
     * @return Context of combine task with mock output.
     * @throws GridException
     */
    private GridHadoopTestTaskContext runMapCombineTask(GridHadoopFileBlock fileBlock, GridHadoopJobId jobId,
                                                        GridHadoopV2JobImpl gridJob) throws GridException {
        GridHadoopTestTaskContext mapCtx = new GridHadoopTestTaskContext(gridJob);

        GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.MAP, jobId, 0, 0, fileBlock);
        GridHadoopTask task = gridJob.createTask(taskInfo);
        task.run(mapCtx);

        //Prepare input for combine
        GridHadoopTestTaskContext combineCtx = new GridHadoopTestTaskContext(gridJob);
        combineCtx.makeTreeOfWritables(mapCtx.mockOutput());

        taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.COMBINE, jobId, 0, 0, null);
        task = gridJob.createTask(taskInfo);
        task.run(combineCtx);

        return combineCtx;
    }

    /**
     * Test all job in complex.
     * Runs 2 chain of map-combine and send result into one reduce task.
     * @throws IOException
     * @throws GridException
     */
    public void testAllTasks() throws IOException, GridException {
        Path outputDir = Files.createTempDirectory(GridGainWordCount2.class.getSimpleName() + "-output");

        try {
            URI testOutputDirURI = URI.create(outputDir.toString());

            File testInputFile = File.createTempFile(GridGainWordCount2.class.getSimpleName(), "-input");
            testInputFile.deleteOnExit();

            URI testInputFileURI = URI.create(testInputFile.getAbsolutePath());

            generateTestFile(testInputFile, "red", 100, "blue", 200, "green", 150, "yellow", 70);

            //Split file into two blocks
            Long l = testInputFile.length() / 2;
            GridHadoopFileBlock fileBlock1 = new GridHadoopFileBlock(null, testInputFileURI, 0, l);
            GridHadoopFileBlock fileBlock2 = new GridHadoopFileBlock(null, testInputFileURI, l, testInputFile.length() - l);

            Job hadoopJob = GridGainWordCount2.getJob(testInputFileURI.toString(), testOutputDirURI.toString());
            hadoopJob.setJobID(new JobID());

            GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);
            GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(hadoopJob.getConfiguration());

            GridHadoopV2JobImpl gridJob = new GridHadoopV2JobImpl(jobId, jobInfo);

            GridHadoopTestTaskContext combine1Ctx = runMapCombineTask(fileBlock1, jobId, gridJob);

            GridHadoopTestTaskContext combine2Ctx = runMapCombineTask(fileBlock2, jobId, gridJob);

            //Prepare input for combine
            GridHadoopTestTaskContext reduceCtx = new GridHadoopTestTaskContext(gridJob);
            reduceCtx.makeTreeOfWritables(combine1Ctx.mockOutput());
            reduceCtx.makeTreeOfWritables(combine2Ctx.mockOutput());

            GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(null, GridHadoopTaskType.REDUCE, jobId, 0, 0, null);
            GridHadoopTask task = gridJob.createTask(taskInfo);
            task.run(reduceCtx);

            assertEquals(
                "blue\t200\n" +
                "green\t150\n" +
                "red\t100\n" +
                "yellow\t70\n",
                readFile(outputDir + "/part-r-00000")
            );
        }
        finally {
            FileUtils.deleteDirectory(outputDir.toFile());
        }
    }

    /**
     * Generates text file with words. In one line there are from 5 to 9 words.
     * @param file file that there is generation for.
     * @param wordCounts pair word and count, i.e "hello", 2, "world", 3, etc.
     * @throws FileNotFoundException
     */
    private void generateTestFile(File file, Object... wordCounts) throws FileNotFoundException {
        List<String> wordsArr = new ArrayList<>();

        //Generating
        for (int i = 0; i < wordCounts.length; i += 2) {
            String word = (String) wordCounts[i];
            int cnt = (Integer) wordCounts[i + 1];

            while (cnt-- > 0) {
                wordsArr.add(word);
            }
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
