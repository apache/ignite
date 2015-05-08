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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.*;
import org.apache.ignite.hadoop.fs.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.hadoop.counter.*;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.examples.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.*;

/**
 * Test of whole cycle of map-reduce processing via Job tracker.
 */
public class HadoopMapReduceTest extends HadoopAbstractWordCountTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * Tests whole job execution with all phases in all combination of new and old versions of API.
     * @throws Exception If fails.
     */
    public void testWholeMapReduceExecution() throws Exception {
        IgfsPath inDir = new IgfsPath(PATH_INPUT);

        igfs.mkdirs(inDir);

        IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

        generateTestFile(inFile.toString(), "red", 100000, "blue", 200000, "green", 150000, "yellow", 70000 );

        for (int i = 0; i < 8; i++) {
            igfs.delete(new IgfsPath(PATH_OUTPUT), true);

            boolean useNewMapper = (i & 1) == 0;
            boolean useNewCombiner = (i & 2) == 0;
            boolean useNewReducer = (i & 4) == 0;

            JobConf jobConf = new JobConf();

            jobConf.set(JOB_COUNTER_WRITER_PROPERTY, IgniteHadoopFileSystemCounterWriter.class.getName());
            jobConf.setUser("yyy");
            jobConf.set(IgniteHadoopFileSystemCounterWriter.COUNTER_WRITER_DIR_PROPERTY, "/xxx/${USER}/zzz");

            //To split into about 40 items for v2
            jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

            //For v1
            jobConf.setInt("fs.local.block.size", 65000);

            // File system coordinates.
            setupFileSystems(jobConf);

            HadoopWordCount1.setTasksClasses(jobConf, !useNewMapper, !useNewCombiner, !useNewReducer);

            Job job = Job.getInstance(jobConf);

            HadoopWordCount2.setTasksClasses(job, useNewMapper, useNewCombiner, useNewReducer);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(igfsScheme() + inFile.toString()));
            FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT));

            job.setJarByClass(HadoopWordCount2.class);

            HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

            IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

            fut.get();

            checkJobStatistics(jobId);

            assertEquals("Use new mapper: " + useNewMapper + ", new combiner: " + useNewCombiner + ", new reducer: " +
                useNewReducer,
                "blue\t200000\n" +
                "green\t150000\n" +
                "red\t100000\n" +
                "yellow\t70000\n",
                readAndSortFile(PATH_OUTPUT + "/" + (useNewReducer ? "part-r-" : "part-") + "00000")
            );
        }
    }

    /**
     * Simple test job statistics.
     *
     * @param jobId Job id.
     * @throws IgniteCheckedException
     */
    private void checkJobStatistics(HadoopJobId jobId) throws IgniteCheckedException, IOException {
        HadoopCounters cntrs = grid(0).hadoop().counters(jobId);

        HadoopPerformanceCounter perfCntr = HadoopPerformanceCounter.getCounter(cntrs, null);

        Map<String, SortedMap<Integer,Long>> tasks = new TreeMap<>();

        Map<String, Integer> phaseOrders = new HashMap<>();
        phaseOrders.put("submit", 0);
        phaseOrders.put("prepare", 1);
        phaseOrders.put("start", 2);
        phaseOrders.put("Cstart", 3);
        phaseOrders.put("finish", 4);

        String prevTaskId = null;

        long apiEvtCnt = 0;

        for (T2<String, Long> evt : perfCntr.evts()) {
            //We expect string pattern: COMBINE 1 run 7fa86a14-5a08-40e3-a7cb-98109b52a706
            String[] parsedEvt = evt.get1().split(" ");

            String taskId;
            String taskPhase;

            if ("JOB".equals(parsedEvt[0])) {
                taskId = parsedEvt[0];
                taskPhase = parsedEvt[1];
            }
            else {
                taskId = ("COMBINE".equals(parsedEvt[0]) ? "MAP" : parsedEvt[0].substring(0, 3)) + parsedEvt[1];
                taskPhase = ("COMBINE".equals(parsedEvt[0]) ? "C" : "") + parsedEvt[2];
            }

            if (!taskId.equals(prevTaskId))
                tasks.put(taskId, new TreeMap<Integer,Long>());

            Integer pos = phaseOrders.get(taskPhase);

            assertNotNull("Invalid phase " + taskPhase, pos);

            tasks.get(taskId).put(pos, evt.get2());

            prevTaskId = taskId;

            apiEvtCnt++;
        }

        for (Map.Entry<String ,SortedMap<Integer,Long>> task : tasks.entrySet()) {
            Map<Integer, Long> order = task.getValue();

            long prev = 0;

            for (Map.Entry<Integer, Long> phase : order.entrySet()) {
                assertTrue("Phase order of " + task.getKey() + " is invalid", phase.getValue() >= prev);

                prev = phase.getValue();
            }
        }

        final IgfsPath statPath = new IgfsPath("/xxx/yyy/zzz/" + jobId + "/performance");

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return igfs.exists(statPath);
            }
        }, 10000);

        final long apiEvtCnt0 = apiEvtCnt;

        boolean res = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(igfs.open(statPath)))) {
                        return apiEvtCnt0 == HadoopTestUtils.simpleCheckJobStatFile(reader);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 10000);

        if (!res) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(igfs.open(statPath)));

            assert false : "Invalid API events count [exp=" + apiEvtCnt0 +
                ", actual=" + HadoopTestUtils.simpleCheckJobStatFile(reader) + ']';
        }
    }
}
