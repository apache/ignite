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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopFileSystemCounterWriter;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopPerformanceCounter;
import org.apache.ignite.internal.processors.hadoop.examples.HadoopWordCount1;
import org.apache.ignite.internal.processors.hadoop.examples.HadoopWordCount2;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.JOB_COUNTER_WRITER_PROPERTY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Test of error resiliency after an error in a map-reduce job execution.
 * Combinations tested:
 * { new ALI, old API }
 *   x { unchecked exception, checked exception, error }
 *   x { phase where the error happens }.
 */
public class HadoopMapReduceErrorResilienceTest extends HadoopAbstractWordCountTest {
    /** IGFS block size. */
    protected static final int IGFS_BLOCK_SIZE = 512 * 1024;

    /** Amount of blocks to prefetch. */
    protected static final int PREFETCH_BLOCKS = 1;

    /** Amount of sequential block reads before prefetch is triggered. */
    protected static final int SEQ_READS_BEFORE_PREFETCH = 2;

    /** Red constant. */
    protected static final int red = 10_000;
    /** Blue constant. */
    protected static final int blue = 20_000;
    /** Green constant. */
    protected static final int green = 15_000;
    /** Yellow constant. */
    protected static final int yellow = 7_000;

    /** Secondary file system URI. */
    protected static final String SECONDARY_URI = "igfs://igfs-secondary:grid-secondary@127.0.0.1:11500/";

    /** Secondary file system configuration path. */
    protected static final String SECONDARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml";

    /** The user to run Hadoop job on behalf of. */
    protected static final String USER = "vasya";

    /** Secondary IGFS name. */
    protected static final String SECONDARY_IGFS_NAME = "igfs-secondary";

    /** The secondary Ignite node. */
    protected Ignite igniteSecondary;

    /** The secondary Fs. */
    protected IgfsSecondaryFileSystem secondaryFs;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * Gets owner of a IgfsEx path.
     * @param p The path.
     * @return The owner.
     */
    private static String getOwner(IgfsEx i, IgfsPath p) {
        return i.info(p).property(IgfsUtils.PROP_USER_NAME);
    }

    /**
     * Gets owner of a secondary Fs path.
     * @param secFs The sec Fs.
     * @param p The path.
     * @return The owner.
     */
    private static String getOwnerSecondary(final IgfsSecondaryFileSystem secFs, final IgfsPath p) {
        return IgfsUserContext.doAs(USER, new IgniteOutClosure<String>() {
            @Override public String apply() {
                return secFs.info(p).property(IgfsUtils.PROP_USER_NAME);
            }
        });
    }

    /**
     * Checks owner of the path.
     * @param p The path.
     */
    private void checkOwner(IgfsPath p) {
        String ownerPrim = getOwner(igfs, p);
        assertEquals(USER, ownerPrim);

        String ownerSec = getOwnerSecondary(secondaryFs, p);
        assertEquals(USER, ownerSec);
    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryAfterAnError0_Runtime() throws Exception {
            doTestRecoveryAfterAnError(0, ErrorSimulator.Kind.Runtime);
    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryAfterAnError0_IOException() throws Exception {
            doTestRecoveryAfterAnError(0, ErrorSimulator.Kind.IOException);
    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryAfterAnError0_Error() throws Exception {
            doTestRecoveryAfterAnError(0, ErrorSimulator.Kind.Error);
    }

//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError1_Runtime() throws Exception {
//        doTestRecoveryAfterAnError(1, ErrorSimulator.Kind.Runtime);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError1_IOException() throws Exception {
//        doTestRecoveryAfterAnError(1, ErrorSimulator.Kind.IOException);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError1_Error() throws Exception {
//        doTestRecoveryAfterAnError(1, ErrorSimulator.Kind.Error);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError2_Runtime() throws Exception {
//        doTestRecoveryAfterAnError(2, ErrorSimulator.Kind.Runtime);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError2_IOException() throws Exception {
//        doTestRecoveryAfterAnError(2, ErrorSimulator.Kind.IOException);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError2_Error() throws Exception {
//        doTestRecoveryAfterAnError(2, ErrorSimulator.Kind.Error);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError3_Runtime() throws Exception {
//        doTestRecoveryAfterAnError(3, ErrorSimulator.Kind.Runtime);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError3_IOException() throws Exception {
//        doTestRecoveryAfterAnError(3, ErrorSimulator.Kind.IOException);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError3_Error() throws Exception {
//        doTestRecoveryAfterAnError(3, ErrorSimulator.Kind.Error);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError4_Runtime() throws Exception {
//        doTestRecoveryAfterAnError(4, ErrorSimulator.Kind.Runtime);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError4_IOException() throws Exception {
//        doTestRecoveryAfterAnError(4, ErrorSimulator.Kind.IOException);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError4_Error() throws Exception {
//        doTestRecoveryAfterAnError(4, ErrorSimulator.Kind.Error);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError5_Runtime() throws Exception {
//        doTestRecoveryAfterAnError(5, ErrorSimulator.Kind.Runtime);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError5_IOException() throws Exception {
//        doTestRecoveryAfterAnError(5, ErrorSimulator.Kind.IOException);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError5_Error() throws Exception {
//        doTestRecoveryAfterAnError(5, ErrorSimulator.Kind.Error);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError6_Runtime() throws Exception {
//        doTestRecoveryAfterAnError(6, ErrorSimulator.Kind.Runtime);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError6_IOException() throws Exception {
//        doTestRecoveryAfterAnError(6, ErrorSimulator.Kind.IOException);
//    }
//
//    /**
//     * Tests recovery.
//     *
//     * @throws Exception If failed.
//     */
//    public void testRecoveryAfterAnError6_Error() throws Exception {
//        doTestRecoveryAfterAnError(6, ErrorSimulator.Kind.Error);
//    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryAfterAnError7_Runtime() throws Exception {
        doTestRecoveryAfterAnError(7, ErrorSimulator.Kind.Runtime);
    }
    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryAfterAnError7_IOException() throws Exception {
        doTestRecoveryAfterAnError(7, ErrorSimulator.Kind.IOException);
    }
    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryAfterAnError7_Error() throws Exception {
        doTestRecoveryAfterAnError(7, ErrorSimulator.Kind.Error);
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        gc();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /**
     * Tests correct work after an error.
     *
     * @throws Exception
     */
    private void doTestRecoveryAfterAnError(int useNewBits, ErrorSimulator.Kind simulatorKind) throws Exception {
        try {
            IgfsPath inDir = new IgfsPath(PATH_INPUT);

            igfs.mkdirs(inDir);

            IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

            generateTestFile(inFile.toString(), "red", red, "blue", blue, "green", green, "yellow", yellow);

            boolean useNewMapper = (useNewBits & 1) == 0;
            boolean useNewCombiner = (useNewBits & 2) == 0;
            boolean useNewReducer = (useNewBits & 4) == 0;

            for (int i = 0; i < 12; i++) {
                System.out.println("############################ Simulator = " + simulatorKind + ", bits = " + i);

                int bits = 1 << i;

                ErrorSimulator sim = ErrorSimulator.create(simulatorKind, bits);

                doTestWithErrorSimulator(sim, inFile, useNewMapper, useNewCombiner, useNewReducer);
            }
        } catch (Throwable t) {
            t.printStackTrace();

            fail("Unexpected throwable: " + t);
        }
    }

    /**
     * Performs test with given error simulator.
     * @param sim The simulator.
     * @param inFile Input file.
     * @param useNewMapper If the use new mapper API.
     * @param useNewCombiner If to use new combiner.
     * @param useNewReducer If to use new reducer API.
     * @throws Exception If failed.
     */
    private void doTestWithErrorSimulator(ErrorSimulator sim, IgfsPath inFile, boolean useNewMapper,
            boolean useNewCombiner, boolean useNewReducer) throws Exception {
        // Set real simulating error simulator:
        assertTrue(ErrorSimulator.setInstance(ErrorSimulator.noopInstance, sim));

        try {
            // expect failure there:
            doTest(inFile, useNewMapper, useNewCombiner, useNewReducer);
        }
        catch (Throwable t) { // This may be an Error.
            // expected:
            t.printStackTrace(); // Ignore, continue the test.
        }

        // Set no-op error simulator:
        assertTrue(ErrorSimulator.setInstance(sim, ErrorSimulator.noopInstance));

        // but expect success there:
        doTest(inFile, useNewMapper, useNewCombiner, useNewReducer);

        gc();
    }

    /**
     * Experimental method to add cleanup upon on Hadoop task ending.
     * Currently left empty.
     *
     * @throws Exception
     */
    private static void gc() throws Exception {
        // Options that affect PermGen clanup: -XX:+UseConcMarkSweepGC -XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses
        //System.gc();
    }

    /**
     *
     *
     * @param useNewMapper
     * @param useNewCombiner
     * @param useNewReducer
     */
    private void doTest(IgfsPath inFile, boolean useNewMapper, boolean useNewCombiner, boolean useNewReducer)
            throws Exception {
        igfs.delete(new IgfsPath(PATH_OUTPUT), true);

        JobConf jobConf = new JobConf();

        jobConf.set(JOB_COUNTER_WRITER_PROPERTY, IgniteHadoopFileSystemCounterWriter.class.getName());
        jobConf.setUser(USER);
        jobConf.set(IgniteHadoopFileSystemCounterWriter.COUNTER_WRITER_DIR_PROPERTY, "/xxx/${USER}/zzz");

        //To split into about 40 items for v2
        jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

        //For v1
        jobConf.setInt("fs.local.block.size", 65000);

        // File system coordinates.
        setupFileSystems(jobConf);

        HadoopWordCount1.setTasksClasses(jobConf, !useNewMapper, !useNewCombiner, !useNewReducer);

        Job job = Job.getInstance(jobConf);

        HadoopWordCount2.setTasksClasses(job, useNewMapper, useNewCombiner, useNewReducer, compressOutputSnappy());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(igfsScheme() + inFile.toString()));
        FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT));

        job.setJarByClass(HadoopWordCount2.class);

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

        fut.get();

        checkJobStatistics(jobId);

        final String outFile = PATH_OUTPUT + "/" + (useNewReducer ? "part-r-" : "part-") + "00000";

        checkOwner(new IgfsPath(PATH_OUTPUT + "/" + "_SUCCESS"));

        checkOwner(new IgfsPath(outFile));

        String actual = readAndSortFile(outFile, job.getConfiguration());

        assertEquals("Use new mapper: " + useNewMapper + ", new combiner: " + useNewCombiner + ", new reducer: " +
                useNewReducer,
            "blue\t" + blue + "\n" +
                "green\t" + green + "\n" +
                "red\t" + red + "\n" +
                "yellow\t" + yellow + "\n",
            actual
        );
    }

    /**
     * Gets if to compress output data with Snappy.
     *
     * @return If to compress output data with Snappy.
     */
    protected boolean compressOutputSnappy() {
        return false;
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

        final IgfsPath statPath = new IgfsPath("/xxx/" + USER + "/zzz/" + jobId + "/performance");

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return igfs.exists(statPath);
            }
        }, 20_000);

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        igniteSecondary = startGridWithIgfs("grid-secondary", SECONDARY_IGFS_NAME, PRIMARY, null, SECONDARY_REST_CFG);

        super.beforeTest();
    }

    /**
     * Start grid with IGFS.
     *
     * @param gridName Grid name.
     * @param igfsName IGFS name
     * @param mode IGFS mode.
     * @param secondaryFs Secondary file system (optional).
     * @param restCfg Rest configuration string (optional).
     * @return Started grid instance.
     * @throws Exception If failed.
     */
    protected Ignite startGridWithIgfs(String gridName, String igfsName, IgfsMode mode,
        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg) throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(igfsName);
        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);
        igfsCfg.setDefaultMode(mode);
        igfsCfg.setIpcEndpointConfiguration(restCfg);
        igfsCfg.setSecondaryFileSystem(secondaryFs);
        igfsCfg.setPrefetchBlocks(PREFETCH_BLOCKS);
        igfsCfg.setSequentialReadsBeforePrefetch(SEQ_READS_BEFORE_PREFETCH);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setOffHeapMaxMemory(0);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return G.start(cfg);
    }

    /**
     * @return IGFS configuration.
     */
    @Override public FileSystemConfiguration igfsConfiguration() throws Exception {
        FileSystemConfiguration fsCfg = super.igfsConfiguration();

        secondaryFs = new IgniteHadoopIgfsSecondaryFileSystem(SECONDARY_URI, SECONDARY_CFG);

        fsCfg.setSecondaryFileSystem(secondaryFs);

        return fsCfg;
    }
}