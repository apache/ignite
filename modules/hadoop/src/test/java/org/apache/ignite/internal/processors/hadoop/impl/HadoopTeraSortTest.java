package org.apache.ignite.internal.processors.hadoop.impl;

import java.net.URI;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraGen;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraInputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraOutputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraSort;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraValidate;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 *
 */
public class HadoopTeraSortTest extends HadoopAbstractSelfTest {
    /** The user to run Hadoop job on behalf of. */
    //protected static final String USER = "ivan";

    /**
     * NB: Job data size in bytes X = NUM_LINES * 100;
     */
    private static final int NUM_LINES = 1_000_000;

    //private static final String FS_BASE = "hdfs://testagent09:9000";
    private static final String FS_BASE = "file:///tmp/hadoop-test/";

    private static final String GENERATE_OUT_DIR = FS_BASE + "/tmp/tera-generated/";
    public static final String SORT_OUT_DIR     = FS_BASE + "/tmp/tera-sorted/";
    private static final String VALIDATE_OUT_DIR = FS_BASE + "/tmp/tera-validated/";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

//        Configuration cfg = new Configuration();
//
//        setupFileSystems(cfg);
//
//        // Init cache by correct LocalFileSystem implementation
//        FileSystem.getLocal(cfg);
    }

    @Override protected void setupFileSystems(Configuration cfg) {
//        //cfg.set("fs.defaultFS", "file:///");
//        cfg.set("fs.defaultFS", FS_BASE);
//
//        // TODO: Not sure if we really need that:
//        HadoopFileSystemsUtils.setupFileSystems(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected final boolean igfsEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    } // initial was 3

    /**
     * Does actual test TeraSort job Through Ignite API
     */
    protected final void teraSort() throws Exception {
        System.out.println("TeraSort ===============================================================");

        getFileSystem().delete(new Path(SORT_OUT_DIR), true);

        JobConf jobConf = new JobConf();

        //jobConf.setUser(USER);
        jobConf.set("fs.defaultFS", FS_BASE);
        // TODO: !!! INVESTIGATION: Test new impl:
        jobConf.set("ignite.shuffle.mapper.stripe.output", "true");
        jobConf.set("ignite.shuffle.striped.direct", "true");
        jobConf.set("mapreduce.job.reduces", "24");
        jobConf.set("mapred.min.split.size", "1000000");
        jobConf.set("mapred.max.split.size", "1000000");

        Job job = setupConfig(jobConf); //Job.getInstance(jobConf);

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

        fut.get();
    }

    FileSystem getFileSystem() throws Exception{
        return FileSystem.get(new URI(FS_BASE), new Configuration());
    }

    private void teraGenerate() throws Exception {
        System.out.println("TeraGenerate ===============================================================");
        getFileSystem().delete(new Path(GENERATE_OUT_DIR), true);

        // Generate input data:
        int ret = TeraGen.mainImpl(String.valueOf(NUM_LINES), GENERATE_OUT_DIR);

        assertEquals(0, ret);

        //getFileSystem().listStatus(); TODO: list to make sure gen ok.
    }

    private Job setupConfig(JobConf conf) throws Exception {
            Job job = Job.getInstance(conf);

            Path inputDir = new Path(GENERATE_OUT_DIR);
            Path outputDir = new Path(SORT_OUT_DIR);

            boolean useSimplePartitioner = TeraSort.getUseSimplePartitioner(job);
            TeraInputFormat.setInputPaths(job, inputDir);
            FileOutputFormat.setOutputPath(job, outputDir);
            job.setJobName("TeraSort");
            //job.setJarByClass(TeraSort.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TeraInputFormat.class);
            job.setOutputFormatClass(TeraOutputFormat.class);
            if (useSimplePartitioner) {
                job.setPartitionerClass(TeraSort.SimplePartitioner.class);
            } else {
                long start = System.currentTimeMillis();
                Path partitionFile = new Path(outputDir,
                    TeraInputFormat.PARTITION_FILENAME);
                URI partitionUri = new URI(partitionFile.toString() +
                    "#" + TeraInputFormat.PARTITION_FILENAME);
                try {
                    TeraInputFormat.writePartitionFile(job, partitionFile);
                } catch (Throwable e) {
//                    LOG.error(e.getMessage());
//                    return -1;
                    throw new RuntimeException(e);
                }
                job.addCacheFile(partitionUri);
                long end = System.currentTimeMillis();
                System.out.println("Spent " + (end - start) + "ms computing partitions.");
                job.setPartitionerClass(TeraSort.TotalOrderPartitioner.class);
            }

            job.getConfiguration().setInt("dfs.replication", TeraSort.getOutputReplication(job));
            TeraOutputFormat.setFinalSync(job, true);
//            int ret = job.waitForCompletion(true) ? 0 : 1;

            return job;
        }

    private void teraValidate() throws Exception {
        System.out.println("TeraValidate ===============================================================");

        getFileSystem().delete(new Path(VALIDATE_OUT_DIR), true);

        // Generate input data:
        int ret = TeraValidate.mainImpl(SORT_OUT_DIR, VALIDATE_OUT_DIR);

        assertEquals(0, ret);
    }


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());
    }

    public void testTeraSort() throws Exception {
        teraGenerate();

        teraSort();

        teraValidate();
    }

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration igc = super.getConfiguration(gridName);

        HadoopConfiguration hc = createHadoopConfiguration();

        igc.setHadoopConfiguration(hc);

        return igc;
    }

    protected HadoopConfiguration createHadoopConfiguration() {
        HadoopConfiguration hadoopCfg = new HadoopConfiguration();

        // See org.apache.ignite.configuration.HadoopConfiguration.DFLT_MAX_TASK_QUEUE_SIZE
        hadoopCfg.setMaxTaskQueueSize(30_000);

        //hadoopCfg.setMaxParallelTasks(); // 16 = ProcCodes * 2 -- default

        return hadoopCfg;
    }

    @Override protected long getTestTimeout() {
        return 30 * 60 * 1000; // 30 min
    }
}
