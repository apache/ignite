package org.apache.ignite.internal.processors.hadoop.impl;

import java.net.URI;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraGen;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraInputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraOutputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraSort;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraValidate;

import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 * Implements TeraSort Hadoop sample as a unit test.
 */
public class HadoopTeraSortTest extends HadoopAbstractSelfTest {
    /**  Out destination dir. */
    protected final String generateOutDir = getFsBase() + "/tera-generated";

    /** Sort destination dir. */
    protected final String sortOutDir = getFsBase() + "/tera-sorted";

    /** Validation destination dir. */
    protected final String validateOutDir = getFsBase() + "/tera-validated";

    /**
     * Gets base directory.
     * @return The base directory.
     */
    protected String getFsBase() {
        return "file:///tmp/" + getUser() + "/hadoop-test";
    }

    /**
     * @return Full input data size, in bytes.
     */
    protected long dataSizeBytes() {
        return 100_000_000;
    }

    /**
     * Desired number of maps in TeraSort job.
     * @return The number of maps.
     */
    protected int numMaps() {
        return gridCount() * 10;
    }

    /**
     * Desired number of reduces in TeraSort job.
     * @return The number of reduces.
     */
    protected int numReduces() {
        return gridCount() * 8;
    }

    /**
     * The user to run Hadoop job on behalf of.
     * @return The user to run Hadoop job on behalf of.
     */
    protected String getUser() {
        return System.getProperty("user.name");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected final boolean igfsEnabled() {
        return false;
    }

    /**
     * Does actual test TeraSort job Through Ignite API
     */
    protected final void teraSort() throws Exception {
        System.out.println("TeraSort ===============================================================");

        getFileSystem().delete(new Path(sortOutDir), true);

        final JobConf jobConf = new JobConf();

        jobConf.setUser(getUser());

        jobConf.set("fs.defaultFS", getFsBase());

        log().info("Desired number of reduces: " + numReduces());

        jobConf.set("mapreduce.job.reduces", String.valueOf(numReduces()));

        log().info("Desired number of maps: " + numMaps());

        final long splitSize = dataSizeBytes() / numMaps();

        log().info("Desired split size: " + splitSize);

        // Force the split to be of the desired size:
        jobConf.set("mapred.min.split.size", String.valueOf(splitSize));
        jobConf.set("mapred.max.split.size", String.valueOf(splitSize));

        Job job = setupConfig(jobConf); //Job.getInstance(jobConf);

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

        fut.get();
    }

    /**
     * Gets the file system we work upon.
     * @return The file system.
     * @throws Exception
     */
    FileSystem getFileSystem() throws Exception{
        return FileSystem.get(new URI(getFsBase()), new Configuration());
    }

    /**
     * Represents the data generation stage.
     * @throws Exception
     */
    private void teraGenerate() throws Exception {
        System.out.println("TeraGenerate ===============================================================");

        getFileSystem().delete(new Path(generateOutDir), true);

        final long numLines = dataSizeBytes() / 100; // TeraGen makes 100 bytes ber line

        if (numLines < 1)
            throw new IllegalStateException("Data size is too small: " + dataSizeBytes());

        // Generate input data:
        int ret = TeraGen.mainImpl(String.valueOf(numLines), generateOutDir);

        assertEquals(0, ret);

        FileStatus[] fileStatuses = getFileSystem().listStatus(new Path(generateOutDir));

        long sumLen = 0;

        for (FileStatus fs: fileStatuses)
            sumLen += fs.getLen();

        assertEquals(dataSizeBytes(), sumLen); // Ensure correct size data is generated.
    }

    /**
     * Creates Job instance and sets up necessary properties for it.
     * @param conf The Job config.
     * @return The job.
     * @throws Exception On error.
     */
    private Job setupConfig(JobConf conf) throws Exception {
        Job job = Job.getInstance(conf);

        Path inputDir = new Path(generateOutDir);
        Path outputDir = new Path(sortOutDir);

        boolean useSimplePartitioner = TeraSort.getUseSimplePartitioner(job);
        TeraInputFormat.setInputPaths(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setJobName("TeraSort");
        //job.setJarByClass(TeraSort.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TeraInputFormat.class);
        job.setOutputFormatClass(TeraOutputFormat.class);
        if (useSimplePartitioner)
            job.setPartitionerClass(TeraSort.SimplePartitioner.class);
        else {
            long start = System.currentTimeMillis();
            Path partitionFile = new Path(outputDir, TeraInputFormat.PARTITION_FILENAME);
            URI partitionUri = new URI(partitionFile.toString() +
                "#" + TeraInputFormat.PARTITION_FILENAME);
            try {
                TeraInputFormat.writePartitionFile(job, partitionFile);
            }
            catch (Throwable e) {
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
        // int ret = job.waitForCompletion(true) ? 0 : 1;

        return job;
    }

    /**
     * Implements validation phase of the sample.
     * @throws Exception
     */
    private void teraValidate() throws Exception {
        System.out.println("TeraValidate ===============================================================");

        getFileSystem().delete(new Path(validateOutDir), true);

        // Generate input data:
        int ret = TeraValidate.mainImpl(sortOutDir, validateOutDir);

        assertEquals(0, ret);

        FileStatus[] fileStatuses = getFileSystem().listStatus(new Path(validateOutDir), new PathFilter() {
            @Override public boolean accept(Path path) {
                // Typically name is "part-r-00000":
                return path.getName().startsWith("part-r-");
            }
        });

        // TeraValidate has only 1 reduce, so should be only 1 result file:
        assertEquals(1, fileStatuses.length);

        // The result file must contain only 1 line with the checksum, like this:
        // "checksum        7a27e2d0d55de",
        // typically it has length of 23 bytes.
        // If sorting was not correct, the result contains list of K-V pairs that are not ordered correctly.
        // In such case the size of the output will be much larger.
        long len = fileStatuses[0].getLen();

        assertTrue("TeraValidate length: " + len, len >= 16 && len <= 32);
    }


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());
    }

    /**
     * Runs generate/sort/validate phases of the terasort sample.
     * @throws Exception
     */
    public void testTeraSort() throws Exception {
        teraGenerate();

        teraSort();

        teraValidate();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration igc = super.getConfiguration(gridName);

        HadoopConfiguration hc = createHadoopConfiguration();

        igc.setHadoopConfiguration(hc);

        return igc;
    }

    /**
     * Creates Hadoop configuration for the test.
     * @return The {@link HadoopConfiguration}.
     */
    protected HadoopConfiguration createHadoopConfiguration() {
        HadoopConfiguration hadoopCfg = new HadoopConfiguration();

        // See org.apache.ignite.configuration.HadoopConfiguration.DFLT_MAX_TASK_QUEUE_SIZE
        hadoopCfg.setMaxTaskQueueSize(30_000);

        return hadoopCfg;
    }
}
