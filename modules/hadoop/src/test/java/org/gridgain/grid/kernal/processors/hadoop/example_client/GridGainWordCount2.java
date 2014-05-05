/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.example_client;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.hadoop.client.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.TRANSACTIONAL;
import static org.gridgain.grid.cache.GridCacheDistributionMode.NEAR_PARTITIONED;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.FULL_SYNC;
import static org.gridgain.grid.ggfs.GridGgfsMode.PRIMARY;
import static org.gridgain.grid.util.ipc.shmem.GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * Example job for testing hadoop task execution.
 */
public class GridGainWordCount2 {
    /**
     * Entry point to start job.
     *
     * @param args Command line parameters.
     * @throws Exception If fails.
     */
    public static void main(String[] args) throws Exception {
        try {
            // Start node with GGFS.
            Grid grid = G.start(gridConfiguration());

            GridGgfs ggfs = grid.ggfs("ggfs");

            ggfs.mkdirs(new GridGgfsPath("/input"));

            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(ggfs.create(
                new GridGgfsPath("/input/test.file"), true)))) {
                bw.write("Hello, world!");
            }

            Job job = getJob("/input", "/output");

            job.submit();
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Gets fully configured Job instance.
     *
     * @param input Input file name.
     * @param output Output directory name.
     * @return Job instance.
     * @throws java.io.IOException If fails.
     */
    public static Job getJob(String input, String output) throws IOException {
        Configuration conf = new Configuration();

        conf.set(MRConfig.FRAMEWORK_NAME, GridHadoopClientProtocol.PROP_FRAMEWORK_NAME);
        conf.set(GridHadoopClientProtocol.PROP_SRV_HOST, "127.0.0.1");
        conf.setInt(GridHadoopClientProtocol.PROP_SRV_PORT, 11212);

        conf.set("fs.default.name", "ggfs://ipc");
        conf.set("fs.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem");

        Job job = Job.getInstance(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(GridGainWordCount2Mapper.class);
        job.setCombinerClass(GridGainWordCount2Reducer.class);
        job.setReducerClass(GridGainWordCount2Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(GridGainWordCount2.class);
        return job;
    }

    /**
     * Create grid configuration.
     *
     * @return Grid configuration.
     */
    private static GridConfiguration gridConfiguration() {
        GridConfiguration cfg = new GridConfiguration();

        cfg.setRestEnabled(true);
        cfg.setRestTcpPort(11212);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setLocalHost("127.0.0.1");

        GridCacheConfiguration dataCacheCfg = new GridCacheConfiguration();

        dataCacheCfg.setStartSize(1024);
        dataCacheCfg.setQueryIndexEnabled(true);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setDistributionMode(NEAR_PARTITIONED);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setEvictionPolicy(null);
        dataCacheCfg.setName("partitioned");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = new GridCacheConfiguration();

        metaCacheCfg.setStartSize(1024);
        metaCacheCfg.setQueryIndexEnabled(true);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
        metaCacheCfg.setDistributionMode(NEAR_PARTITIONED);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        metaCacheCfg.setEvictionPolicy(null);
        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration hadoopCacheCfg = new GridCacheConfiguration();

        hadoopCacheCfg.setCacheMode(REPLICATED);
        hadoopCacheCfg.setAtomicWriteOrderMode(GridCacheAtomicWriteOrderMode.PRIMARY);
        hadoopCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        hadoopCacheCfg.setName("hadoop-sys");

        cfg.setCacheConfiguration(metaCacheCfg, dataCacheCfg, hadoopCacheCfg);

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setPrefetchBlocks(1);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.
        ggfsCfg.setManagementPort(-1);
        ggfsCfg.setIpcEndpointEnabled(true);
        ggfsCfg.setIpcEndpointConfiguration("{type:'tcp', port:" + DFLT_IPC_PORT + "}");

        cfg.setGgfsConfiguration(ggfsCfg);

        GridHadoopConfiguration hadoopCfg = new GridHadoopConfiguration();

        hadoopCfg.setSystemCacheName("hadoop-sys");
        hadoopCfg.setJobFactory(new GridHadoopDefaultJobFactory());

        cfg.setHadoopConfiguration(hadoopCfg);

        return cfg;
    }
}
