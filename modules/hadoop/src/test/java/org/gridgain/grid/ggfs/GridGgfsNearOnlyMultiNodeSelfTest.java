/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test hadoop file system implementation.
 */
public class GridGgfsNearOnlyMultiNodeSelfTest extends GridCommonAbstractTest {
    /** Path to the default hadoop configuration. */
    public static final String HADOOP_FS_CFG = "examples/config/hadoop/core-site.xml";

    /** Group size. */
    public static final int GRP_SIZE = 128;

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Node count. */
    private int cnt;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(nodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("partitioned");
        ggfsCfg.setName("ggfs");

        ggfsCfg.setIpcEndpointConfiguration(GridHadoopTestUtils.jsonToMap(
            "{type:'shmem', port:" + (GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + cnt) + "}"));

        ggfsCfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.

        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cnt++;

        return cfg;
    }

    /** @return Node count for test. */
    protected int nodeCount() {
        return 4;
    }

    /**
     * Gets cache configuration.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected GridCacheConfiguration cacheConfiguration(String gridName) {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(cnt == 0 ? NEAR_ONLY : PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /**
     * Gets config of concrete File System.
     *
     * @return Config of concrete File System.
     */
    protected Configuration getFileSystemConfig() {
        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveGridGainUrl(HADOOP_FS_CFG));

        return cfg;
    }

    /**
     * Gets File System name.
     *
     * @param grid Grid index.
     * @return File System name.
     */
    protected URI getFileSystemURI(int grid) {
        try {
            return new URI("ggfs://127.0.0.1:" + (GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + grid));
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /** @throws Exception If failed. */
    public void testContentsConsistency() throws Exception {
        try (FileSystem fs = FileSystem.get(getFileSystemURI(0), getFileSystemConfig())) {
            Collection<GridBiTuple<String, Long>> files = F.asList(
                F.t("/dir1/dir2/file1", 1024L),
                F.t("/dir1/dir2/file2", 8 * 1024L),
                F.t("/dir1/file1", 1024 * 1024L),
                F.t("/dir1/file2", 5 * 1024 * 1024L),
                F.t("/file1", 64 * 1024L + 13),
                F.t("/file2", 13L),
                F.t("/file3", 123764L)
            );

            for (GridBiTuple<String, Long> file : files) {

                info("Writing file: " + file.get1());

                try (OutputStream os = fs.create(new Path(file.get1()), (short)3)) {
                    byte[] data = new byte[file.get2().intValue()];

                    data[0] = 25;
                    data[data.length - 1] = 26;

                    os.write(data);
                }

                info("Finished writing file: " + file.get1());
            }

            for (int i = 1; i < nodeCount(); i++) {

                try (FileSystem ignored = FileSystem.get(getFileSystemURI(i), getFileSystemConfig())) {
                    for (GridBiTuple<String, Long> file : files) {
                        Path path = new Path(file.get1());

                        FileStatus fileStatus = fs.getFileStatus(path);

                        assertEquals(file.get2(), (Long)fileStatus.getLen());

                        byte[] read = new byte[file.get2().intValue()];

                        info("Reading file: " + path);

                        try (FSDataInputStream in = fs.open(path)) {
                            in.readFully(read);

                            assert read[0] == 25;
                            assert read[read.length - 1] == 26;
                        }

                        info("Finished reading file: " + path);
                    }
                }
            }
        }
    }
}
