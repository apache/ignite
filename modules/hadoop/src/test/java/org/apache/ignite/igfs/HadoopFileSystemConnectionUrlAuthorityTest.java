package org.apache.ignite.igfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.hadoop.SecondaryFileSystemProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.IgniteFileSystem.IGFS_SCHEME;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.*;

/**
 * Checks various cases of IGFS connection authority.
 */
public class HadoopFileSystemConnectionUrlAuthorityTest extends GridCommonAbstractTest {
    /**
     * Starts the Ignite node with IGFS.
     *
     * @param gridName The Ignite node name.
     * @param igfsName IGFS name.
     * @param port The IGFS port.
     */
    private void startIgnite(String gridName, String igfsName, int port) throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("data-cache");
        igfsCfg.setMetaCacheName("meta-cache");
        igfsCfg.setName(igfsName);

        IgfsIpcEndpointConfiguration endpoint = new IgfsIpcEndpointConfiguration();
        endpoint.setType(IgfsIpcEndpointType.TCP);
        endpoint.setPort(port);

        igfsCfg.setIpcEndpointConfiguration(endpoint);
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setPrefetchBlocks(1);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("data-cache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("meta-cache");
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, dataCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);
        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSharedMemoryPort(-1);
        cfg.setCommunicationSpi(commSpi);

        G.start(cfg);
    }

    /**
     * Connects to IGFS file system with given authority URI part.
     *
     * @param igfsAuthority The authority.
     * @return The Hadoop file system.
     * @throws IOException On error.
     */
    private IgniteBiTuple<FileSystem, URI> connect(String igfsAuthority) throws IOException {
        Configuration secondaryConf = configuration(IGFS_SCHEME, igfsAuthority, true, true);

        secondaryConf.setInt("fs.igfs.block.size", 1024);

        String igfsCfgPath = writeConfiguration(secondaryConf, "/work/core-site-test.xml");

        String uriStr = mkUri(IGFS_SCHEME, igfsAuthority);

        SecondaryFileSystemProvider provider =
            new SecondaryFileSystemProvider(uriStr, igfsCfgPath);

        FileSystem fs = provider.createFileSystem(null/*user*/);

        URI fsURI = provider.uri();

        return new IgniteBiTuple<>(fs, fsURI);
    }

    /**
     * Creates a file and checks it.
     *
     * @param fs The file system.
     * @param uri The uri.
     * @throws Exception
     */
    private void checkFile(final FileSystem fs, URI uri) throws Exception {
        Path fsHome = new Path(uri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");
        final Path file = new Path(dir, "someFile");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(file);
            }
        }, FileNotFoundException.class, null);


        FsPermission fsPerm = new FsPermission((short)644);

        FSDataOutputStream os = fs.create(file, fsPerm, false, 1, (short)1, 1L, null);

        // Try to write something in file.
        os.write("abc".getBytes());

        os.close();

        // Check file status.
        FileStatus fileStatus = fs.getFileStatus(file);

        assertFalse(fileStatus.isDir());
        assertEquals(file, fileStatus.getPath());
        assertEquals(fsPerm, fileStatus.getPermission());
    }

    /**
     * Closes file system.
     *
     * @throws Exception
     */
    private void close(FileSystem fs) throws Exception {
        if (fs != null) {
            try {
                fs.delete(new Path("/"), true);
            }
            catch (Exception ignore) {
                // No-op.
            }

            U.closeQuiet(fs);
        }
    }

    /**
     * Test implementation.
     *
     * @param igfsName IGFS name.
     * @param gridName Grid name.
     * @param authorities authorities.
     * @param expectOk Expected result.
     * @throws Exception On error.
     */
    private void testImpl(String igfsName, String gridName, String[] authorities, boolean[] expectOk) throws Exception {
        assert authorities.length == expectOk.length;

        String auth;
        boolean expOk;

        for (int i = 0; i<authorities.length; i++) {
            startIgnite(gridName, igfsName, 10500);

            auth = authorities[i];
            expOk = expectOk[i];

            X.println("### Index: " + i);
            X.println("### Authority: [" + auth + "]");

            try {
                IgniteBiTuple<FileSystem,URI> t2 = connect(auth);

                FileSystem fs = t2.get1();
                URI fsURI = t2.get2();

                X.println("### Fs URI: [" + fsURI + "]");

                try {
                    checkFile(fs, fsURI);
                }
                finally {
                    close(fs);
                }

                if (!expOk)
                    fail("Exception expected.");
            }
            catch (IOException ioe) {
                if (expOk)
                    throw ioe;
            }

            G.stopAll(true);
        }
    }

    /**
     * Both Grid and IGFS are not null.
     *
     * @throws Exception On error.
     */
    public void testBothNotNull() throws Exception {
        testImpl("igfs-name", "grid-name",
            new String[] { ":@:10500", "", "igfs-name:grid-name@localhost:10500", "igfs-name:grid-name@" },
            new boolean[] { true, true, true, true, });
    }

    /**
     * Null Grid name.
     *
     * @throws Exception On error.
     */
    public void testNullGrid() throws Exception {
        testImpl("igfs-name", null,
            new String[] { "",  "igfs-name:@localhost:10500", "igfs-name@" },
            new boolean[] { true, true, true } );
    }

    /**
     * Null IGFS name.
     *
     * @throws Exception On error.
     */
    public void testNullIgfs() throws Exception {
        testImpl(null, "grid-name",
            new String[] { "", "localhost:10500", "grid-name@localhost:10500", ":grid-name@localhost:10500" },
            new boolean[] { true, true, false/*In this case "grid-name" is treated as IGFS name.*/, true });
    }

    /**
     * Both IGFS and Grid names are null.
     *
     * @throws Exception On error.
     */
    public void testBothNull() throws Exception {
        testImpl(null, null,
            new String[] { "", ":@", "@:", ":@:10500", ":@localhost", "localhost:10500",
                "@localhost:10500", ":@localhost:10500", "foo:moo@" },
            new boolean[] { true, true, false/*empty port*/, true, true, true, true, true, false, });
    }
}
