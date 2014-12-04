/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.ggfs.mapreduce.records.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;

/**
 * Tests for {@link GridGgfsTask}.
 */
public class GridGgfsTaskSelfTest extends GridGgfsCommonAbstractTest {
    /** Predefined words dictionary. */
    private static final String[] DICTIONARY = new String[] {"word0", "word1", "word2", "word3", "word4", "word5",
        "word6", "word7"};

    /** File path. */
    private static final GridGgfsPath FILE = new GridGgfsPath("/file");

    /** Shared IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Block size: 64 Kb. */
    private static final int BLOCK_SIZE = 64 * 1024;

    /** Total words in file. */
    private static final int TOTAL_WORDS = 2 * 1024 * 1024;

    /** Node count */
    private static final int NODE_CNT = 4;

    /** Repeat count. */
    private static final int REPEAT_CNT = 10;

    /** GGFS. */
    private static IgniteFs ggfs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < NODE_CNT; i++) {
            Ignite g = G.start(config(i));

            if (i + 1 == NODE_CNT)
                ggfs = g.fileSystem("ggfs");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ggfs.format();
    }

    /**
     * Create grid configuration.
     *
     * @param idx Node index.
     * @return Grid configuration
     */
    private IgniteConfiguration config(int idx) {
        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(BLOCK_SIZE);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setFragmentizerEnabled(false);

        GridCacheConfiguration dataCacheCfg = new GridCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setDistributionMode(PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(1));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);

        GridCacheConfiguration metaCacheCfg = new GridCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);

        IgniteConfiguration cfg = new IgniteConfiguration();

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setGridName("node-" + idx);

        return cfg;
    }

    /**
     * Test task.
     *
     * @throws Exception If failed.
     */
    public void testTask() throws Exception {
        U.sleep(3000); // TODO: Sleep in order to wait for fragmentizing to finish.

        for (int i = 0; i < REPEAT_CNT; i++) {
            String arg = DICTIONARY[new Random(System.currentTimeMillis()).nextInt(DICTIONARY.length)];

            generateFile(TOTAL_WORDS);
            Long genLen = ggfs.info(FILE).length();

            IgniteBiTuple<Long, Integer> taskRes = ggfs.execute(new Task(),
                new GridGgfsStringDelimiterRecordResolver(" "), Collections.singleton(FILE), arg);

            assert F.eq(genLen, taskRes.getKey());
            assert F.eq(TOTAL_WORDS, taskRes.getValue());
        }
    }

    /**
     * Test task.
     *
     * @throws Exception If failed.
     */
    public void testTaskAsync() throws Exception {
        U.sleep(3000);

        assertFalse(ggfs.isAsync());

        IgniteFs ggfsAsync = ggfs.enableAsync();

        assertTrue(ggfsAsync.isAsync());

        for (int i = 0; i < REPEAT_CNT; i++) {
            String arg = DICTIONARY[new Random(System.currentTimeMillis()).nextInt(DICTIONARY.length)];

            generateFile(TOTAL_WORDS);
            Long genLen = ggfs.info(FILE).length();

            assertNull(ggfsAsync.execute(
                new Task(), new GridGgfsStringDelimiterRecordResolver(" "), Collections.singleton(FILE), arg));

            IgniteFuture<IgniteBiTuple<Long, Integer>> fut = ggfsAsync.future();

            assertNotNull(fut);

            IgniteBiTuple<Long, Integer> taskRes = fut.get();

            assert F.eq(genLen, taskRes.getKey());
            assert F.eq(TOTAL_WORDS, taskRes.getValue());
        }

        ggfsAsync.format();

        IgniteFuture<?> fut = ggfsAsync.future();

        assertNotNull(fut);

        fut.get();
    }

    // TODO: Remove.
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * Generate file with random data and provided argument.
     *
     * @param wordCnt Word count.
     * @throws Exception If failed.
     */
    private void generateFile(int wordCnt)
        throws Exception {
        Random rnd = new Random(System.currentTimeMillis());

        try (OutputStreamWriter writer = new OutputStreamWriter(ggfs.create(FILE, true))) {
            int cnt = 0;

            while (cnt < wordCnt) {
                String word = DICTIONARY[rnd.nextInt(DICTIONARY.length)];

                writer.write(word + " ");

                cnt++;
            }
        }
    }

    /**
     * Task.
     */
    private static class Task extends GridGgfsTask<String, IgniteBiTuple<Long, Integer>> {
        /** {@inheritDoc} */
        @Override public GridGgfsJob createJob(GridGgfsPath path, GridGgfsFileRange range,
            GridGgfsTaskArgs<String> args) throws GridException {
            return new Job();
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<Long, Integer> reduce(List<ComputeJobResult> ress) throws GridException {
            long totalLen = 0;
            int argCnt = 0;

            for (ComputeJobResult res : ress) {
                IgniteBiTuple<Long, Integer> res0 = (IgniteBiTuple<Long, Integer>)res.getData();

                if (res0 != null) {
                    totalLen += res0.getKey();
                    argCnt += res0.getValue();
                }
            }

            return F.t(totalLen, argCnt);
        }
    }

    /**
     * Job.
     */
    private static class Job implements GridGgfsJob, Serializable {
        @GridInstanceResource
        private Ignite ignite;

        @GridTaskSessionResource
        private ComputeTaskSession ses;

        @GridJobContextResource
        private ComputeJobContext ctx;

        /** {@inheritDoc} */
        @Override public Object execute(IgniteFs ggfs, GridGgfsFileRange range, GridGgfsInputStream in)
            throws GridException, IOException {
            assert ignite != null;
            assert ses != null;
            assert ctx != null;

            in.seek(range.start());

            byte[] buf = new byte[(int)range.length()];

            int totalRead = 0;

            while (totalRead < buf.length) {
                int b = in.read();

                assert b != -1;

                buf[totalRead++] = (byte)b;
            }

            String str = new String(buf);

            String[] chunks = str.split(" ");

            int ctr = 0;

            for (String chunk : chunks) {
                if (!chunk.isEmpty())
                    ctr++;
            }

            return F.t(range.length(), ctr);
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }
    }
}
