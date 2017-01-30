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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsTask;
import org.apache.ignite.igfs.mapreduce.IgfsTaskArgs;
import org.apache.ignite.igfs.mapreduce.records.IgfsStringDelimiterRecordResolver;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Tests for {@link IgfsTask}.
 */
public class IgfsTaskSelfTest extends IgfsCommonAbstractTest {
    /** Predefined words dictionary. */
    private static final String[] DICTIONARY = new String[] {"word0", "word1", "word2", "word3", "word4", "word5",
        "word6", "word7"};

    /** File path. */
    private static final IgfsPath FILE = new IgfsPath("/file");

    /** Shared IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Block size: 64 Kb. */
    private static final int BLOCK_SIZE = 64 * 1024;

    /** Total words in file. */
    private static final int TOTAL_WORDS = 1024 * 1024;

    /** Node count */
    private static final int NODE_CNT = 3;

    /** IGFS. */
    private static IgniteFileSystem igfs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < NODE_CNT; i++) {
            Ignite g = G.start(config(i));

            if (i + 1 == NODE_CNT)
                igfs = g.fileSystem("igfs");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        igfs.format();
    }

    /**
     * Create grid configuration.
     *
     * @param idx Node index.
     * @return Grid configuration
     */
    private IgniteConfiguration config(int idx) {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(BLOCK_SIZE);
        igfsCfg.setDefaultMode(PRIMARY);
        igfsCfg.setFragmentizerEnabled(false);

        CacheConfiguration dataCacheCfg = new CacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(1));
        dataCacheCfg.setBackups(0);

        CacheConfiguration metaCacheCfg = new CacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setGridName("node-" + idx);

        return cfg;
    }

    /**
     * Test task.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testTask() throws Exception {
        String arg = DICTIONARY[new Random(System.currentTimeMillis()).nextInt(DICTIONARY.length)];

        generateFile(TOTAL_WORDS);
        Long genLen = igfs.info(FILE).length();

        IgniteBiTuple<Long, Integer> taskRes = igfs.execute(new Task(),
            new IgfsStringDelimiterRecordResolver(" "), Collections.singleton(FILE), arg);

        assert F.eq(genLen, taskRes.getKey());
        assert F.eq(TOTAL_WORDS, taskRes.getValue());
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

        try (OutputStreamWriter writer = new OutputStreamWriter(igfs.create(FILE, true))) {
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
    private static class Task extends IgfsTask<String, IgniteBiTuple<Long, Integer>> {
        /** {@inheritDoc} */
        @Override public IgfsJob createJob(IgfsPath path, IgfsFileRange range,
            IgfsTaskArgs<String> args) {
            return new Job();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ConstantConditions")
        @Override public IgniteBiTuple<Long, Integer> reduce(List<ComputeJobResult> ress) {
            long totalLen = 0;
            int argCnt = 0;

            for (ComputeJobResult res : ress) {
                IgniteBiTuple<Long, Integer> res0 = res.getData();

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
    @SuppressWarnings("unused")
    private static class Job implements IgfsJob, Serializable {
        @IgniteInstanceResource
        private Ignite ignite;

        @TaskSessionResource
        private ComputeTaskSession ses;

        @JobContextResource
        private ComputeJobContext ctx;

        /** {@inheritDoc} */
        @Override public Object execute(IgniteFileSystem igfs, IgfsFileRange range, IgfsInputStream in)
            throws IOException {
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