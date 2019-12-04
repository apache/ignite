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

package org.apache.ignite.internal.processors.query.h2;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CP_FILE_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;

/**
 *
 */
public class GridIndexFullRebuildTest extends GridCommonAbstractTest {
    public static final String FIRST_CACHE = "cache1";

    public static final String SECOND_CACHE = "cache2";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(256 * 1024 * 1024)
            .setPersistenceEnabled(true)
        );

        dsCfg.setCheckpointFrequency(3_000);

        configuration.setDataStorageConfiguration(dsCfg);

        CacheConfiguration ccfgFirst = new CacheConfiguration();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("updateDate", "java.lang.Date");
        fields.put("amount", "java.lang.Long");
        fields.put("name", "java.lang.String");

        Set<QueryIndex> indices = Collections.singleton(new QueryIndex("name", QueryIndexType.SORTED));

        ccfgFirst.setName(FIRST_CACHE)
            .setBackups(2)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setGroupName("group")
            .setCacheMode(CacheMode.PARTITIONED)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Long.class, Account.class)
                    .setFields(fields)
                    .setIndexes(indices)
            ));

        CacheConfiguration ccfgSecond = new CacheConfiguration(ccfgFirst).setName(SECOND_CACHE);

        configuration.setCacheConfiguration(ccfgFirst, ccfgSecond);

        return configuration;
    }

    /**
     * We start several nodes, populate caches, then start replacing values.
     * After that one node is killed, their index.bin files would be removed.
     * Finally, we restart the node, index rebuild starting after recovery.
     * And we checke indexes by "validate indexes" task.
     */
    @Test
    public void test() throws Exception {
        long start = System.currentTimeMillis();

        IgniteEx grid1 = startGrids(4);

        grid1.cluster().active(true);

        final int accountCnt = 2048;

        fillData(grid1, accountCnt);

        AtomicBoolean stop = startFillData(grid1, 0);

        long diff = System.currentTimeMillis() - start;

        U.sleep(7500 - (diff % 5000));

        IgniteProcessProxy.kill(getTestIgniteInstanceName(3));

        stop.set(true);

        removeIndexBin(3);

        startGrid(3);

        awaitPartitionMapExchange();

        U.sleep(3_000);

        validateIndexes(grid1, grid(2), grid(3));
    }

    /**
     * Test index recovery after killed rebuild.
     *
     * <ol>
     * <li>Start cluster (4x).</li>.
     * <li>Fill enough data as we need a lengthy index rebuild process.</li>
     * <li>Stop node #3.</li>
     * <li>Remove index.bin on node #3 to trigger indexes rebuild on start-up.</li>
     * <li>Re-start node #3 (must start index rebuild).</li>
     * <li>Start background puts.</li>
     * <li>Kill node #3.</li>
     * <li>Re-start node #3 (must start index rebuild).</li>
     * <li>Continue background puts.</li>
     * <li>Wait index rebuild completion.</li>
     * <li>Validate indexes on node #3.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKillRebuild() throws Exception {
        IgniteEx grid1 = startGrids(4);

        grid1.cluster().active(true);

        int accountCnt = 50_000; // Need lengthy rebuild.

        fillData(grid1, accountCnt);

        stopGrid(3);

        removeIndexBin(3);

        startGrid(3);

        awaitPartitionMapExchange();

        AtomicBoolean stop = startFillData(grid1, 1024);

        U.sleep(2000);

        IgniteProcessProxy.kill(getTestIgniteInstanceName(3));

        stop.set(true);

        startGrid(3);

        awaitPartitionMapExchange();

        stop = startFillData(grid1, 1024);

        try {
            assertTrue(waitIndexesAreReady(grid1, 3, 60_000));
        }
        finally {
            stop.set(true);
        }

        assertTrue(watchCheckpointStatus(3, CheckpointEntryType.END, 60_000));

        validateIndexes(grid1, grid(3));
    }

    /**
     *
     * @param ignite Ignite.
     * @param accountCnt Number of accounts to create.
     */
    private void fillData(IgniteEx ignite, int accountCnt) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(FIRST_CACHE)) {
            for (long i = 0; i < accountCnt; i++)
                streamer.addData(i, new Account(i));

            streamer.flush();
        }

        try (IgniteDataStreamer streamer = ignite.dataStreamer(SECOND_CACHE)) {
            for (long i = 0; i < accountCnt; i++)
                streamer.addData(i, new Account(i));

            streamer.flush();
        }
    }

    /**
     *
     * @param ignite Ignite.
     * @param startAccount First account id to create.
     * @return Cancellation flag.
     */
    private AtomicBoolean startFillData(IgniteEx ignite, int startAccount) {
        AtomicBoolean stop = new AtomicBoolean();

        IgniteCache<Object, Object> cache1 = ignite.cache(FIRST_CACHE);
        IgniteCache<Object, Object> cache2 = ignite.cache(SECOND_CACHE);

        new Thread(() -> {
            long i = startAccount;

            while (!stop.get()) {
                try {
                    cache1.put(i, new Account(i));

                    cache2.put(i, new Account(i));

                    i++;
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }).start();

        return stop;
    }

    /**
     * @param idx Node index.
     * @throws IgniteCheckedException If failed.
     */
    private void removeIndexBin(int idx) throws IgniteCheckedException {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);
        String instanceDir = U.maskForFileName(getTestIgniteInstanceName(idx));

        for (File grp : Objects.requireNonNull(new File(workDir, instanceDir).listFiles()))
            new File(grp, INDEX_FILE_NAME).delete();
    }

    /**
     * @param ignite Ignite.
     * @param idx Node index.
     * @param timeout Timeout.
     * @return {@code true} When index ready condition is observed within given time.
     * @throws IgniteCheckedException If failed.
     */
    private boolean waitIndexesAreReady(IgniteEx ignite, int idx, long timeout) throws IgniteCheckedException {
        IgniteCompute compute = ignite.compute(ignite.cluster().forNodeId(grid(idx).localNode().id()));

        return GridTestUtils.waitForCondition(()-> compute.callAsync(new IgniteCallable<Boolean>() {
            /** */
            @IgniteInstanceResource
            Ignite ignite;

            /** {@inheritDoc} */
            @Override public Boolean call() {
                return ignite.cache(FIRST_CACHE).indexReadyFuture().isDone() &&
                    ignite.cache(SECOND_CACHE).indexReadyFuture().isDone();
            }
        }).get(timeout), timeout);
    }

    /**
     *
     * @param ignite Ignite.
     * @param targets Target ignite instance.
     * @throws IgniteCheckedException If failed.
     */
    private void validateIndexes(IgniteEx ignite, IgniteEx... targets) throws IgniteCheckedException {
        Set<UUID> nodes = Arrays.stream(targets).map(g -> ((IgniteProcessProxy)g).getId()).collect(Collectors.toSet());

        VisorTaskArgument<VisorValidateIndexesTaskArg> arg = new VisorTaskArgument<>(nodes,
            new VisorValidateIndexesTaskArg(null, nodes, 0, 1),
            true);

        ComputeTaskInternalFuture<VisorValidateIndexesTaskResult> exec =
            ignite.context().task().execute(new VisorValidateIndexesTask(), arg);

        VisorValidateIndexesTaskResult result = exec.get();

        assertTrue(F.isEmpty(result.exceptions()));

        Map<UUID, VisorValidateIndexesJobResult> results = result.results();

        boolean hasIssue = false;

        for (VisorValidateIndexesJobResult jobResult : results.values()) {
            System.err.println(jobResult);

            hasIssue |= jobResult.hasIssues();
        }

        assertFalse(hasIssue);
    }

    /**
     * Watches filesystem directory for new checkpoint status files.
     *
     * @param idx Node index.
     * @param type Checkpoint entry type.
     * @param timeout Timeout.
     * @return {@code true} When specified checkpoint entry file was created within given time.
     * @throws IgniteCheckedException If failed.
     */
    private boolean watchCheckpointStatus(int idx, CheckpointEntryType type, long timeout)
        throws IgniteCheckedException {
        long since = System.currentTimeMillis();
        long till = since + timeout;

        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        Path cpPath = Paths.get(workDir.toPath().toString(),
            U.maskForFileName(getTestIgniteInstanceName(idx)),
            "cp");

        try (WatchService svc = FileSystems.getDefault().newWatchService()) {
            WatchKey key = cpPath.register(svc, ENTRY_CREATE);

            while (System.currentTimeMillis() < till) {
                WatchKey key0;
                try {
                    key0 = svc.take();
                }
                catch (InterruptedException e) {
                    return false;
                }

                for (WatchEvent<?> event: key0.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == OVERFLOW)
                        continue;

                    WatchEvent<Path> ev = (WatchEvent<Path>)event;
                    Path filename = ev.context();

                    Matcher match = CP_FILE_NAME_PATTERN.matcher(filename.toString());

                    if (match.matches()) {
                        long ts = Long.parseLong(match.group(1));

                        if (ts > since) {
                            CheckpointEntryType entryType = CheckpointEntryType.valueOf(match.group(3));

                            if (entryType == type) {
                                log.info("waitForRemoteCheckpoint since=" + since +
                                    ", ts=" + ts + ", entryType=" + entryType + ", file=" + filename);

                                return true;
                            }
                        }
                    }

                    if (!key.reset())
                        break;
                }
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** */
    public class Account {
        /** */
        private Long id;

        /** */
        private String name;

        /** */
        private Long amount;

        /** */
        private Date updateDate;

        /** */
        public Account(Long id) {
            this.id = id;

            name = "Account" + id;
            amount = id * 1000;
            updateDate = new Date();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Account account = (Account)o;
            return Objects.equals(id, account.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }
}
