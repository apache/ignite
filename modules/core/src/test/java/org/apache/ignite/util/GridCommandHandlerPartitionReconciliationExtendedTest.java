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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.SESSION_CHANGE_MSG;

/**
 * Tests for checking partition reconciliation.
 */
public class GridCommandHandlerPartitionReconciliationExtendedTest extends
    GridCommandHandlerClusterPerMethodAbstractTest {
    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(2));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests for checking partition reconciliation cancel control.sh command.
     */
    @Test
    public void testPartitionReconciliationCancel() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.contains(SESSION_CHANGE_MSG)).times(3).build();
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);
        }

        IgniteEx grid = grid(1);

        for (int i = 0; i < 100; i++)
            corruptDataEntry(grid.cachex(DEFAULT_CACHE_NAME).context(), i);

        assertEquals(0, reconciliationSessionId());

        GridTestUtils.runAsync(() -> assertEquals(EXIT_CODE_OK, execute("--cache", "partition-reconciliation", "--repair",
            "MAJORITY", "--recheck-attempts", "5")));

        assertTrue(GridTestUtils.waitForCondition(() -> reconciliationSessionId() != 0, 10_000));

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition-reconciliation-cancel"));

        assertEquals(0, reconciliationSessionId());

        assertTrue(lsnr.check(10_000));
    }

    /**
     * Checks that the start message exist.
     */
    @Test
    public void testProgressLogPrinted() throws Exception {
        System.setProperty("RECONCILIATION_WORK_PROGRESS_PRINT_INTERVAL", "0");
        LogListener lsnr = LogListener.matches(s -> s.startsWith("Partition reconciliation task [sesId=")).atLeast(1).build();
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition-reconciliation", "--repair", "MAJORITY", "--recheck-attempts", "1"));

        assertTrue(lsnr.check(10_000));
        System.clearProperty("RECONCILIATION_WORK_PROGRESS_PRINT_INTERVAL");
    }

    /**
     * Check that utility works only with specified subset of caches in case parameter is set
     */
    @Test
    public void testWorkWithSubsetOfCaches() throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        for (int i = 1; i <= 3; i++)
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i);

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition-reconciliation", "default, default3"));

        assertTrue(lsnr.check(10_000));

        assertTrue(usedCaches.containsAll(Arrays.asList("default", "default3")));
        assertEquals(usedCaches.size(), 2);
    }

    /**
     * Check that utility works only with specified subset of caches in case parameter is set, using regexp.
     */
    @Test
    public void testWorkWithSubsetOfCachesByRegexp() throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        for (int i = 1; i <= 3; i++)
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i);

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition-reconciliation", "default.*"));

        assertTrue(lsnr.check(10_000));

        assertTrue(usedCaches.containsAll(Arrays.asList("default", "default1", "default2", "default3")));
        assertEquals(usedCaches.size(), 4);
    }

    /**
     * Tests that utility will started with all available user caches.
     */
    @Test
    public void testWorkWithAllSetOfCachesIfParameterAbsent() throws Exception {
        Set<String> usedCaches = new HashSet<>();
        LogListener lsnr = fillCacheNames(usedCaches);
        log.registerListener(lsnr);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        List<String> setOfCaches = new ArrayList<>();
        setOfCaches.add(DEFAULT_CACHE_NAME);

        for (int i = 1; i <= 3; i++)
            setOfCaches.add(ignite.getOrCreateCache(DEFAULT_CACHE_NAME + i).getName());

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition-reconciliation"));

        assertTrue(lsnr.check(10_000));

        assertTrue(usedCaches.containsAll(setOfCaches));
        assertEquals(usedCaches.size(), setOfCaches.size());
    }

    /**
     * Checks that a wrong cache name leads to interruption of utility.
     */
    @Test
    public void testWrongCacheNameTerminatesOperation() throws Exception {
        String wrongCacheName = "wrong_cache_name";
        LogListener errorMsg = LogListener.matches(s -> s.contains("The cache '" + wrongCacheName + "' doesn't exist.")).atLeast(1).build();
        log.registerListener(errorMsg);

        startGrids(3);

        IgniteEx ignite = grid(0);
        ignite.cluster().active(true);

        Logger logger = CommandHandler.initLogger(null);

        logger.addHandler(new StreamHandler(System.out, new Formatter() {
            /** {@inheritDoc} */
            @Override public String format(LogRecord record) {
                log.info(record.getMessage());

                return record.getMessage() + "\n";
            }
        }));

        assertEquals(EXIT_CODE_OK, execute(new CommandHandler(logger), "--cache", "partition-reconciliation", wrongCacheName));

        assertTrue(errorMsg.check(10_000));
    }

    /**
     * Extract cache names which used for a start.
     */
    private LogListener fillCacheNames(Set<String> usedCaches) {
        Pattern r = Pattern.compile("Partition reconciliation has started.*caches=\\[(.*)\\]\\].*");

        LogListener lsnr = LogListener.matches(s -> {
            Matcher m = r.matcher(s);

            boolean found = m.find();

            if (found && m.group(1) != null && !m.group(1).isEmpty())
                usedCaches.addAll(Arrays.asList(m.group(1).split(", ")));

            return found;
        }).atLeast(1).build();

        return lsnr;
    }

    /**
     * Extract reconciliation sessionId.
     */
    private long reconciliationSessionId() {
        List<Ignite> srvs = G.allGrids().stream().filter(g -> !g.configuration().getDiscoverySpi().isClientMode()).collect(toList());

        List<Long> collect;

        do {
            collect = srvs.stream()
                .map(g -> ((IgniteEx)g).context().diagnostic().reconciliationExecutionContext().sessionId())
                .distinct()
                .collect(toList());
        }
        while (collect.size() > 1);

        assert collect.size() == 1;

        return collect.get(0);
    }

    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     */
    protected void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        Object key
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                new GridCacheVersion(),
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }
}
