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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.RECONCILIATION_DIR;

/**
 * Tests per cache output of partition reconciliation utility (aka compact mode).
 */
public class PartitionReconciliationCompactCollectorTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    private static final int NODES_CNT = 3;

    /** Number of partitions. */
    private static final int PARTS = 12;

    /** Keys count. */
    private static final int KEYS_CNT = PARTS * 10;

    /** Caches count. */
    private static final int CACHES_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration<Integer, Integer>[] ccfgs = new CacheConfiguration[CACHES_CNT];
        for (int i = 0; i < ccfgs.length; ++i) {
            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();
            ccfg.setName(DEFAULT_CACHE_NAME + '-' + i);
            ccfg.setAtomicityMode(ATOMIC);
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
            ccfg.setBackups(NODES_CNT - 1);

            ccfgs[i] = ccfg;
        }
        cfg.setCacheConfiguration(ccfgs);

        cfg.setConsistentId(name);

        cfg.setAutoActivationEnabled(false);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();
        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));

        cleanPersistenceDir();
    }

    /**
     * Tests that both modes (locOutput and compact) of execution of partition reconciliation utility provide the same result.
     *
     * @throws Exception If failed.
     */
    public void testCompactMode() throws Exception {
        startGrids(NODES_CNT);

        grid(0).cluster().active(true);

        Set<String> cacheNames = IntStream.range(0, CACHES_CNT)
            .mapToObj(i -> DEFAULT_CACHE_NAME + '-' + i).collect(Collectors.toSet());

        cacheNames.forEach(cacheName -> {
            IgniteCache<Object, Object> cache = grid(0).cache(cacheName);

            for (int i = 0; i < KEYS_CNT; i++)
                cache.put(i, String.valueOf(i));

            GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

            for (int i = 0; i < NODES_CNT; i++)
                nodeCacheCtxs[i] = grid(i).cachex(cacheName).context();

            for (int i = 0; i < KEYS_CNT; i++) {
                if (i % 3 == 0)
                    simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], i);
                else
                    simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], i);
            }
        });

        ReconciliationResult locOutputRes = partitionReconciliation(
            grid(0),
            new VisorPartitionReconciliationTaskArg.Builder()
                .locOutput(true)
                .repair(false)
                .repairAlg(RepairAlgorithm.PRINT_ONLY));

        Map<String, Map<Integer, String>> parsedLocOutput = parseResult(locOutputRes, cacheNames);

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));

        ReconciliationResult compactRes = partitionReconciliation(
            grid(0),
            new VisorPartitionReconciliationTaskArg.Builder()
                .locOutput(false)
                .repair(false)
                .repairAlg(RepairAlgorithm.PRINT_ONLY));

        Map<String, Map<Integer, String>> parsedCompactOutput = parseResult(compactRes, cacheNames);

        assertEquals(parsedLocOutput, parsedCompactOutput);
    }

    /**
     * Parses partition reconciliation file a nd returns mapping
     * cacheName -> Map partId -> string representation of inconsistent keys.
     *
     * @param res Partition reconciliation result.
     * @param cacheNames Set of cache names.
     * @return Parsed result based on partition reconciliation file(s).
     */
    private Map<String, Map<Integer, String>> parseResult(ReconciliationResult res, Set<String> cacheNames) {
        Map<String, Map<Integer, String>> ret = new HashMap<>();

        res.nodeIdToFolder().forEach((k, v) -> {
            try {
                Map<String, Map<Integer, String>> r = parseResult(v, cacheNames);

                r.forEach((c, p) -> {
                    if (ret.containsKey(c))
                        ret.get(c).putAll(p);
                    else
                        ret.put(c, p);
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return ret;
    }

    /**
     * Parses partition reconciliation file a nd returns mapping
     * cacheName -> Map partId -> string representation of inconsistent keys.
     *
     * @param pathToReport Path to report.
     * @param cacheNames Set of cache names.
     * @return Parsed result based on partition reconciliation file(s).
     * @throws IOException If failed.
     */
    private Map<String, Map<Integer, String>> parseResult(String pathToReport, Set<String> cacheNames) throws IOException {
        assertNotNull("Partition reconciliation file is not created.", pathToReport);
        assertTrue("Cannot find partition reconciliation file.", new File(pathToReport).exists());

        Map<String, Map<Integer, String>> ret = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(pathToReport))) {
            String line;

            while ((line = reader.readLine()) != null) {
                // skip header
                if (cacheNames.contains(line))
                    break;
            }

            if (line == null)
                return ret;

            String cacheName = line;

            line = reader.readLine();
            if (line == null)
                throw new IllegalStateException("emprty cache should not be logged.");

            Integer partId = Integer.valueOf(line.substring(1));
            ret.put(cacheName, new HashMap<>());
            Set<String> partKeys = new HashSet<>();

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("\t\t\t")) {
                }
                else if (line.startsWith("\t\t"))
                    partKeys.add(line);
                else {
                    if (line.startsWith("\t")) {
                        // new partition
                        ret.get(cacheName).put(partId, String.join("\n", partKeys));
                        partKeys.clear();
                        partId = Integer.valueOf(line.substring(1));
                    }
                    else {
                        // new cache
                        ret.get(cacheName).put(partId, String.join("\n", partKeys));
                        partKeys.clear();
                        ret.put((cacheName = line), new HashMap<>());
                    }
                }
            }

            ret.get(cacheName).put(partId, String.join("\n", partKeys));
        }

        return ret;
    }
}
