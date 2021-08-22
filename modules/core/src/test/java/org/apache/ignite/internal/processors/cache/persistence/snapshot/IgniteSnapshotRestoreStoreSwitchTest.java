/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.resolveSnapshotWorkDirectory;

/** */
public class IgniteSnapshotRestoreStoreSwitchTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** */
    private static final String FIRST_CLUSTER_PREFIX = "one_";

    /** */
    private static final String SECOND_CLUSTER_PREFIX = "two_";

    /** */
    private static final Function<String, BiFunction<Integer, IgniteConfiguration, String>> CLUSTER_DIR =
        new Function<String, BiFunction<Integer, IgniteConfiguration, String>>() {
            @Override public BiFunction<Integer, IgniteConfiguration, String> apply(String prefix) {
                return (id, cfg) -> Paths.get(defaultWorkDirectory().toString(),
                    prefix + U.maskForFileName(cfg.getIgniteInstanceName())).toString();
            }
        };

    /** Cache value builder. */
    private final Function<Integer, Object> valBuilder = String::valueOf;

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return valBuilder;
    }

    /** @throws Exception If fails. */
    @Before
    public void beforeSwitchSnapshot() throws Exception {
        beforeTestSnapshot();
        cleanupDedicatedPersistenceDirs(FIRST_CLUSTER_PREFIX, SECOND_CLUSTER_PREFIX);
    }

    /** @throws Exception If fails. */
    @After
    public void afterSwitchSnapshot() throws Exception {
        afterTestSnapshot();
        cleanupDedicatedPersistenceDirs(FIRST_CLUSTER_PREFIX, SECOND_CLUSTER_PREFIX);
    }

    // TODO add test when force affinity reassignment occurs during the restore procedure.
    /** @throws Exception If failed. */
    @Test
    public void testRestoreAllGroups() throws Exception {
        CacheConfiguration<Integer, Object> cacheCfg1 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

        CacheConfiguration<Integer, Object> cacheCfg2 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE2)).setGroupName(SHARED_GRP);

        IgniteEx ignite = startDedicatedGridsWithCache(FIRST_CLUSTER_PREFIX, 6, CACHE_KEYS_RANGE, valBuilder,
            dfltCacheCfg.setBackups(0), cacheCfg1, cacheCfg2);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        awaitPartitionMapExchange();
        stopAllGrids();

        Set<Path> snpParts = resolveSnapshotParts(FIRST_CLUSTER_PREFIX, SNAPSHOT_NAME);

        // TODO map resolved snapshot randomly to a newer cluster.

        IgniteEx scc = startDedicatedGrids(SECOND_CLUSTER_PREFIX, 2);
        scc.cluster().state(ClusterState.ACTIVE);

        snpParts.forEach(p -> {
            try {
                IgniteEx loc = grid(ThreadLocalRandom.current().nextInt(2));
                String snpName = p.getFileName().toString();

                U.copy(p.toFile(),
                    Paths.get(resolveSnapshotWorkDirectory(loc.configuration()).getAbsolutePath(), snpName).toFile(),
                    false);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        });

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        // Restore all cache groups.
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        awaitPartitionMapExchange(true, true, null, true);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE1), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE2), CACHE_KEYS_RANGE);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);
    }

    /**
     * @param clusterPrefix Array of prefixes to cleanup directories.
     */
    private void cleanupDedicatedPersistenceDirs(String... clusterPrefix) {
        for (String prefix : clusterPrefix) {
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(defaultWorkDirectory(),
                path -> Files.isDirectory(path) && path.getFileName().toString().toLowerCase().startsWith(prefix))
            ) {
                for (Path dir : ds)
                    U.delete(dir);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @return Collection of dedicated snapshot paths located in Ignite working directory.
     */
    private static Set<Path> resolveSnapshotParts(String prefix, String snpName) {
        Set<Path> snpPaths = new HashSet<>();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(defaultWorkDirectory(),
            path -> Files.isDirectory(path) && path.getFileName().toString().toLowerCase().startsWith(prefix))
        ) {
            for (Path dir : ds)
                snpPaths.add(searchDirectoryRecursively(dir, snpName)
                    .orElseThrow(() -> new IgniteException("Snapshot not found in the Ignite work directory " +
                        "[dir=" + dir.toString() + ", snpName=" + snpName + ']')));

            return snpPaths;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param grids Number of ignite instances to start.
     * @param keys Number of keys to create.
     * @param valMapper Factory which produces values.
     * @param <V> Cache value type.
     * @return Ignite coordinator instance.
     * @throws Exception If fails.
     */
    private <V> IgniteEx startDedicatedGridsWithCache(
        String prefix,
        int grids,
        int keys,
        Function<Integer, V> valMapper,
        CacheConfiguration<Integer, V>... ccfgs
    ) throws Exception {
        return startGridsWithCache(grids,
            keys,
            valMapper,
            CLUSTER_DIR.apply(prefix),
            ccfgs);
    }

    /**
     * @param grids Number of ignite instances to start.
     * @return Ignite coordinator instance.
     * @throws Exception If fails.
     */
    private IgniteEx startDedicatedGrids(String prefix, int grids) throws Exception {
        for (int g = 0; g < grids; g++) {
            IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(g)));
            cfg.setWorkDirectory(CLUSTER_DIR.apply(prefix).apply(g, cfg));

            startGrid(cfg);
        }

        return grid(0);
    }

    /**
     * @return Default work directory.
     */
    private static Path defaultWorkDirectory() {
        try {
            return Paths.get(U.defaultWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
