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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.resolveSnapshotWorkDirectory;

/** */
public class SnapshotRestoreFromRemoteWithPairedConnectionsTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Snapshot parts on dedicated cluster. Each part has its own local directory. */
    private static final Set<Path> snpParts = new HashSet<>();

    /** */
    private static final Function<String, BiFunction<Integer, IgniteConfiguration, String>> CLUSTER_DIR =
        new Function<String, BiFunction<Integer, IgniteConfiguration, String>>() {
            @Override public BiFunction<Integer, IgniteConfiguration, String> apply(String prefix) {
                return (id, cfg) -> Paths.get(defaultWorkDirectory().toString(),
                    prefix + U.maskForFileName(cfg.getIgniteInstanceName())).toString();
            }
        };

    /** */
    private String changedConsistentId;

    /** */
    private boolean cmPairedConnections;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if(!F.isEmpty(changedConsistentId))
            cfg.setConsistentId(cfg.getConsistentId() + changedConsistentId);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(cmPairedConnections);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreWithPairedConnections() throws Exception {
        U.delete(defaultWorkDirectory());

        IgniteEx ignite =  startGridsWithCache(4, CACHE_KEYS_RANGE, String::valueOf, CLUSTER_DIR.apply("_one"),
            dfltCacheCfg.setBackups(0));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        stopAllGrids();

        snpParts.addAll(findSnapshotParts("_one", SNAPSHOT_NAME));

        changedConsistentId = "_new";

        // Set false of remove to pass the test.
        cmPairedConnections = true;

        IgniteEx scc = startDedicatedGrids("_two", 4);

        scc.cluster().state(ClusterState.ACTIVE);

        copyAndShuffle(snpParts, G.allGrids());

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();;

        // Restore all cache groups.
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(scc.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /**
     * @param snpParts Snapshot parts.
     * @param toNodes List of toNodes to copy parts to.
     */
    private static void copyAndShuffle(Set<Path> snpParts, List<Ignite> toNodes) {
        AtomicInteger cnt = new AtomicInteger();

        snpParts.forEach(p -> {
            try {
                IgniteEx loc = (IgniteEx)toNodes.get(cnt.getAndIncrement() % toNodes.size());
                String snpName = p.getFileName().toString();

                U.copy(p.toFile(),
                    Paths.get(resolveSnapshotWorkDirectory(loc.configuration()).getAbsolutePath(), snpName).toFile(),
                    false);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        });
    }

    /**
     * @return Collection of dedicated snapshot paths located in Ignite working directory.
     */
    private static Set<Path> findSnapshotParts(String prefix, String snpName) {
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

    /** */
    private IgniteEx startDedicatedGrids(String prefix, int grids) throws Exception {
        for (int g = 0; g < grids; g++) {
            IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(g)));

            cfg.setWorkDirectory(CLUSTER_DIR.apply(prefix).apply(g, cfg));

            startGrid(cfg);
        }

        return grid(0);
    }

    /** */
    private static Path defaultWorkDirectory() {
        try {
            return Paths.get(U.defaultWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
