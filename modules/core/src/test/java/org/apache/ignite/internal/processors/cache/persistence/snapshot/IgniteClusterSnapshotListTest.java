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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommandArg;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_THREAD_POOL_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/** Cluster-wide snapshot list procedure tests.*/
public class IgniteClusterSnapshotListTest extends AbstractSnapshotSelfTest {
    /** Number of cache keys to pre-create at node start. */
    private static final int CACHE_KEYS_RANGE = 10;

    /** Number of partitions within a snapshot cache group. */
    private static final int CACHE_PARTITIONS_COUNT = 4;

    /** */
    @Parameterized.Parameter(2)
    public int snpThrdPoolSz;

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimary={1}, snpThrdPoolSz={2}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            encryptionParameters(),
            F.asList(false, true),
            F.asList(DFLT_SNAPSHOT_THREAD_POOL_SIZE, 1)
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        return super.txCacheConfig(ccfg).setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTITIONS_COUNT));
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        // Also cleans separated snapshot working directories and custom snapshot paths.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** */
    @Test
    public void testParallelDeletion() throws Exception {
        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, true, false);

        SnapshotFileTree sftNode1 = new SnapshotFileTree(grid(1).context(), SNAPSHOT_NAME, null);


    }

    /** */
    @Test
    public void testMissingMeta() throws Exception {
        doTestWithWrongMeta(false);
    }

    /** */
    @Test
    public void testCorruptedMeta() throws Exception {
        doTestWithWrongMeta(true);
    }

    /**
     * Tests snapshot lists when one snapshot metadata cannot be read.
     *
     * @param corruptFile If {@code true}, corrupts metadata. Otherwise, deletes metadata.
     */
    private void doTestWithWrongMeta(boolean corruptFile) throws Exception {
        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, true, false);

        SnapshotFileTree sftNode1 = new SnapshotFileTree(grid(1).context(), SNAPSHOT_NAME, null);

        assertTrue(sftNode1.meta().exists());

        if (corruptFile) {
            try (RandomAccessFile raf = new RandomAccessFile(sftNode1.meta(), "rw")) {
                raf.write(UUID.randomUUID().toString().getBytes());
            }
        }
        else
            assertTrue(sftNode1.meta().delete() && !sftNode1.meta().exists());

        SnapshotListTaskResult res = listSnapshots(grid(2));

        int foundSnpsCnt = 0;
        boolean victimNodeFound = false;

        for (int i = 0; i < res.nodesIds().length; i++) {
            UUID nid = res.nodesIds()[i];

            foundSnpsCnt += res.snapshots()[i].snapshots().size();

            if (nid.equals(grid(1).localNode().id())) {
                victimNodeFound = true;

                assertTrue(res.snapshots()[i].snapshots().isEmpty());
            }
        }

        assertTrue(victimNodeFound);
        assertEquals(2, foundSnpsCnt);
    }

    /** */
    private static SnapshotListTaskResult listSnapshots(IgniteEx grid) throws Exception {
        SnapshotListCommandArg arg = new SnapshotListCommandArg();

        Collection<UUID> nodes = grid.cluster().forServers().nodes().stream().map(ClusterNode::id).toList();

        return grid.compute().execute(SnapshotListTask.class, new VisorTaskArgument<>(nodes, arg, false)).result();
    }
}
