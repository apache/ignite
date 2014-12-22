/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;

import java.util.*;

/**
 * Tests transaction consistency when originating node fails.
 */
public class GridCachePartitionedTxOriginatingNodeFailureSelfTest extends
    IgniteTxOriginatingNodeFailureAbstractSelfTest {
    /** */
    private static final int BACKUP_CNT = 2;

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.NEAR_PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(BACKUP_CNT);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?> ignoreMessageClass() {
        return GridNearTxPrepareRequest.class;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFromPrimary() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((GridKernal)grid(originatingNode())).internalCache();

        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (cache.affinity().isPrimary(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFromBackup() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((GridKernal)grid(originatingNode())).internalCache();

        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (cache.affinity().isBackup(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFromNotColocated() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((GridKernal)grid(originatingNode())).internalCache();

        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (!cache.affinity().isPrimary(txNode, i) && !cache.affinity().isBackup(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxAllNodes() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((GridKernal)grid(originatingNode())).internalCache();

        List<ClusterNode> allNodes = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            allNodes.add(grid(i).localNode());

        Collection<Integer> keys = new ArrayList<>();

        for (int i = 0; i < Integer.MAX_VALUE && !allNodes.isEmpty(); i++) {
            for (Iterator<ClusterNode> iter = allNodes.iterator(); iter.hasNext();) {
                ClusterNode node = iter.next();

                if (cache.affinity().isPrimary(node, i)) {
                    keys.add(i);

                    iter.remove();

                    break;
                }
            }
        }

        assertEquals(GRID_CNT, keys.size());

        testTxOriginatingNodeFails(keys, false);
    }
}
