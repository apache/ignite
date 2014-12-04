/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Utility methods for dht preloader testing.
 */
public class GridCacheDhtTestUtils {
    /**
     * Ensure singleton.
     */
    private GridCacheDhtTestUtils() {
        // No-op.
    }

    /**
     * @param cache Cache.
     * @return Dht cache.
     */
    static <K, V> GridDhtCacheAdapter<K, V> dht(GridCacheProjection<K, V> cache) {
        return ((GridNearCacheAdapter<K, V>)cache.<K, V>cache()).dht();
    }

    /**
     * @param dht Cache.
     * @param keyCnt Number of test keys to put into cache.
     * @throws GridException If failed to prepare.
     */
    @SuppressWarnings({"UnusedAssignment", "unchecked"})
    static void prepareKeys(GridDhtCache<Integer, String> dht, int keyCnt) throws GridException {
        GridCacheAffinityFunction aff = dht.context().config().getAffinity();

        GridCacheConcurrentMap<Integer, String> cacheMap;

        try {
            Field field = GridCacheAdapter.class.getDeclaredField("map");

            field.setAccessible(true);

            cacheMap = (GridCacheConcurrentMap<Integer, String>)field.get(dht);
        }
        catch (Exception e) {
            throw new GridException("Failed to get cache map.", e);
        }

        GridDhtPartitionTopology<Integer,String> top = dht.topology();

        for (int i = 0; i < keyCnt; i++) {
            cacheMap.putEntry(-1, i, "value" + i, 0);

            dht.preloader().request(Collections.singleton(i), -1);

            GridDhtLocalPartition part = top.localPartition(aff.partition(i), false);

            assert part != null;

            part.own();
        }
    }

    /**
     * @param cache Dht cache.
     */
    static void printAffinityInfo(GridCache<?, ?> cache) {
        GridCacheConsistentHashAffinityFunction aff =
            (GridCacheConsistentHashAffinityFunction)cache.configuration().getAffinity();

        System.out.println("Affinity info.");
        System.out.println("----------------------------------");
        System.out.println("Number of key backups: " + cache.configuration().getBackups());
        System.out.println("Number of cache partitions: " + aff.getPartitions());
    }

    /**
     * @param dht Dht cache.
     * @param idx Cache index
     */
    static void printDhtTopology(GridDhtCache<Integer, String> dht, int idx) {
        final GridCacheAffinity<Integer> aff = dht.affinity();

        Ignite ignite = dht.context().grid();
        GridNode locNode = ignite.cluster().localNode();

        GridDhtPartitionTopology<Integer, String> top = dht.topology();

        System.out.println("\nTopology of cache #" + idx + " (" + locNode.id() + ")" + ":");
        System.out.println("----------------------------------");

        List<Integer> affParts = new LinkedList<>();

        GridDhtPartitionMap map = dht.topology().partitions(locNode.id());

        if (map != null)
            for (int p : map.keySet())
                affParts.add(p);

        Collections.sort(affParts);

        System.out.println("Affinity partitions: " + affParts + "\n");

        List<GridDhtLocalPartition> locals = new ArrayList<GridDhtLocalPartition>(top.localPartitions());

        Collections.sort(locals);

        for (final GridDhtLocalPartition part : locals) {
            Collection<GridNode> partNodes = aff.mapKeyToPrimaryAndBackups(part.id());

            String ownStr = !partNodes.contains(dht.context().localNode()) ? "NOT AN OWNER" :
                F.eqNodes(CU.primary(partNodes), locNode) ? "PRIMARY" : "BACKUP";

            Collection<Integer> keys = F.viewReadOnly(dht.keySet(), F.<Integer>identity(), new P1<Integer>() {
                @Override public boolean apply(Integer k) {
                    return aff.partition(k) == part.id();
                }
            });

            System.out.println("Local partition: [" + part + "], [owning=" + ownStr + ", keyCnt=" + keys.size() +
                ", keys=" + keys + "]");
        }

        System.out.println("\nNode map:");

        for (Map.Entry<UUID, GridDhtPartitionMap> e : top.partitionMap(false).entrySet()) {
            List<Integer> list = new ArrayList<>(e.getValue().keySet());

            Collections.sort(list);

            System.out.println("[node=" + e.getKey() + ", parts=" + list + "]");
        }

        System.out.println("");
    }

    /**
     * Checks consistency of partitioned cache.
     * Any preload processes must be finished before this method call().
     *
     * @param dht Dht cache.
     * @param idx Cache index.
     * @param log Logger.
     */
    static void checkDhtTopology(GridDhtCache<Integer, String> dht, int idx, GridLogger log) {
        assert dht != null;
        assert idx >= 0;
        assert log != null;

        log.info("Checking balanced state of cache #" + idx);

        GridCacheAffinity<Integer> aff = dht.affinity();

        Ignite ignite = dht.context().grid();
        GridNode locNode = ignite.cluster().localNode();

        GridDhtPartitionTopology<Integer,String> top = dht.topology();

        // Expected partitions calculated with affinity function.
        // They should be in topology in OWNING state.
        Collection<Integer> affParts = new HashSet<>();

        GridDhtPartitionMap map = dht.topology().partitions(locNode.id());

        if (map != null)
            for (int p : map.keySet())
                affParts.add(p);

        if (F.isEmpty(affParts))
            return;

        for (int p : affParts)
            assert top.localPartition(p, false) != null :
                "Partition does not exist in topology: [cache=" + idx + ", part=" + p + "]";

        for (GridDhtLocalPartition p : top.localPartitions()) {
            assert affParts.contains(p.id()) :
                "Invalid local partition: [cache=" + idx + ", part=" + p + ", node partitions=" + affParts + "]";

            assert p.state() == OWNING : "Invalid partition state [cache=" + idx + ", part=" + p + "]";

            Collection<GridNode> partNodes = aff.mapPartitionToPrimaryAndBackups(p.id());

            assert partNodes.contains(locNode) :
                "Partition affinity nodes does not contain local node: [cache=" + idx + "]";
        }

        // Check keys.
        for (GridCacheEntryEx<Integer, String> e : dht.entries()) {
            GridDhtCacheEntry<Integer, String> entry = (GridDhtCacheEntry<Integer, String>)e;

            if (!affParts.contains(entry.partition()))
                log.warning("Partition of stored entry is obsolete for node: [cache=" + idx + ", entry=" + entry +
                    ", node partitions=" + affParts + "]");

            int p = aff.partition(entry.key());

            if (!affParts.contains(p))
                log.warning("Calculated entry partition is not in node partitions: [cache=" + idx + ", part=" + p +
                    ", entry=" + entry + ", node partitions=" + affParts + "]");
        }
    }
}
