/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * DHT transaction mapping.
 */
public class GridDhtTxMapping<K, V> {
    /** Transaction nodes mapping (primary node -> related backup nodes). */
    private final Map<UUID, Collection<UUID>> txNodes = new GridLeanMap<>();

    /** */
    private final List<TxMapping> mappings = new ArrayList<>();

    /** */
    private TxMapping last;

    /**
     * Adds information about next mapping.
     *
     * @param nodes Nodes.
     */
    @SuppressWarnings("ConstantConditions")
    public void addMapping(List<ClusterNode> nodes) {
        ClusterNode primary = F.first(nodes);

        Collection<ClusterNode> backups = F.view(nodes, F.notEqualTo(primary));

        if (last == null || !last.primary.equals(primary.id())) {
            last = new TxMapping(primary, backups);

            mappings.add(last);
        }
        else
            last.add(backups);

        Collection<UUID> storedBackups = txNodes.get(last.primary);

        if (storedBackups == null)
            txNodes.put(last.primary, storedBackups = new HashSet<>());

        storedBackups.addAll(last.backups);
    }

    /**
     * @return Primary to backup mapping.
     */
    public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * For each mapping sets flags indicating if mapping is last for node.
     *
     * @param mappings Mappings.
     */
    public void initLast(Collection<GridDistributedTxMapping<K, V>> mappings) {
        assert this.mappings.size() == mappings.size();

        int idx = 0;

        for (GridDistributedTxMapping<?, ?> map : mappings) {
            TxMapping mapping = this.mappings.get(idx);

            map.lastBackups(lastBackups(mapping, idx));

            boolean last = true;

            for (int i = idx + 1; i < this.mappings.size(); i++) {
                TxMapping nextMap = this.mappings.get(i);

                if (nextMap.primary.equals(mapping.primary)) {
                    last = false;

                    break;
                }
            }

            map.last(last);

            idx++;
        }
    }

    /**
     * @param mapping Mapping.
     * @param idx Mapping index.
     * @return IDs of backup nodes receiving last prepare request during this mapping.
     */
    @Nullable private Collection<UUID> lastBackups(TxMapping mapping, int idx) {
        Collection<UUID> res = null;

        for (UUID backup : mapping.backups) {
            boolean foundNext = false;

            for (int i = idx + 1; i < mappings.size(); i++) {
                TxMapping nextMap = mappings.get(i);

                if (nextMap.primary.equals(mapping.primary) && nextMap.backups.contains(backup)) {
                    foundNext = true;

                    break;
                }
            }

            if (!foundNext) {
                if (res == null)
                    res = new ArrayList<>(mapping.backups.size());

                res.add(backup);
            }
        }

        return res;
    }

    /**
     */
    private static class TxMapping {
        /** */
        private final UUID primary;

        /** */
        private final Set<UUID> backups;

        /**
         * @param primary Primary node.
         * @param backups Backup nodes.
         */
        private TxMapping(ClusterNode primary, Iterable<ClusterNode> backups) {
            this.primary = primary.id();

            this.backups = new HashSet<>();

            add(backups);
        }

        /**
         * @param backups Backup nodes.
         */
        private void add(Iterable<ClusterNode> backups) {
            for (ClusterNode n : backups)
                this.backups.add(n.id());
        }
    }
}
