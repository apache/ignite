/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Affinity which controls where nodes end up using mod operation.
 */
public class GridCacheModuloAffinityFunction implements GridCacheAffinityFunction {
    /** Node attribute for index. */
    public static final String IDX_ATTR = "nodeIndex";

    /** Number of backups. */
    private int backups = -1;

    /** Number of partitions. */
    private int parts = -1;

    /**
     * Empty constructor.
     */
    public GridCacheModuloAffinityFunction() {
        // No-op.
    }

    /**
     * @param parts Number of partitions.
     * @param backups Number of backups.
     */
    public GridCacheModuloAffinityFunction(int parts, int backups) {
        assert parts > 0;
        assert backups >= 0;

        this.parts = parts;
        this.backups = backups;
    }

    /**
     * @param parts Number of partitions.
     */
    public void partitions(int parts) {
        assert parts > 0;

        this.parts = parts;
    }

    /**
     * @param backups Number of backups.
     */
    public void backups(int backups) {
        assert backups >= 0;

        this.backups = backups;
    }

    /**
     * @return Number of backups.
     */
    public int backups() {
        return backups;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<List<ClusterNode>> assignPartitions(GridCacheAffinityFunctionContext ctx) {
        List<List<ClusterNode>> res = new ArrayList<>(parts);

        Collection<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        for (int part = 0; part < parts; part++) {
            res.add(F.isEmpty(topSnapshot) ?
                Collections.<ClusterNode>emptyList() :
                // Wrap affinity nodes with unmodifiable list since unmodifiable generic collection
                // doesn't provide equals and hashCode implementations.
                U.sealList(nodes(part, topSnapshot)));
        }

        return Collections.unmodifiableList(res);
    }

    /** {@inheritDoc} */
    public Collection<ClusterNode> nodes(int part, Collection<ClusterNode> nodes) {
        List<ClusterNode> sorted = new ArrayList<>(nodes);

        Collections.sort(sorted, new Comparator<ClusterNode>() {
            @Override public int compare(ClusterNode n1, ClusterNode n2) {
                int idx1 = n1.<Integer>attribute(IDX_ATTR);
                int idx2 = n2.<Integer>attribute(IDX_ATTR);

                return idx1 < idx2 ? -1 : idx1 == idx2 ? 0 : 1;
            }
        });

        int max = 1 + backups;

        if (max > nodes.size())
            max = nodes.size();

        Collection<ClusterNode> ret = new ArrayList<>(max);

        Iterator<ClusterNode> it = sorted.iterator();

        for (int i = 0; i < max; i++) {
            ClusterNode n = null;

            if (i == 0) {
                while (it.hasNext()) {
                    n = it.next();

                    int nodeIdx = n.<Integer>attribute(IDX_ATTR);

                    if (part <= nodeIdx)
                        break;
                    else
                        n = null;
                }
            }
            else {
                if (it.hasNext())
                    n = it.next();
                else {
                    it = sorted.iterator();

                    assert it.hasNext();

                    n = it.next();
                }
            }

            assert n != null || nodes.size() < parts;

            if (n == null)
                n = (it = sorted.iterator()).next();


            ret.add(n);
        }

        return ret;
    }

    /**
     * @param parts Number of partitions.
     * @param backups Number of backups.
     */
    public void reset(int parts, int backups) {
        this.parts = parts;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        parts = -1;
        backups = -1;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        if (key instanceof Number)
            return ((Number)key).intValue() % parts;

        return key == null ? 0 : U.safeAbs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheModuloAffinityFunction.class, this);
    }
}
