package org.apache.ignite.ml.idd;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

public class IDDFactory {

    @SuppressWarnings("unchecked")
    public static <K, V, D, L> IDD<K, V, D, L> createIDD(Ignite ignite, IgniteCache<K, V> upstreamCache,
        IgniteBiFunction<IgniteCache<K, V>, Integer, L> loader) {

        UUID iddId = UUID.randomUUID();

        CacheConfiguration<Integer, IDDPartitionDistributedSegment<K, V, D, L>> cc = new CacheConfiguration<>();
        cc.setName(iddId.toString());
        AffinityFunction af = upstreamCache.getConfiguration(CacheConfiguration.class).getAffinity();
        cc.setAffinity(new AffinityFunctionWrapper(af));
        IgniteCache<Integer, IDDPartitionDistributedSegment<K, V, D, L>> iddCache = ignite.createCache(cc);

        Affinity<K> affinity = ignite.affinity(iddCache.getName());
        int partitions = affinity.partitions();

        for (int part = 0; part < partitions; part++) {
            IDDPartitionDistributedSegment<K, V, D, L> distributedSegment
                = new IDDPartitionDistributedSegment<>(loader, iddId, part, null);
            iddCache.put(part, distributedSegment);
        }

        return new IDDImpl<>(ignite, upstreamCache, iddCache, iddId);
    }

    private static class AffinityFunctionWrapper implements AffinityFunction {

        private final AffinityFunction delegate;

        public AffinityFunctionWrapper(AffinityFunction delegate) {
            this.delegate = delegate;
        }

        @Override public void reset() {
            delegate.reset();
        }

        @Override public int partitions() {
            return delegate.partitions();
        }

        @Override public int partition(Object key) {
            return (Integer) key;
        }

        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            return delegate.assignPartitions(affCtx);
        }

        @Override public void removeNode(UUID nodeId) {
            delegate.removeNode(nodeId);
        }
    }
}
