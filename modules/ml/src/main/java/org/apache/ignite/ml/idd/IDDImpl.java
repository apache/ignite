package org.apache.ignite.ml.idd;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteConsumer;

public class IDDImpl<K, V, D, L> implements IDD<K, V, D, L> {

    private final Ignite ignite;

    private final IgniteCache<K, V> upstreamCache;

    private final IgniteCache<Integer, IDDPartitionDistributedSegment<K, V, D, L>> iddCache;

    private final UUID iddId;

    public IDDImpl(Ignite ignite, IgniteCache<K, V> upstreamCache,
        IgniteCache<Integer, IDDPartitionDistributedSegment<K, V, D, L>> iddCache, UUID iddId) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
        this.iddCache = iddCache;
        this.iddId = iddId;
    }

    @SuppressWarnings("unchecked")
    @Override public void compute(IgniteConsumer<IDDPartition<K, V, D, L>> consumer) {
        Affinity<K> affinity = ignite.affinity(iddCache.getName());
        int partitions = affinity.partitions();

        String upstreamCacheName = upstreamCache.getName();
        String iddCacheName = iddCache.getName();

        for (int part = 0; part < partitions; part++) {
            int currPart = part;
            ignite.compute().affinityRun(
                Arrays.asList(iddCacheName, upstreamCacheName),
                currPart,
                () -> {
                    Ignite localIgnite = Ignition.localIgnite();

                    IgniteCache<Integer, IDDPartitionDistributedSegment<K, V, D, L>> localIddCache = localIgnite.cache(iddCacheName);
                    IDDPartitionDistributedSegment<K, V, D, L> distributedSegment = localIddCache.get(currPart);

                    IDDInstanceLocalStorage localStorage = IDDInstanceLocalStorage.getInstance();
                    ConcurrentMap<Integer, Object> localPartStorage = localStorage.getOrCreateIDDLocalStorage(iddId);

                    L localSegment = (L) localPartStorage.computeIfAbsent(currPart, p -> {
                        IgniteCache<K, V> localUpstreamCache = ignite.cache(upstreamCacheName);
                        IgniteBiFunction<IgniteCache<K, V>, Integer, L> loader = distributedSegment.getLoader();
                        return loader.apply(localUpstreamCache, currPart);
                    });

                    if (localSegment == null)
                        throw new IllegalStateException();

                    IDDPartition<K, V, D, L> partition = new IDDPartition<>(distributedSegment, localSegment);

                    consumer.accept(partition);
                }
            );
        }
    }
}
