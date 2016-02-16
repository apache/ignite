package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Throws another exception
 */
public class IgniteInstanceDeserializationTest extends GridCacheAbstractSelfTest {

    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterGroupDeserializationByGateway() throws Exception {
        CacheConfiguration<Integer, ClusterGroupHolder> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setName("cacheCfg");

        IgniteCache<Integer, ClusterGroupHolder> cache = grid(0).getOrCreateCache(cacheCfg);

        final int key0 = primaryKey(cache);
        final int key1 = backupKey(cache);

        ClusterGroupHolder remotes = new ClusterGroupHolder(grid(0).cluster().forRemotes());
        ClusterGroupHolder servers = new ClusterGroupHolder(grid(0).cluster().forServers());

        cache.put(key1, remotes);
        cache.put(key0, servers);

        grid(0).getOrCreateCache("cacheCfg").get(key0);
        grid(0).getOrCreateCache("cacheCfg").get(key1);
        grid(1).getOrCreateCache("cacheCfg").get(key0);
        grid(1).getOrCreateCache("cacheCfg").get(key1);

    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteDeserializationByGateway() throws Exception {
        CacheConfiguration<Integer, IgniteHolder> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setName("cacheCfg");

        IgniteCache<Integer, IgniteHolder> cache = grid(0).getOrCreateCache(cacheCfg);

        final int key0 = primaryKey(cache);
        final int key1 = backupKey(cache);

        System.out.println(grid(0).name());
        System.out.println(grid(1).name());

        IgniteHolder ignite0 = new IgniteHolder(grid(0));
        IgniteHolder ignite1 = new IgniteHolder(grid(1));

        cache.put(key1, ignite1);
        cache.put(key0, ignite0);

        grid(0).getOrCreateCache("cacheCfg").get(key0);
        grid(0).getOrCreateCache("cacheCfg").get(key1);
        grid(1).getOrCreateCache("cacheCfg").get(key0);
        grid(1).getOrCreateCache("cacheCfg").get(key1);

    }


    public static class ClusterGroupHolder {

        private final ClusterGroup clusterGroup;

        public ClusterGroupHolder(ClusterGroup group) {
            clusterGroup = group;
        }

        public ClusterGroup getClusterGroup() {
            return clusterGroup;
        }
    }

    public static class IgniteHolder implements Externalizable {

       /* @IgniteInstanceResource
        private Ignite ignite;

        private String gridName;

        public IgniteHolder(Ignite ignite) {
            this.ignite = ignite;
            this.gridName = ignite.name();
        }

        public Ignite getIgnite() {
            return ignite;
        }*/

        private Ignite ignite;
        private String gridName;

        public IgniteHolder() {}

        public IgniteHolder(Ignite ignite) {
            this.ignite = ignite;
            this.gridName = ignite.name();
        }

        public Ignite getIgnite() {
            return ignite;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            gridName = U.readString(in);
            ignite = IgnitionEx.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, gridName);
        }

        /**
         * @return IgniteKernal instance.
         *
         * @throws ObjectStreamException If failed.
         */
        protected Object readResolve() throws ObjectStreamException {
            try {
                return new IgniteHolder(IgnitionEx.localIgnite());
            }
            catch (IllegalStateException e) {
                throw U.withCause(new InvalidObjectException(e.getMessage()), e);
            }
        }

    }

}
