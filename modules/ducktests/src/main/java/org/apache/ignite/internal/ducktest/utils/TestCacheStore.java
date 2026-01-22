package org.apache.ignite.internal.ducktest.utils;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilders;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

public class TestCacheStore implements CacheStore<Object, Object> {
    private final Ignite ignite;

    private final IgniteCache<Object, Object> cache;

    private final AtomicBoolean registerScheme = new AtomicBoolean();

    private final AtomicReference<BinaryObjectImpl> binObjRef = new AtomicReference<>();

    public TestCacheStore() {
        System.err.println("TEST | Starting ExtCacheStore");

        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder()
            .setAddresses(F.asList("ducker02:45000"));

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(ipFinder)
                .setLocalPort(45000)
        ).setCommunicationSpi(
            new TcpCommunicationSpi().setLocalPort(46000)
        ).setClientMode(true)
            .setPeerClassLoadingEnabled(true);

        ignite = Ignition.start(cfg);

        System.err.println("TEST | Started ExtCacheStore");

        cache = ignite.cache("EXT_STORAGE_CACHE");

        assert cache != null;

        System.err.println("TEST | cache: " + cache);
    }

    @Override public void loadCache(IgniteBiInClosure clo, @Nullable Object... args) throws CacheLoaderException {
        throw new UnsupportedOperationException();
    }

    @Override public void sessionEnd(boolean commit) throws CacheWriterException {
        // No-op.
    }

    @Override public Object load(Object key) throws CacheLoaderException {
        System.err.println("TEST | ExternalStorage: load, key=" + key);

        DTO val = (DTO)cache.get(key);

        System.err.println("TEST | ExternalStorage: load, value=" + val);

        if (val == null)
            return null;

        BinaryObjectImpl bo = binObjRef.get();

//        BinaryObjectBuilder bob = BinaryObjectBuilders.builder(bo);

        BinaryObjectBuilder bob = BinaryObjectBuilders.builder(bo.context(),
            bo.context().metadata0(bo.typeId()).typeName());

        assert bo != null;

        bob.setField("STRVAL", val.strVal);
        bob.setField("DECVAL", val.decVal);

        BinaryObject newBo = bob.build();

        System.err.println("TEST | ExternalStorage: load, key=" + key + ", bo=" + bo + ", newBo=" + newBo);

        return newBo;
    }

    @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
        System.err.println("TEST | ExternalStorage: loadAll");

        Map<Object, Object> res = new HashMap<>();

        keys.forEach(key -> res.put(key, load(key)));

        return res;
    }

    @Override public void write(Cache.Entry entry) throws CacheWriterException {
        // System.err.println("TEST | ExternalStorage: write, keyClass=" + entry.getValue().getClass().getSimpleName());
        Object val = entry.getValue();

        if (val instanceof BinaryObjectImpl && !registerScheme.get()) {
            synchronized (registerScheme) {
                if (!registerScheme.get()) {
                    BinaryObjectImpl bo = (BinaryObjectImpl)val;

                    binObjRef.set(bo);

                    registerScheme.set(true);
                }
            }
        }

        if (val instanceof BinaryObjectImpl) {
            BinaryObjectImpl bo = (BinaryObjectImpl)val;

            DTO dto = new DTO();

            dto.id = (Integer)entry.getKey();
            dto.strVal = bo.field("STRVAL");
            dto.decVal = bo.field("DECVAL");

            val = dto;
        }

        cache.put(entry.getKey(), val);
    }

    @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) throws CacheWriterException {
        entries.forEach(e -> cache.put(e.getKey(), e.getValue()));
    }

    @Override public void delete(Object key) throws CacheWriterException {
        cache.remove(key);
    }

    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        Set<Object> keys0 = keys instanceof Set ? (Set<Object>)keys : new HashSet<>(keys);

        cache.removeAll(keys0);
    }

    public static final class DTO {
        int id;
        String strVal;
        BigDecimal decVal;

        @Override public String toString() {
            return "DTO{" +
                "id=" + id +
                ", decVal=" + decVal +
                ", strVal='" + strVal + '\'' +
                '}';
        }
    }

    public static final class IgniteCacheStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new TestCacheStore();
        }
    }
}
