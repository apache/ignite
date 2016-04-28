/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.utils;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.binary.*;
import org.apache.ignite.platform.dotnet.PlatformDotNetBinaryConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetBinaryTypeConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetCacheStoreFactoryNative;
import org.apache.ignite.platform.dotnet.PlatformDotNetConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration utils.
 */
@SuppressWarnings("unchecked")
public class PlatformConfigurationUtils {
    /**
     * Write .Net configuration to the stream.
     *
     * @param writer Writer.
     * @param cfg Configuration.
     */
    public static void writeDotNetConfiguration(BinaryRawWriterEx writer, PlatformDotNetConfiguration cfg) {
        // 1. Write assemblies.
        PlatformUtils.writeNullableCollection(writer, cfg.getAssemblies());

        PlatformDotNetBinaryConfiguration binaryCfg = cfg.getBinaryConfiguration();

        if (binaryCfg != null) {
            writer.writeBoolean(true);

            PlatformUtils.writeNullableCollection(writer, binaryCfg.getTypesConfiguration(),
                new PlatformWriterClosure<PlatformDotNetBinaryTypeConfiguration>() {
                    @Override public void write(BinaryRawWriterEx writer, PlatformDotNetBinaryTypeConfiguration typ) {
                        writer.writeString(typ.getTypeName());
                        writer.writeString(typ.getNameMapper());
                        writer.writeString(typ.getIdMapper());
                        writer.writeString(typ.getSerializer());
                        writer.writeString(typ.getAffinityKeyFieldName());
                        writer.writeObject(typ.getKeepDeserialized());
                        writer.writeBoolean(typ.isEnum());
                    }
                });

            PlatformUtils.writeNullableCollection(writer, binaryCfg.getTypes());
            writer.writeString(binaryCfg.getDefaultNameMapper());
            writer.writeString(binaryCfg.getDefaultIdMapper());
            writer.writeString(binaryCfg.getDefaultSerializer());
            writer.writeBoolean(binaryCfg.isDefaultKeepDeserialized());
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Reads cache configuration from a stream.
     *
     * @param in Stream.
     * @return Cache configuration.
     */
    public static CacheConfiguration readCacheConfiguration(BinaryRawReaderEx in) {
        assert in != null;

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(CacheAtomicityMode.fromOrdinal(in.readInt()));
        ccfg.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.fromOrdinal((byte)in.readInt()));
        ccfg.setBackups(in.readInt());
        ccfg.setCacheMode(CacheMode.fromOrdinal(in.readInt()));
        ccfg.setCopyOnRead(in.readBoolean());
        ccfg.setEagerTtl(in.readBoolean());
        ccfg.setSwapEnabled(in.readBoolean());
        ccfg.setEvictSynchronized(in.readBoolean());
        ccfg.setEvictSynchronizedConcurrencyLevel(in.readInt());
        ccfg.setEvictSynchronizedKeyBufferSize(in.readInt());
        ccfg.setEvictSynchronizedTimeout(in.readLong());
        ccfg.setInvalidate(in.readBoolean());
        ccfg.setStoreKeepBinary(in.readBoolean());
        ccfg.setLoadPreviousValue(in.readBoolean());
        ccfg.setDefaultLockTimeout(in.readLong());
        ccfg.setLongQueryWarningTimeout(in.readLong());
        ccfg.setMaxConcurrentAsyncOperations(in.readInt());
        ccfg.setEvictMaxOverflowRatio(in.readFloat());
        ccfg.setMemoryMode(CacheMemoryMode.values()[in.readInt()]);
        ccfg.setName(in.readString());
        ccfg.setOffHeapMaxMemory(in.readLong());
        ccfg.setReadFromBackup(in.readBoolean());
        ccfg.setRebalanceBatchSize(in.readInt());
        ccfg.setRebalanceDelay(in.readLong());
        ccfg.setRebalanceMode(CacheRebalanceMode.fromOrdinal(in.readInt()));
        ccfg.setRebalanceThrottle(in.readLong());
        ccfg.setRebalanceTimeout(in.readLong());
        ccfg.setSqlEscapeAll(in.readBoolean());
        ccfg.setSqlOnheapRowCacheSize(in.readInt());
        ccfg.setStartSize(in.readInt());
        ccfg.setWriteBehindBatchSize(in.readInt());
        ccfg.setWriteBehindEnabled(in.readBoolean());
        ccfg.setWriteBehindFlushFrequency(in.readLong());
        ccfg.setWriteBehindFlushSize(in.readInt());
        ccfg.setWriteBehindFlushThreadCount(in.readInt());
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.fromOrdinal(in.readInt()));
        ccfg.setReadThrough(in.readBoolean());
        ccfg.setWriteThrough(in.readBoolean());

        Object storeFactory = in.readObjectDetached();

        if (storeFactory != null)
            ccfg.setCacheStoreFactory(new PlatformDotNetCacheStoreFactoryNative(storeFactory));

        int qryEntCnt = in.readInt();

        if (qryEntCnt > 0) {
            Collection<QueryEntity> entities = new ArrayList<>(qryEntCnt);

            for (int i = 0; i < qryEntCnt; i++)
                entities.add(readQueryEntity(in));

            ccfg.setQueryEntities(entities);
        }

        if (in.readBoolean())
            ccfg.setNearConfiguration(readNearConfiguration(in));

        return ccfg;
    }

    /**
     * Reads the near config.
     *
     * @param in Stream.
     * @return NearCacheConfiguration.
     */
    public static NearCacheConfiguration readNearConfiguration(BinaryRawReader in) {
        NearCacheConfiguration cfg = new NearCacheConfiguration();
        cfg.setNearStartSize(in.readInt());

        byte plcTyp = in.readByte();

        switch (plcTyp) {
            case 0:
                break;
            case 1: {
                FifoEvictionPolicy p = new FifoEvictionPolicy();
                p.setBatchSize(in.readInt());
                p.setMaxSize(in.readInt());
                p.setMaxMemorySize(in.readLong());
                cfg.setNearEvictionPolicy(p);
                break;
            }
            case 2: {
                LruEvictionPolicy p = new LruEvictionPolicy();
                p.setBatchSize(in.readInt());
                p.setMaxSize(in.readInt());
                p.setMaxMemorySize(in.readLong());
                cfg.setNearEvictionPolicy(p);
                break;
            }
            default:
                assert false;
        }

        return cfg;
    }

    /**
     * Reads the near config.
     *
     * @param out Stream.
     * @param cfg NearCacheConfiguration.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public static void writeNearConfiguration(BinaryRawWriter out, NearCacheConfiguration cfg) {
        assert cfg != null;

        out.writeInt(cfg.getNearStartSize());

        EvictionPolicy p = cfg.getNearEvictionPolicy();

        if (p instanceof FifoEvictionPolicy) {
            out.writeByte((byte)1);

            FifoEvictionPolicy p0 = (FifoEvictionPolicy)p;
            out.writeInt(p0.getBatchSize());
            out.writeInt(p0.getMaxSize());
            out.writeLong(p0.getMaxMemorySize());
        }
        else if (p instanceof LruEvictionPolicy) {
            out.writeByte((byte)2);

            LruEvictionPolicy p0 = (LruEvictionPolicy)p;
            out.writeInt(p0.getBatchSize());
            out.writeInt(p0.getMaxSize());
            out.writeLong(p0.getMaxMemorySize());
        }
        else {
            out.writeByte((byte)0);
        }
    }

    /**
     * Reads the query entity.
     *
     * @param in Stream.
     * @return QueryEntity.
     */
    public static QueryEntity readQueryEntity(BinaryRawReader in) {
        QueryEntity res = new QueryEntity();

        res.setKeyType(in.readString());
        res.setValueType(in.readString());

        // Fields
        int cnt = in.readInt();

        if (cnt > 0) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>(cnt);

            for (int i = 0; i < cnt; i++)
                fields.put(in.readString(), in.readString());

            res.setFields(fields);
        }

        // Aliases
        cnt = in.readInt();

        if (cnt > 0) {
            Map<String, String> aliases = new HashMap<>(cnt);

            for (int i = 0; i < cnt; i++)
                aliases.put(in.readString(), in.readString());

            res.setAliases(aliases);
        }

        // Indexes
        cnt = in.readInt();

        if (cnt > 0) {
            Collection<QueryIndex> indexes = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++)
                indexes.add(readQueryIndex(in));

            res.setIndexes(indexes);
        }

        return res;
    }

    /**
     * Reads the query index.
     *
     * @param in Reader.
     * @return Query index.
     */
    public static QueryIndex readQueryIndex(BinaryRawReader in) {
        QueryIndex res = new QueryIndex();

        res.setName(in.readString());
        res.setIndexType(QueryIndexType.values()[in.readByte()]);

        int cnt = in.readInt();

        if (cnt > 0) {
            LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>(cnt);

            for (int i = 0; i < cnt; i++)
                fields.put(in.readString(), !in.readBoolean());

            res.setFields(fields);
        }

        return res;
    }

    /**
     * Reads Ignite configuration.
     * @param in Reader.
     * @param cfg Configuration.
     */
    public static void readIgniteConfiguration(BinaryRawReaderEx in, IgniteConfiguration cfg) {
        if (!in.readBoolean())
            return;  // there is no config

        cfg.setClientMode(in.readBoolean());
        cfg.setIncludeEventTypes(in.readIntArray());
        cfg.setMetricsExpireTime(in.readLong());
        cfg.setMetricsHistorySize(in.readInt());
        cfg.setMetricsLogFrequency(in.readLong());
        cfg.setMetricsUpdateFrequency(in.readLong());
        cfg.setNetworkSendRetryCount(in.readInt());
        cfg.setNetworkSendRetryDelay(in.readLong());
        cfg.setNetworkTimeout(in.readLong());
        cfg.setWorkDirectory(in.readString());
        cfg.setLocalHost(in.readString());
        cfg.setDaemon(in.readBoolean());
        cfg.setLateAffinityAssignment(in.readBoolean());

        readCacheConfigurations(in, cfg);
        readDiscoveryConfiguration(in, cfg);

        if (in.readBoolean()) {
            if (cfg.getBinaryConfiguration() == null)
                cfg.setBinaryConfiguration(new BinaryConfiguration());

            cfg.getBinaryConfiguration().setCompactFooter(in.readBoolean());
        }

        int attrCnt = in.readInt();

        if (attrCnt > 0) {
            Map<String, Object> attrs = new HashMap<>(attrCnt);

            for (int i = 0; i < attrCnt; i++)
                attrs.put(in.readString(), in.readObject());

            cfg.setUserAttributes(attrs);
        }

        if (in.readBoolean()) {
            AtomicConfiguration atomic = new AtomicConfiguration();

            atomic.setAtomicSequenceReserveSize(in.readInt());
            atomic.setBackups(in.readInt());
            atomic.setCacheMode(CacheMode.fromOrdinal(in.readInt()));

            cfg.setAtomicConfiguration(atomic);
        }

        if (in.readBoolean()) {
            TransactionConfiguration tx = new TransactionConfiguration();

            tx.setPessimisticTxLogSize(in.readInt());
            tx.setDefaultTxConcurrency(TransactionConcurrency.fromOrdinal(in.readInt()));
            tx.setDefaultTxIsolation(TransactionIsolation.fromOrdinal(in.readInt()));
            tx.setDefaultTxTimeout(in.readLong());
            tx.setPessimisticTxLogLinger(in.readInt());

            cfg.setTransactionConfiguration(tx);
        }
    }

    /**
     * Reads cache configurations from a stream and updates provided IgniteConfiguration.
     *
     * @param cfg IgniteConfiguration to update.
     * @param in Reader.
     */
    public static void readCacheConfigurations(BinaryRawReaderEx in, IgniteConfiguration cfg) {
        int len = in.readInt();

        if (len == 0)
            return;

        List<CacheConfiguration> caches = new ArrayList<>();

        for (int i = 0; i < len; i++)
            caches.add(readCacheConfiguration(in));

        CacheConfiguration[] oldCaches = cfg.getCacheConfiguration();
        CacheConfiguration[] caches0 = caches.toArray(new CacheConfiguration[caches.size()]);

        if (oldCaches == null)
            cfg.setCacheConfiguration(caches0);
        else {
            CacheConfiguration[] mergedCaches = new CacheConfiguration[oldCaches.length + caches.size()];

            System.arraycopy(oldCaches, 0, mergedCaches, 0, oldCaches.length);
            System.arraycopy(caches0, 0, mergedCaches, oldCaches.length, caches.size());

            cfg.setCacheConfiguration(mergedCaches);
        }
    }

    /**
     * Reads discovery configuration from a stream and updates provided IgniteConfiguration.
     *
     * @param cfg IgniteConfiguration to update.
     * @param in Reader.
     */
    public static void readDiscoveryConfiguration(BinaryRawReader in, IgniteConfiguration cfg) {
        boolean hasConfig = in.readBoolean();

        if (!hasConfig)
            return;

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        boolean hasIpFinder = in.readBoolean();

        if (hasIpFinder) {
            byte ipFinderType = in.readByte();

            int addrCount = in.readInt();

            ArrayList<String> addrs = null;

            if (addrCount > 0) {
                addrs = new ArrayList<>(addrCount);

                for (int i = 0; i < addrCount; i++)
                    addrs.add(in.readString());
            }

            TcpDiscoveryVmIpFinder finder = null;
            if (ipFinderType == 1) {
                finder = new TcpDiscoveryVmIpFinder();
            }
            else if (ipFinderType == 2) {
                TcpDiscoveryMulticastIpFinder finder0 = new TcpDiscoveryMulticastIpFinder();

                finder0.setLocalAddress(in.readString());
                finder0.setMulticastGroup(in.readString());
                finder0.setMulticastPort(in.readInt());
                finder0.setAddressRequestAttempts(in.readInt());
                finder0.setResponseWaitTime(in.readInt());

                boolean hasTtl = in.readBoolean();

                if (hasTtl)
                    finder0.setTimeToLive(in.readInt());

                finder = finder0;
            }
            else {
                assert false;
            }

            finder.setAddresses(addrs);

            disco.setIpFinder(finder);
        }

        disco.setSocketTimeout(in.readLong());
        disco.setAckTimeout(in.readLong());
        disco.setMaxAckTimeout(in.readLong());
        disco.setNetworkTimeout(in.readLong());
        disco.setJoinTimeout(in.readLong());

        cfg.setDiscoverySpi(disco);
    }

    /**
     * Writes cache configuration.
     *
     * @param writer Writer.
     * @param ccfg Configuration.
     */
    public static void writeCacheConfiguration(BinaryRawWriter writer, CacheConfiguration ccfg) {
        assert writer != null;
        assert ccfg != null;

        writer.writeInt(ccfg.getAtomicityMode() == null ?
            CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE.ordinal() : ccfg.getAtomicityMode().ordinal());
        writer.writeInt(ccfg.getAtomicWriteOrderMode() == null ? 0 : ccfg.getAtomicWriteOrderMode().ordinal());
        writer.writeInt(ccfg.getBackups());
        writer.writeInt(ccfg.getCacheMode() == null ?
            CacheConfiguration.DFLT_CACHE_MODE.ordinal() : ccfg.getCacheMode().ordinal());
        writer.writeBoolean(ccfg.isCopyOnRead());
        writer.writeBoolean(ccfg.isEagerTtl());
        writer.writeBoolean(ccfg.isSwapEnabled());
        writer.writeBoolean(ccfg.isEvictSynchronized());
        writer.writeInt(ccfg.getEvictSynchronizedConcurrencyLevel());
        writer.writeInt(ccfg.getEvictSynchronizedKeyBufferSize());
        writer.writeLong(ccfg.getEvictSynchronizedTimeout());
        writer.writeBoolean(ccfg.isInvalidate());
        writer.writeBoolean(ccfg.isStoreKeepBinary());
        writer.writeBoolean(ccfg.isLoadPreviousValue());
        writer.writeLong(ccfg.getDefaultLockTimeout());
        writer.writeLong(ccfg.getLongQueryWarningTimeout());
        writer.writeInt(ccfg.getMaxConcurrentAsyncOperations());
        writer.writeFloat(ccfg.getEvictMaxOverflowRatio());
        writer.writeInt(ccfg.getMemoryMode() == null ?
            CacheConfiguration.DFLT_MEMORY_MODE.ordinal() : ccfg.getMemoryMode().ordinal());
        writer.writeString(ccfg.getName());
        writer.writeLong(ccfg.getOffHeapMaxMemory());
        writer.writeBoolean(ccfg.isReadFromBackup());
        writer.writeInt(ccfg.getRebalanceBatchSize());
        writer.writeLong(ccfg.getRebalanceDelay());
        writer.writeInt(ccfg.getRebalanceMode() == null ?
            CacheConfiguration.DFLT_REBALANCE_MODE.ordinal() : ccfg.getRebalanceMode().ordinal());
        writer.writeLong(ccfg.getRebalanceThrottle());
        writer.writeLong(ccfg.getRebalanceTimeout());
        writer.writeBoolean(ccfg.isSqlEscapeAll());
        writer.writeInt(ccfg.getSqlOnheapRowCacheSize());
        writer.writeInt(ccfg.getStartSize());
        writer.writeInt(ccfg.getWriteBehindBatchSize());
        writer.writeBoolean(ccfg.isWriteBehindEnabled());
        writer.writeLong(ccfg.getWriteBehindFlushFrequency());
        writer.writeInt(ccfg.getWriteBehindFlushSize());
        writer.writeInt(ccfg.getWriteBehindFlushThreadCount());
        writer.writeInt(ccfg.getWriteSynchronizationMode() == null ? 0 : ccfg.getWriteSynchronizationMode().ordinal());
        writer.writeBoolean(ccfg.isReadThrough());
        writer.writeBoolean(ccfg.isWriteThrough());

        if (ccfg.getCacheStoreFactory() instanceof PlatformDotNetCacheStoreFactoryNative)
            writer.writeObject(((PlatformDotNetCacheStoreFactoryNative)ccfg.getCacheStoreFactory()).getNativeFactory());
        else
            writer.writeObject(null);

        Collection<QueryEntity> qryEntities = ccfg.getQueryEntities();

        if (qryEntities != null) {
            writer.writeInt(qryEntities.size());

            for (QueryEntity e : qryEntities)
                writeQueryEntity(writer, e);
        }
        else
            writer.writeInt(0);

        NearCacheConfiguration nearCfg = ccfg.getNearConfiguration();

        if (nearCfg != null) {
            writer.writeBoolean(true);

            writeNearConfiguration(writer, nearCfg);
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Write query entity.
     *
     * @param writer Writer.
     * @param queryEntity Query entity.
     */
    private static void writeQueryEntity(BinaryRawWriter writer, QueryEntity queryEntity) {
        assert queryEntity != null;

        writer.writeString(queryEntity.getKeyType());
        writer.writeString(queryEntity.getValueType());

        // Fields
        LinkedHashMap<String, String> fields = queryEntity.getFields();

        if (fields != null) {
            writer.writeInt(fields.size());

            for (Map.Entry<String, String> field : fields.entrySet()) {
                writer.writeString(field.getKey());
                writer.writeString(field.getValue());
            }
        }
        else
            writer.writeInt(0);

        // Aliases
        Map<String, String> aliases = queryEntity.getAliases();

        if (aliases != null) {
            writer.writeInt(aliases.size());

            for (Map.Entry<String, String> alias : aliases.entrySet()) {
                writer.writeString(alias.getKey());
                writer.writeString(alias.getValue());
            }
        }
        else
            writer.writeInt(0);

        // Indexes
        Collection<QueryIndex> indexes = queryEntity.getIndexes();

        if (indexes != null) {
            writer.writeInt(indexes.size());

            for (QueryIndex index : indexes)
                writeQueryIndex(writer, index);
        }
        else
            writer.writeInt(0);
    }

    /**
     * Writer query index.
     *
     * @param writer Writer.
     * @param index Index.
     */
    private static void writeQueryIndex(BinaryRawWriter writer, QueryIndex index) {
        assert index != null;

        writer.writeString(index.getName());
        writer.writeByte((byte)index.getIndexType().ordinal());

        LinkedHashMap<String, Boolean> fields = index.getFields();

        if (fields != null) {
            writer.writeInt(fields.size());

            for (Map.Entry<String, Boolean> field : fields.entrySet()) {
                writer.writeString(field.getKey());
                writer.writeBoolean(!field.getValue());
            }
        }
        else
            writer.writeInt(0);
    }

    /**
     * Writes Ignite configuration.
     *
     * @param w Writer.
     * @param cfg Configuration.
     */
    public static void writeIgniteConfiguration(BinaryRawWriter w, IgniteConfiguration cfg) {
        assert w != null;
        assert cfg != null;

        w.writeBoolean(cfg.isClientMode());
        w.writeIntArray(cfg.getIncludeEventTypes());
        w.writeLong(cfg.getMetricsExpireTime());
        w.writeInt(cfg.getMetricsHistorySize());
        w.writeLong(cfg.getMetricsLogFrequency());
        w.writeLong(cfg.getMetricsUpdateFrequency());
        w.writeInt(cfg.getNetworkSendRetryCount());
        w.writeLong(cfg.getNetworkSendRetryDelay());
        w.writeLong(cfg.getNetworkTimeout());
        w.writeString(cfg.getWorkDirectory());
        w.writeString(cfg.getLocalHost());
        w.writeBoolean(cfg.isDaemon());
        w.writeBoolean(cfg.isLateAffinityAssignment());

        CacheConfiguration[] cacheCfg = cfg.getCacheConfiguration();

        if (cacheCfg != null) {
            w.writeInt(cacheCfg.length);

            for (CacheConfiguration ccfg : cacheCfg)
                writeCacheConfiguration(w, ccfg);
        }
        else
            w.writeInt(0);

        writeDiscoveryConfiguration(w, cfg.getDiscoverySpi());

        BinaryConfiguration bc = cfg.getBinaryConfiguration();
        w.writeBoolean(bc != null);

        if (bc != null)
            w.writeBoolean(bc.isCompactFooter());

        Map<String, ?> attrs = cfg.getUserAttributes();

        if (attrs != null) {
            w.writeInt(attrs.size());

            for (Map.Entry<String, ?> e : attrs.entrySet()) {
                w.writeString(e.getKey());
                w.writeObject(e.getValue());
            }
        }
        else
            w.writeInt(0);

        AtomicConfiguration atomic = cfg.getAtomicConfiguration();

        if (atomic != null) {
            w.writeBoolean(true);

            w.writeInt(atomic.getAtomicSequenceReserveSize());
            w.writeInt(atomic.getBackups());
            w.writeInt(atomic.getCacheMode().ordinal());
        }
        else
            w.writeBoolean(false);

        TransactionConfiguration tx = cfg.getTransactionConfiguration();

        if (tx != null) {
            w.writeBoolean(true);

            w.writeInt(tx.getPessimisticTxLogSize());
            w.writeInt(tx.getDefaultTxConcurrency().ordinal());
            w.writeInt(tx.getDefaultTxIsolation().ordinal());
            w.writeLong(tx.getDefaultTxTimeout());
            w.writeInt(tx.getPessimisticTxLogLinger());
        }
        else
            w.writeBoolean(false);

        w.writeString(cfg.getIgniteHome());

        w.writeLong(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getInit());
        w.writeLong(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
    }

    /**
     * Writes discovery configuration.
     *
     * @param w Writer.
     * @param spi Disco.
     */
    private static void writeDiscoveryConfiguration(BinaryRawWriter w, DiscoverySpi spi) {
        assert w != null;
        assert spi != null;

        if (!(spi instanceof TcpDiscoverySpi)) {
            w.writeBoolean(false);
            return;
        }

        w.writeBoolean(true);

        TcpDiscoverySpi tcp = (TcpDiscoverySpi)spi;

        TcpDiscoveryIpFinder finder = tcp.getIpFinder();

        if (finder instanceof TcpDiscoveryVmIpFinder) {
            w.writeBoolean(true);

            boolean isMulticast = finder instanceof TcpDiscoveryMulticastIpFinder;

            w.writeByte((byte)(isMulticast ? 2 : 1));

            Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

            w.writeInt(addrs.size());

            for (InetSocketAddress a : addrs)
                w.writeString(a.toString());

            if (isMulticast) {
                TcpDiscoveryMulticastIpFinder multiFinder = (TcpDiscoveryMulticastIpFinder) finder;

                w.writeString(multiFinder.getLocalAddress());
                w.writeString(multiFinder.getMulticastGroup());
                w.writeInt(multiFinder.getMulticastPort());
                w.writeInt(multiFinder.getAddressRequestAttempts());
                w.writeInt(multiFinder.getResponseWaitTime());

                int ttl = multiFinder.getTimeToLive();
                w.writeBoolean(ttl != -1);

                if (ttl != -1)
                    w.writeInt(ttl);
            }
        }
        else {
            w.writeBoolean(false);
        }

        w.writeLong(tcp.getSocketTimeout());
        w.writeLong(tcp.getAckTimeout());
        w.writeLong(tcp.getMaxAckTimeout());
        w.writeLong(tcp.getNetworkTimeout());
        w.writeLong(tcp.getJoinTimeout());
    }

    /**
     * Private constructor.
     */
    private PlatformConfigurationUtils() {
        // No-op.
    }
}
