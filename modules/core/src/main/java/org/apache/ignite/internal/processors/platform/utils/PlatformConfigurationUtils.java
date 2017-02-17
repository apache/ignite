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

import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryFieldIdentityResolver;
import org.apache.ignite.binary.BinaryIdentityResolver;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
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
import org.apache.ignite.internal.processors.platform.cache.affinity.PlatformAffinityFunction;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.platform.dotnet.*;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpiMBean;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Configuration utils.
 */
@SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
public class PlatformConfigurationUtils {
    /** */
    private static final byte SWAP_TYP_NONE = 0;

    /** */
    private static final byte SWAP_TYP_FILE = 1;

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
        ccfg.setStatisticsEnabled(in.readBoolean());

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

        ccfg.setEvictionPolicy(readEvictionPolicy(in));
        ccfg.setAffinity(readAffinityFunction(in));
        ccfg.setExpiryPolicyFactory(readExpiryPolicyFactory(in));

        return ccfg;
    }

    /**
     * Reads the expiry policy factory.
     *
     * @param in Reader.
     * @return Expiry policy factory.
     */
    private static Factory<? extends ExpiryPolicy> readExpiryPolicyFactory(BinaryRawReader in) {
        if (!in.readBoolean())
            return null;

        return new PlatformExpiryPolicyFactory(in.readLong(), in.readLong(), in.readLong());
    }

    /**
     * Writes the policy factory.
     *
     * @param out Writer.
     */
    private static void writeExpiryPolicyFactory(BinaryRawWriter out, Factory<? extends ExpiryPolicy> factory) {
        if (!(factory instanceof PlatformExpiryPolicyFactory)) {
            out.writeBoolean(false);

            return;
        }

        out.writeBoolean(true);

        PlatformExpiryPolicyFactory f = (PlatformExpiryPolicyFactory)factory;

        out.writeLong(f.getCreate());
        out.writeLong(f.getUpdate());
        out.writeLong(f.getAccess());
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
        cfg.setNearEvictionPolicy(readEvictionPolicy(in));

        return cfg;
    }

    /**
     * Reads the eviction policy.
     *
     * @param in Stream.
     * @return Eviction policy.
     */
    private static EvictionPolicy readEvictionPolicy(BinaryRawReader in) {
        byte plcTyp = in.readByte();

        switch (plcTyp) {
            case 0:
                break;
            case 1: {
                FifoEvictionPolicy p = new FifoEvictionPolicy();
                p.setBatchSize(in.readInt());
                p.setMaxSize(in.readInt());
                p.setMaxMemorySize(in.readLong());
                return p;
            }
            case 2: {
                LruEvictionPolicy p = new LruEvictionPolicy();
                p.setBatchSize(in.readInt());
                p.setMaxSize(in.readInt());
                p.setMaxMemorySize(in.readLong());
                return p;
            }
            default:
                assert false;
        }

        return null;
    }

    /**
     * Reads the eviction policy.
     *
     * @param in Stream.
     * @return Affinity function.
     */
    public static PlatformAffinityFunction readAffinityFunction(BinaryRawReaderEx in) {
        byte plcTyp = in.readByte();

        if (plcTyp == 0)
            return null;

        int partitions = in.readInt();
        boolean exclNeighbours = in.readBoolean();
        byte overrideFlags = in.readByte();
        Object userFunc = in.readObjectDetached();

        AffinityFunction baseFunc = null;

        switch (plcTyp) {
            case 1: {
                FairAffinityFunction f = new FairAffinityFunction();
                f.setPartitions(partitions);
                f.setExcludeNeighbors(exclNeighbours);
                baseFunc = f;
                break;
            }
            case 2: {
                RendezvousAffinityFunction f = new RendezvousAffinityFunction();
                f.setPartitions(partitions);
                f.setExcludeNeighbors(exclNeighbours);
                baseFunc = f;
                break;
            }
            default:
                assert plcTyp == 3;
        }

        return new PlatformAffinityFunction(userFunc, partitions, overrideFlags, baseFunc);
    }

    /**
     * Reads the near config.
     *
     * @param out Stream.
     * @param cfg NearCacheConfiguration.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static void writeNearConfiguration(BinaryRawWriter out, NearCacheConfiguration cfg) {
        assert cfg != null;

        out.writeInt(cfg.getNearStartSize());
        writeEvictionPolicy(out, cfg.getNearEvictionPolicy());
    }

    /**
     * Writes the affinity functions.
     *
     * @param out Stream.
     * @param f Affinity.
     */
    private static void writeAffinityFunction(BinaryRawWriter out, AffinityFunction f) {
        if (f instanceof PlatformDotNetAffinityFunction)
            f = ((PlatformDotNetAffinityFunction)f).getFunc();

        if (f instanceof FairAffinityFunction) {
            out.writeByte((byte) 1);

            FairAffinityFunction f0 = (FairAffinityFunction) f;
            out.writeInt(f0.getPartitions());
            out.writeBoolean(f0.isExcludeNeighbors());
            out.writeByte((byte) 0);  // override flags
            out.writeObject(null);  // user func
        } else if (f instanceof RendezvousAffinityFunction) {
            out.writeByte((byte) 2);

            RendezvousAffinityFunction f0 = (RendezvousAffinityFunction) f;
            out.writeInt(f0.getPartitions());
            out.writeBoolean(f0.isExcludeNeighbors());
            out.writeByte((byte) 0);  // override flags
            out.writeObject(null);  // user func
        } else if (f instanceof PlatformAffinityFunction) {
            PlatformAffinityFunction f0 = (PlatformAffinityFunction) f;
            AffinityFunction baseFunc = f0.getBaseFunc();

            if (baseFunc instanceof FairAffinityFunction) {
                out.writeByte((byte) 1);
                out.writeInt(f0.partitions());
                out.writeBoolean(((FairAffinityFunction) baseFunc).isExcludeNeighbors());
                out.writeByte(f0.getOverrideFlags());
                out.writeObject(f0.getUserFunc());
            } else if (baseFunc instanceof RendezvousAffinityFunction) {
                out.writeByte((byte) 2);
                out.writeInt(f0.partitions());
                out.writeBoolean(((RendezvousAffinityFunction) baseFunc).isExcludeNeighbors());
                out.writeByte(f0.getOverrideFlags());
                out.writeObject(f0.getUserFunc());
            } else {
                out.writeByte((byte) 3);
                out.writeInt(f0.partitions());
                out.writeBoolean(false);  // exclude neighbors
                out.writeByte(f0.getOverrideFlags());
                out.writeObject(f0.getUserFunc());
            }
        } else {
            out.writeByte((byte) 0);
        }
    }

    /**
     * Writes the eviction policy.
     * @param out Stream.
     * @param p Policy.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static void writeEvictionPolicy(BinaryRawWriter out, EvictionPolicy p) {
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
    private static QueryEntity readQueryEntity(BinaryRawReader in) {
        QueryEntity res = new QueryEntity();

        res.setKeyType(in.readString());
        res.setValueType(in.readString());

        // Fields
        int cnt = in.readInt();
        Set<String> keyFields = new HashSet<>(cnt);

        if (cnt > 0) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>(cnt);

            for (int i = 0; i < cnt; i++) {
                String fieldName = in.readString();
                String fieldType = in.readString();

                fields.put(fieldName, fieldType);

                if (in.readBoolean())
                    keyFields.add(fieldName);
            }

            res.setFields(fields);

            if (!keyFields.isEmpty())
                res.setKeyFields(keyFields);
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
    private static QueryIndex readQueryIndex(BinaryRawReader in) {
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
        if (in.readBoolean()) cfg.setClientMode(in.readBoolean());
        int[] eventTypes = in.readIntArray(); if (eventTypes != null) cfg.setIncludeEventTypes(eventTypes);
        if (in.readBoolean()) cfg.setMetricsExpireTime(in.readLong());
        if (in.readBoolean()) cfg.setMetricsHistorySize(in.readInt());
        if (in.readBoolean()) cfg.setMetricsLogFrequency(in.readLong());
        if (in.readBoolean()) cfg.setMetricsUpdateFrequency(in.readLong());
        if (in.readBoolean()) cfg.setNetworkSendRetryCount(in.readInt());
        if (in.readBoolean()) cfg.setNetworkSendRetryDelay(in.readLong());
        if (in.readBoolean()) cfg.setNetworkTimeout(in.readLong());
        String workDir = in.readString(); if (workDir != null) cfg.setWorkDirectory(workDir);
        String localHost = in.readString(); if (localHost != null) cfg.setLocalHost(localHost);
        if (in.readBoolean()) cfg.setDaemon(in.readBoolean());
        if (in.readBoolean()) cfg.setLateAffinityAssignment(in.readBoolean());
        if (in.readBoolean()) cfg.setFailureDetectionTimeout(in.readLong());

        readCacheConfigurations(in, cfg);
        readDiscoveryConfiguration(in, cfg);

        if (in.readBoolean()) {
            TcpCommunicationSpi comm = new TcpCommunicationSpi();

            comm.setAckSendThreshold(in.readInt());
            comm.setConnectTimeout(in.readLong());
            comm.setDirectBuffer(in.readBoolean());
            comm.setDirectSendBuffer(in.readBoolean());
            comm.setIdleConnectionTimeout(in.readLong());
            comm.setLocalAddress(in.readString());
            comm.setLocalPort(in.readInt());
            comm.setLocalPortRange(in.readInt());
            comm.setMaxConnectTimeout(in.readLong());
            comm.setMessageQueueLimit(in.readInt());
            comm.setReconnectCount(in.readInt());
            comm.setSelectorsCount(in.readInt());
            comm.setSlowClientQueueLimit(in.readInt());
            comm.setSocketReceiveBuffer(in.readInt());
            comm.setSocketSendBuffer(in.readInt());
            comm.setTcpNoDelay(in.readBoolean());
            comm.setUnacknowledgedMessagesBufferSize(in.readInt());

            cfg.setCommunicationSpi(comm);
        }

        if (in.readBoolean()) {  // binary config is present
            if (cfg.getBinaryConfiguration() == null)
                cfg.setBinaryConfiguration(new BinaryConfiguration());

            if (in.readBoolean())  // compact footer is set
                cfg.getBinaryConfiguration().setCompactFooter(in.readBoolean());

            int typeCnt = in.readInt();

            if (typeCnt > 0) {
                Collection<BinaryTypeConfiguration> types = new ArrayList<>(typeCnt);

                for (int i = 0; i < typeCnt; i++) {
                    BinaryTypeConfiguration type = new BinaryTypeConfiguration(in.readString());

                    type.setEnum(in.readBoolean());
                    type.setIdentityResolver(readBinaryIdentityResolver(in));

                    types.add(type);
                }

                cfg.getBinaryConfiguration().setTypeConfigurations(types);
            }
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

        byte swapType = in.readByte();

        switch (swapType) {
            case SWAP_TYP_FILE: {
                FileSwapSpaceSpi swap = new FileSwapSpaceSpi();

                swap.setBaseDirectory(in.readString());
                swap.setMaximumSparsity(in.readFloat());
                swap.setMaxWriteQueueSize(in.readInt());
                swap.setReadStripesNumber(in.readInt());
                swap.setWriteBufferSize(in.readInt());

                cfg.setSwapSpaceSpi(swap);

                break;
            }

            default:
                assert swapType == SWAP_TYP_NONE;
        }
    }

    /**
     * Reads cache configurations from a stream and updates provided IgniteConfiguration.
     *
     * @param cfg IgniteConfiguration to update.
     * @param in Reader.
     */
    private static void readCacheConfigurations(BinaryRawReaderEx in, IgniteConfiguration cfg) {
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
    private static void readDiscoveryConfiguration(BinaryRawReader in, IgniteConfiguration cfg) {
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

        disco.setForceServerMode(in.readBoolean());
        disco.setClientReconnectDisabled(in.readBoolean());
        disco.setLocalAddress(in.readString());
        disco.setReconnectCount(in.readInt());
        disco.setLocalPort(in.readInt());
        disco.setLocalPortRange(in.readInt());
        disco.setMaxMissedHeartbeats(in.readInt());
        disco.setMaxMissedClientHeartbeats(in.readInt());
        disco.setStatisticsPrintFrequency(in.readLong());
        disco.setIpFinderCleanFrequency(in.readLong());
        disco.setThreadPriority(in.readInt());
        disco.setHeartbeatFrequency(in.readLong());
        disco.setTopHistorySize(in.readInt());

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

        writeEnumInt(writer, ccfg.getAtomicityMode(), CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE);
        writeEnumInt(writer, ccfg.getAtomicWriteOrderMode());
        writer.writeInt(ccfg.getBackups());
        writeEnumInt(writer, ccfg.getCacheMode(), CacheConfiguration.DFLT_CACHE_MODE);
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
        writeEnumInt(writer, ccfg.getMemoryMode(), CacheConfiguration.DFLT_MEMORY_MODE);
        writer.writeString(ccfg.getName());
        writer.writeLong(ccfg.getOffHeapMaxMemory());
        writer.writeBoolean(ccfg.isReadFromBackup());
        writer.writeInt(ccfg.getRebalanceBatchSize());
        writer.writeLong(ccfg.getRebalanceDelay());
        writeEnumInt(writer, ccfg.getRebalanceMode(), CacheConfiguration.DFLT_REBALANCE_MODE);
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
        writeEnumInt(writer, ccfg.getWriteSynchronizationMode());
        writer.writeBoolean(ccfg.isReadThrough());
        writer.writeBoolean(ccfg.isWriteThrough());
        writer.writeBoolean(ccfg.isStatisticsEnabled());

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

        writeEvictionPolicy(writer, ccfg.getEvictionPolicy());
        writeAffinityFunction(writer, ccfg.getAffinity());
        writeExpiryPolicyFactory(writer, ccfg.getExpiryPolicyFactory());
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
            Set<String> keyFields = queryEntity.getKeyFields();

            writer.writeInt(fields.size());

            for (Map.Entry<String, String> field : fields.entrySet()) {
                writer.writeString(field.getKey());
                writer.writeString(field.getValue());
                writer.writeBoolean(keyFields != null && keyFields.contains(field.getKey()));
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
        writeEnumByte(writer, index.getIndexType());

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

        w.writeBoolean(true); w.writeBoolean(cfg.isClientMode());
        w.writeIntArray(cfg.getIncludeEventTypes());
        w.writeBoolean(true); w.writeLong(cfg.getMetricsExpireTime());
        w.writeBoolean(true); w.writeInt(cfg.getMetricsHistorySize());
        w.writeBoolean(true); w.writeLong(cfg.getMetricsLogFrequency());
        w.writeBoolean(true); w.writeLong(cfg.getMetricsUpdateFrequency());
        w.writeBoolean(true); w.writeInt(cfg.getNetworkSendRetryCount());
        w.writeBoolean(true); w.writeLong(cfg.getNetworkSendRetryDelay());
        w.writeBoolean(true); w.writeLong(cfg.getNetworkTimeout());
        w.writeString(cfg.getWorkDirectory());
        w.writeString(cfg.getLocalHost());
        w.writeBoolean(true); w.writeBoolean(cfg.isDaemon());
        w.writeBoolean(true); w.writeBoolean(cfg.isLateAffinityAssignment());
        w.writeBoolean(true); w.writeLong(cfg.getFailureDetectionTimeout());

        CacheConfiguration[] cacheCfg = cfg.getCacheConfiguration();

        if (cacheCfg != null) {
            w.writeInt(cacheCfg.length);

            for (CacheConfiguration ccfg : cacheCfg)
                writeCacheConfiguration(w, ccfg);
        }
        else
            w.writeInt(0);

        writeDiscoveryConfiguration(w, cfg.getDiscoverySpi());

        CommunicationSpi comm = cfg.getCommunicationSpi();

        if (comm instanceof TcpCommunicationSpi) {
            w.writeBoolean(true);
            TcpCommunicationSpiMBean tcp = (TcpCommunicationSpiMBean) comm;

            w.writeInt(tcp.getAckSendThreshold());
            w.writeLong(tcp.getConnectTimeout());
            w.writeBoolean(tcp.isDirectBuffer());
            w.writeBoolean(tcp.isDirectSendBuffer());
            w.writeLong(tcp.getIdleConnectionTimeout());
            w.writeString(tcp.getLocalAddress());
            w.writeInt(tcp.getLocalPort());
            w.writeInt(tcp.getLocalPortRange());
            w.writeLong(tcp.getMaxConnectTimeout());
            w.writeInt(tcp.getMessageQueueLimit());
            w.writeInt(tcp.getReconnectCount());
            w.writeInt(tcp.getSelectorsCount());
            w.writeInt(tcp.getSlowClientQueueLimit());
            w.writeInt(tcp.getSocketReceiveBuffer());
            w.writeInt(tcp.getSocketSendBuffer());
            w.writeBoolean(tcp.isTcpNoDelay());
            w.writeInt(tcp.getUnacknowledgedMessagesBufferSize());
        }
        else
            w.writeBoolean(false);

        BinaryConfiguration bc = cfg.getBinaryConfiguration();

        if (bc != null) {
            w.writeBoolean(true);  // binary config exists
            w.writeBoolean(true);  // compact footer is set
            w.writeBoolean(bc.isCompactFooter());

            Collection<BinaryTypeConfiguration> types = bc.getTypeConfigurations();

            if (types != null) {
                w.writeInt(types.size());

                for (BinaryTypeConfiguration type : types) {
                    w.writeString(type.getTypeName());
                    w.writeBoolean(type.isEnum());
                    writeBinaryIdentityResolver(w, type.getIdentityResolver());
                }
            }
            else
                w.writeInt(0);
        }
        else
            w.writeBoolean(false);

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
            writeEnumInt(w, atomic.getCacheMode(), AtomicConfiguration.DFLT_CACHE_MODE);
        }
        else
            w.writeBoolean(false);

        TransactionConfiguration tx = cfg.getTransactionConfiguration();

        if (tx != null) {
            w.writeBoolean(true);

            w.writeInt(tx.getPessimisticTxLogSize());
            writeEnumInt(w, tx.getDefaultTxConcurrency(), TransactionConfiguration.DFLT_TX_CONCURRENCY);
            writeEnumInt(w, tx.getDefaultTxIsolation(), TransactionConfiguration.DFLT_TX_ISOLATION);
            w.writeLong(tx.getDefaultTxTimeout());
            w.writeInt(tx.getPessimisticTxLogLinger());
        }
        else
            w.writeBoolean(false);

        SwapSpaceSpi swap = cfg.getSwapSpaceSpi();

        if (swap instanceof FileSwapSpaceSpiMBean) {
            w.writeByte(SWAP_TYP_FILE);

            FileSwapSpaceSpiMBean fileSwap = (FileSwapSpaceSpiMBean)swap;

            w.writeString(fileSwap.getBaseDirectory());
            w.writeFloat(fileSwap.getMaximumSparsity());
            w.writeInt(fileSwap.getMaxWriteQueueSize());
            w.writeInt(fileSwap.getReadStripesNumber());
            w.writeInt(fileSwap.getWriteBufferSize());
        }
        else {
            w.writeByte(SWAP_TYP_NONE);
        }

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

        w.writeBoolean(tcp.isForceServerMode());
        w.writeBoolean(tcp.isClientReconnectDisabled());
        w.writeString(tcp.getLocalAddress());
        w.writeInt(tcp.getReconnectCount());
        w.writeInt(tcp.getLocalPort());
        w.writeInt(tcp.getLocalPortRange());
        w.writeInt(tcp.getMaxMissedHeartbeats());
        w.writeInt(tcp.getMaxMissedClientHeartbeats());
        w.writeLong(tcp.getStatisticsPrintFrequency());
        w.writeLong(tcp.getIpFinderCleanFrequency());
        w.writeInt(tcp.getThreadPriority());
        w.writeLong(tcp.getHeartbeatFrequency());
        w.writeInt((int)tcp.getTopHistorySize());
    }

    /**
     * Writes enum as byte.
     *
     * @param w Writer.
     * @param e Enum.
     */
    private static void writeEnumByte(BinaryRawWriter w, Enum e) {
        w.writeByte(e == null ? 0 : (byte)e.ordinal());
    }

    /**
     * Writes enum as int.
     *
     * @param w Writer.
     * @param e Enum.
     */
    private static void writeEnumInt(BinaryRawWriter w, Enum e) {
        w.writeInt(e == null ? 0 : e.ordinal());
    }

    /**
     * Writes enum as int.
     *
     * @param w Writer.
     * @param e Enum.
     */
    private static void writeEnumInt(BinaryRawWriter w, Enum e, Enum def) {
        assert def != null;

        w.writeInt(e == null ? def.ordinal() : e.ordinal());
    }

    /**
     * Reads resolver
     *
     * @param r Reader.
     * @return Resolver.
     */
    private static BinaryIdentityResolver readBinaryIdentityResolver(BinaryRawReader r) {
        int type = r.readByte();

        switch (type) {
            case 0:
                return null;

            case 1:
                return new BinaryArrayIdentityResolver();

            case 2:
                int cnt = r.readInt();

                String[] fields = new String[cnt];

                for (int i = 0; i < cnt; i++)
                    fields[i] = r.readString();

                return new BinaryFieldIdentityResolver().setFieldNames(fields);

            default:
                assert false;
                return null;
        }
    }

    /**
     * Writes the resolver.
     *
     * @param w Writer.
     * @param resolver Resolver.
     */
    private static void writeBinaryIdentityResolver(BinaryRawWriter w, BinaryIdentityResolver resolver) {
        if (resolver instanceof BinaryArrayIdentityResolver)
            w.writeByte((byte)1);
        else if (resolver instanceof BinaryFieldIdentityResolver) {
            w.writeByte((byte)2);

            String[] fields = ((BinaryFieldIdentityResolver)resolver).getFieldNames();

            if (fields != null) {
                w.writeInt(fields.length);

                for (String field : fields)
                    w.writeString(field);
            }
            else
                w.writeInt(0);
        }
        else {
            w.writeByte((byte)0);
        }
    }

    /**
     * Private constructor.
     */
    private PlatformConfigurationUtils() {
        // No-op.
    }
}
