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

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.SqlConnectorConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.cache.affinity.PlatformAffinityFunction;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.processors.platform.events.PlatformLocalEventListener;
import org.apache.ignite.internal.processors.platform.plugin.cache.PlatformCachePluginConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.platform.dotnet.PlatformDotNetAffinityFunction;
import org.apache.ignite.platform.dotnet.PlatformDotNetBinaryConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetBinaryTypeConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetCacheStoreFactoryNative;
import org.apache.ignite.platform.dotnet.PlatformDotNetConfiguration;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.platform.PlatformCachePluginConfigurationClosure;
import org.apache.ignite.plugin.platform.PlatformCachePluginConfigurationClosureFactory;
import org.apache.ignite.plugin.platform.PlatformPluginConfigurationClosure;
import org.apache.ignite.plugin.platform.PlatformPluginConfigurationClosureFactory;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.EventStorageSpi;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_2_0;

/**
 * Configuration utils.
 *
 * WARNING: DO NOT MODIFY THIS FILE without updating corresponding platform code!
 * Each read/write method has a counterpart in .NET platform (see IgniteConfiguration.cs, CacheConfiguration.cs, etc).
 */
@SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
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
     * @param ver Client version.
     * @return Cache configuration.
     */
    public static CacheConfiguration readCacheConfiguration(BinaryRawReaderEx in, ClientListenerProtocolVersion ver) {
        assert in != null;

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(CacheAtomicityMode.fromOrdinal(in.readInt()));
        ccfg.setBackups(in.readInt());
        ccfg.setCacheMode(CacheMode.fromOrdinal(in.readInt()));
        ccfg.setCopyOnRead(in.readBoolean());
        ccfg.setEagerTtl(in.readBoolean());
        ccfg.setInvalidate(in.readBoolean());
        ccfg.setStoreKeepBinary(in.readBoolean());
        ccfg.setLoadPreviousValue(in.readBoolean());
        ccfg.setDefaultLockTimeout(in.readLong());
        //noinspection deprecation
        ccfg.setLongQueryWarningTimeout(in.readLong());
        ccfg.setMaxConcurrentAsyncOperations(in.readInt());
        ccfg.setName(in.readString());
        ccfg.setReadFromBackup(in.readBoolean());
        ccfg.setRebalanceBatchSize(in.readInt());
        ccfg.setRebalanceDelay(in.readLong());
        ccfg.setRebalanceMode(CacheRebalanceMode.fromOrdinal(in.readInt()));
        ccfg.setRebalanceThrottle(in.readLong());
        ccfg.setRebalanceTimeout(in.readLong());
        ccfg.setSqlEscapeAll(in.readBoolean());
        ccfg.setWriteBehindBatchSize(in.readInt());
        ccfg.setWriteBehindEnabled(in.readBoolean());
        ccfg.setWriteBehindFlushFrequency(in.readLong());
        ccfg.setWriteBehindFlushSize(in.readInt());
        ccfg.setWriteBehindFlushThreadCount(in.readInt());
        ccfg.setWriteBehindCoalescing(in.readBoolean());
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.fromOrdinal(in.readInt()));
        ccfg.setReadThrough(in.readBoolean());
        ccfg.setWriteThrough(in.readBoolean());
        ccfg.setStatisticsEnabled(in.readBoolean());

        String dataRegionName = in.readString();

        if (dataRegionName != null)
            //noinspection deprecation
            ccfg.setMemoryPolicyName(dataRegionName);

        ccfg.setPartitionLossPolicy(PartitionLossPolicy.fromOrdinal((byte)in.readInt()));
        ccfg.setGroupName(in.readString());

        Object storeFactory = in.readObjectDetached();

        if (storeFactory != null)
            ccfg.setCacheStoreFactory(new PlatformDotNetCacheStoreFactoryNative(storeFactory));

        ccfg.setSqlIndexMaxInlineSize(in.readInt());
        ccfg.setOnheapCacheEnabled(in.readBoolean());
        ccfg.setStoreConcurrentLoadAllThreshold(in.readInt());
        ccfg.setRebalanceOrder(in.readInt());
        ccfg.setRebalanceBatchesPrefetchCount(in.readLong());
        ccfg.setMaxQueryIteratorsCount(in.readInt());
        ccfg.setQueryDetailMetricsSize(in.readInt());
        ccfg.setQueryParallelism(in.readInt());
        ccfg.setSqlSchema(in.readString());

        int qryEntCnt = in.readInt();

        if (qryEntCnt > 0) {
            Collection<QueryEntity> entities = new ArrayList<>(qryEntCnt);

            for (int i = 0; i < qryEntCnt; i++)
                entities.add(readQueryEntity(in, ver));

            ccfg.setQueryEntities(entities);
        }

        if (in.readBoolean())
            ccfg.setNearConfiguration(readNearConfiguration(in));

        ccfg.setEvictionPolicy(readEvictionPolicy(in));
        if (ccfg.getEvictionPolicy() != null)
            ccfg.setOnheapCacheEnabled(true);

        ccfg.setAffinity(readAffinityFunction(in));
        ccfg.setExpiryPolicyFactory(readExpiryPolicyFactory(in));

        int keyCnt = in.readInt();

        if (keyCnt > 0) {
            CacheKeyConfiguration[] keys = new CacheKeyConfiguration[keyCnt];

            for (int i = 0; i < keyCnt; i++) {
                keys[i] = new CacheKeyConfiguration(in.readString(), in.readString());
            }

            ccfg.setKeyConfiguration(keys);
        }

        int pluginCnt = in.readInt();

        if (pluginCnt > 0) {
            ArrayList<CachePluginConfiguration> plugins = new ArrayList<>();

            for (int i = 0; i < pluginCnt; i++) {
                if (in.readBoolean()) {
                    // Java cache plugin.
                    readCachePluginConfiguration(ccfg, in);
                } else {
                    // Platform cache plugin.
                    plugins.add(new PlatformCachePluginConfiguration(in.readObjectDetached()));
                }
            }

            if (ccfg.getPluginConfigurations() != null)
                Collections.addAll(plugins, ccfg.getPluginConfigurations());

            ccfg.setPluginConfigurations(plugins.toArray(new CachePluginConfiguration[plugins.size()]));
        }

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
                throw new IllegalStateException("FairAffinityFunction");
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

        if (f instanceof RendezvousAffinityFunction) {
            out.writeByte((byte) 2);

            RendezvousAffinityFunction f0 = (RendezvousAffinityFunction) f;
            out.writeInt(f0.getPartitions());
            out.writeBoolean(f0.isExcludeNeighbors());
            out.writeByte((byte) 0);  // override flags
            out.writeObject(null);  // user func
        }
        else if (f instanceof PlatformAffinityFunction) {
            PlatformAffinityFunction f0 = (PlatformAffinityFunction) f;
            AffinityFunction baseFunc = f0.getBaseFunc();

            if (baseFunc instanceof RendezvousAffinityFunction) {
                out.writeByte((byte) 2);
                out.writeInt(f0.partitions());
                out.writeBoolean(((RendezvousAffinityFunction) baseFunc).isExcludeNeighbors());
                out.writeByte(f0.getOverrideFlags());
                out.writeObject(f0.getUserFunc());
            }
            else {
                out.writeByte((byte) 3);
                out.writeInt(f0.partitions());
                out.writeBoolean(false);  // exclude neighbors
                out.writeByte(f0.getOverrideFlags());
                out.writeObject(f0.getUserFunc());
            }
        }
        else
            out.writeByte((byte)0);
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
        else
            out.writeByte((byte)0);
    }

    /**
     * Reads the query entity.
     *
     * @param in Stream.
     * @param ver Client version.
     * @return QueryEntity.
     */
    public static QueryEntity readQueryEntity(BinaryRawReader in, ClientListenerProtocolVersion ver) {
        QueryEntity res = new QueryEntity();

        res.setKeyType(in.readString());
        res.setValueType(in.readString());
        res.setTableName(in.readString());
        res.setKeyFieldName(in.readString());
        res.setValueFieldName(in.readString());

        // Fields
        int cnt = in.readInt();
        Set<String> keyFields = new HashSet<>(cnt);
        Set<String> notNullFields = new HashSet<>(cnt);
        Map<String, Object> defVals = new HashMap<>(cnt);
        Map<String, Integer> fieldsPrecision = new HashMap<>(cnt);
        Map<String, Integer> fieldsScale = new HashMap<>(cnt);

        if (cnt > 0) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>(cnt);

            for (int i = 0; i < cnt; i++) {
                String fieldName = in.readString();
                String fieldType = in.readString();

                fields.put(fieldName, fieldType);

                if (in.readBoolean())
                    keyFields.add(fieldName);

                if (in.readBoolean())
                    notNullFields.add(fieldName);

                Object defVal = in.readObject();
                if (defVal != null)
                    defVals.put(fieldName, defVal);
                
                if (ver.compareTo(VER_1_2_0) >= 0) {
                    int precision = in.readInt();

                    if (precision != -1)
                        fieldsPrecision.put(fieldName, precision);

                    int scale = in.readInt();

                    if (scale != -1)
                        fieldsScale.put(fieldName, scale);
                }
            }

            res.setFields(fields);

            if (!keyFields.isEmpty())
                res.setKeyFields(keyFields);

            if (!notNullFields.isEmpty())
                res.setNotNullFields(notNullFields);

            if (!defVals.isEmpty())
                res.setDefaultFieldValues(defVals);

            if (!fieldsPrecision.isEmpty())
                res.setFieldsPrecision(fieldsPrecision);

            if (!fieldsScale.isEmpty())
                res.setFieldsScale(fieldsScale);
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
        res.setInlineSize(in.readInt());

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
     * @param ver Client version.
     */
    @SuppressWarnings("deprecation")
    public static void readIgniteConfiguration(BinaryRawReaderEx in, IgniteConfiguration cfg, 
        ClientListenerProtocolVersion ver) {
        if (in.readBoolean())
            cfg.setClientMode(in.readBoolean());
        int[] evtTypes = in.readIntArray();
        if (evtTypes != null)
            cfg.setIncludeEventTypes(evtTypes);
        if (in.readBoolean())
            cfg.setMetricsExpireTime(in.readLong());
        if (in.readBoolean())
            cfg.setMetricsHistorySize(in.readInt());
        if (in.readBoolean())
            cfg.setMetricsLogFrequency(in.readLong());
        if (in.readBoolean())
            cfg.setMetricsUpdateFrequency(in.readLong());
        if (in.readBoolean())
            cfg.setNetworkSendRetryCount(in.readInt());
        if (in.readBoolean())
            cfg.setNetworkSendRetryDelay(in.readLong());
        if (in.readBoolean())
            cfg.setNetworkTimeout(in.readLong());
        String workDir = in.readString();
        if (workDir != null)
            cfg.setWorkDirectory(workDir);
        String locHost = in.readString();
        if (locHost != null)
            cfg.setLocalHost(locHost);
        if (in.readBoolean())
            cfg.setDaemon(in.readBoolean());
        if (in.readBoolean())
            cfg.setFailureDetectionTimeout(in.readLong());
        if (in.readBoolean())
            cfg.setClientFailureDetectionTimeout(in.readLong());
        if (in.readBoolean())
            cfg.setLongQueryWarningTimeout(in.readLong());
        if (in.readBoolean())
            cfg.setActiveOnStart(in.readBoolean());
        if (in.readBoolean())
            cfg.setAuthenticationEnabled(in.readBoolean());

        int sqlSchemasCnt = in.readInt();

        if (sqlSchemasCnt == -1)
            cfg.setSqlSchemas((String[])null);
        else {
            String[] sqlSchemas = new String[sqlSchemasCnt];

            for (int i = 0; i < sqlSchemasCnt; i++)
                sqlSchemas[i] = in.readString();

            cfg.setSqlSchemas(sqlSchemas);
        }

        Object consId = in.readObjectDetached();

        if (consId instanceof Serializable) {
            cfg.setConsistentId((Serializable) consId);
        } else if (consId != null) {
            throw new IgniteException("IgniteConfiguration.ConsistentId should be Serializable.");
        }

        // Thread pools.
        if (in.readBoolean())
            cfg.setPublicThreadPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setStripedPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setServiceThreadPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setSystemThreadPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setAsyncCallbackPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setManagementThreadPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setDataStreamerThreadPoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setUtilityCachePoolSize(in.readInt());
        if (in.readBoolean())
            cfg.setQueryThreadPoolSize(in.readInt());

        readCacheConfigurations(in, cfg, ver);
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

            if (in.readBoolean()) {
                // Simple name mapper.
                cfg.getBinaryConfiguration().setNameMapper(new BinaryBasicNameMapper(true));
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
            tx.setTxTimeoutOnPartitionMapExchange(in.readLong());

            cfg.setTransactionConfiguration(tx);
        }

        switch (in.readByte()) {
            case 1:
                cfg.setEventStorageSpi(new NoopEventStorageSpi());
                break;

            case 2:
                cfg.setEventStorageSpi(new MemoryEventStorageSpi()
                        .setExpireCount(in.readLong())
                        .setExpireAgeMs(in.readLong()));
                break;
        }

        if (in.readBoolean())
            cfg.setMemoryConfiguration(readMemoryConfiguration(in));

        if (in.readBoolean())
            cfg.setSqlConnectorConfiguration(readSqlConnectorConfiguration(in));

        if (in.readBoolean())
            cfg.setClientConnectorConfiguration(readClientConnectorConfiguration(in));

        if (!in.readBoolean())  // ClientConnectorConfigurationEnabled override
            cfg.setClientConnectorConfiguration(null);

        if (in.readBoolean())
            cfg.setPersistentStoreConfiguration(readPersistentStoreConfiguration(in));

        if (in.readBoolean())
            cfg.setDataStorageConfiguration(readDataStorageConfiguration(in));

        if (in.readBoolean())
            cfg.setSslContextFactory(readSslContextFactory(in));

        if (in.readBoolean()) {
            switch (in.readByte()) {
                case 0:
                    cfg.setFailureHandler(new NoOpFailureHandler());

                    break;

                case 1:
                    cfg.setFailureHandler(new StopNodeFailureHandler());

                    break;

                case 2:
                    cfg.setFailureHandler(new StopNodeOrHaltFailureHandler(in.readBoolean(), in.readLong()));

                    break;
            }
        }

        readPluginConfiguration(cfg, in);

        readLocalEventListeners(cfg, in);
    }

    /**
     * Reads cache configurations from a stream and updates provided IgniteConfiguration.
     *
     * @param cfg IgniteConfiguration to update.
     * @param in Reader.
     * @param ver Client version.
     */
    private static void readCacheConfigurations(BinaryRawReaderEx in, IgniteConfiguration cfg, 
        ClientListenerProtocolVersion ver) {
        int len = in.readInt();

        if (len == 0)
            return;

        List<CacheConfiguration> caches = new ArrayList<>();

        for (int i = 0; i < len; i++)
            caches.add(readCacheConfiguration(in, ver));

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
        boolean hasCfg = in.readBoolean();

        if (!hasCfg)
            return;

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        boolean hasIpFinder = in.readBoolean();

        if (hasIpFinder) {
            byte ipFinderType = in.readByte();

            int addrCnt = in.readInt();

            ArrayList<String> addrs = null;

            if (addrCnt > 0) {
                addrs = new ArrayList<>(addrCnt);

                for (int i = 0; i < addrCnt; i++)
                    addrs.add(in.readString());
            }

            TcpDiscoveryVmIpFinder finder = null;
            if (ipFinderType == 1)
                finder = new TcpDiscoveryVmIpFinder();
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
            else
                assert false;

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
        disco.setStatisticsPrintFrequency(in.readLong());
        disco.setIpFinderCleanFrequency(in.readLong());
        disco.setThreadPriority(in.readInt());
        disco.setTopHistorySize(in.readInt());

        cfg.setDiscoverySpi(disco);
    }

    /**
     * Writes cache configuration.
     *
     * @param writer Writer.
     * @param ccfg Configuration.
     * @param ver Client version.
     */
    public static void writeCacheConfiguration(BinaryRawWriter writer, CacheConfiguration ccfg, 
        ClientListenerProtocolVersion ver) {
        assert writer != null;
        assert ccfg != null;

        writeEnumInt(writer, ccfg.getAtomicityMode(), CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE);
        writer.writeInt(ccfg.getBackups());
        writeEnumInt(writer, ccfg.getCacheMode(), CacheConfiguration.DFLT_CACHE_MODE);
        writer.writeBoolean(ccfg.isCopyOnRead());
        writer.writeBoolean(ccfg.isEagerTtl());
        writer.writeBoolean(ccfg.isInvalidate());
        writer.writeBoolean(ccfg.isStoreKeepBinary());
        writer.writeBoolean(ccfg.isLoadPreviousValue());
        writer.writeLong(ccfg.getDefaultLockTimeout());
        //noinspection deprecation
        writer.writeLong(ccfg.getLongQueryWarningTimeout());
        writer.writeInt(ccfg.getMaxConcurrentAsyncOperations());
        writer.writeString(ccfg.getName());
        writer.writeBoolean(ccfg.isReadFromBackup());
        writer.writeInt(ccfg.getRebalanceBatchSize());
        writer.writeLong(ccfg.getRebalanceDelay());
        writeEnumInt(writer, ccfg.getRebalanceMode(), CacheConfiguration.DFLT_REBALANCE_MODE);
        writer.writeLong(ccfg.getRebalanceThrottle());
        writer.writeLong(ccfg.getRebalanceTimeout());
        writer.writeBoolean(ccfg.isSqlEscapeAll());
        writer.writeInt(ccfg.getWriteBehindBatchSize());
        writer.writeBoolean(ccfg.isWriteBehindEnabled());
        writer.writeLong(ccfg.getWriteBehindFlushFrequency());
        writer.writeInt(ccfg.getWriteBehindFlushSize());
        writer.writeInt(ccfg.getWriteBehindFlushThreadCount());
        writer.writeBoolean(ccfg.getWriteBehindCoalescing());
        writeEnumInt(writer, ccfg.getWriteSynchronizationMode());
        writer.writeBoolean(ccfg.isReadThrough());
        writer.writeBoolean(ccfg.isWriteThrough());
        writer.writeBoolean(ccfg.isStatisticsEnabled());
        //noinspection deprecation
        writer.writeString(ccfg.getMemoryPolicyName());
        writer.writeInt(ccfg.getPartitionLossPolicy().ordinal());
        writer.writeString(ccfg.getGroupName());

        if (ccfg.getCacheStoreFactory() instanceof PlatformDotNetCacheStoreFactoryNative)
            writer.writeObject(((PlatformDotNetCacheStoreFactoryNative)ccfg.getCacheStoreFactory()).getNativeFactory());
        else
            writer.writeObject(null);

        writer.writeInt(ccfg.getSqlIndexMaxInlineSize());
        writer.writeBoolean(ccfg.isOnheapCacheEnabled());
        writer.writeInt(ccfg.getStoreConcurrentLoadAllThreshold());
        writer.writeInt(ccfg.getRebalanceOrder());
        writer.writeLong(ccfg.getRebalanceBatchesPrefetchCount());
        writer.writeInt(ccfg.getMaxQueryIteratorsCount());
        writer.writeInt(ccfg.getQueryDetailMetricsSize());
        writer.writeInt(ccfg.getQueryParallelism());
        writer.writeString(ccfg.getSqlSchema());

        Collection<QueryEntity> qryEntities = ccfg.getQueryEntities();

        if (qryEntities != null) {
            writer.writeInt(qryEntities.size());

            for (QueryEntity e : qryEntities)
                writeQueryEntity(writer, e, ver);
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

        CacheKeyConfiguration[] keys = ccfg.getKeyConfiguration();

        if (keys != null) {
            writer.writeInt(keys.length);

            for (CacheKeyConfiguration key : keys) {
                writer.writeString(key.getTypeName());
                writer.writeString(key.getAffinityKeyFieldName());
            }
        } else {
            writer.writeInt(0);
        }

        CachePluginConfiguration[] plugins = ccfg.getPluginConfigurations();
        if (plugins != null) {
            int cnt = 0;

            for (CachePluginConfiguration cfg : plugins) {
                if (cfg instanceof PlatformCachePluginConfiguration)
                    cnt++;
            }

            writer.writeInt(cnt);

            for (CachePluginConfiguration cfg : plugins) {
                if (cfg instanceof PlatformCachePluginConfiguration) {
                    writer.writeBoolean(false);  // Pure platform plugin.
                    writer.writeObject(((PlatformCachePluginConfiguration) cfg).nativeCfg());
                }
            }
        }
    }

    /**
     * Write query entity.
     *
     * @param writer Writer.
     * @param qryEntity Query entity.
     * @param ver Client version.
     */
    public static void writeQueryEntity(BinaryRawWriter writer, QueryEntity qryEntity, 
        ClientListenerProtocolVersion ver) {
        assert qryEntity != null;

        writer.writeString(qryEntity.getKeyType());
        writer.writeString(qryEntity.getValueType());
        writer.writeString(qryEntity.getTableName());
        writer.writeString(qryEntity.getKeyFieldName());
        writer.writeString(qryEntity.getValueFieldName());

        // Fields
        LinkedHashMap<String, String> fields = qryEntity.getFields();

        if (fields != null) {
            Set<String> keyFields = qryEntity.getKeyFields();
            Set<String> notNullFields = qryEntity.getNotNullFields();
            Map<String, Object> defVals = qryEntity.getDefaultFieldValues();
            Map<String, Integer> fieldsPrecision = qryEntity.getFieldsPrecision();
            Map<String, Integer> fieldsScale = qryEntity.getFieldsScale();

            writer.writeInt(fields.size());

            for (Map.Entry<String, String> field : fields.entrySet()) {
                writer.writeString(field.getKey());
                writer.writeString(field.getValue());
                writer.writeBoolean(keyFields != null && keyFields.contains(field.getKey()));
                writer.writeBoolean(notNullFields != null && notNullFields.contains(field.getKey()));
                writer.writeObject(defVals != null ? defVals.get(field.getKey()) : null);

                if (ver.compareTo(VER_1_2_0) >= 0) {
                    writer.writeInt(fieldsPrecision == null ? -1 : fieldsPrecision.getOrDefault(field.getKey(), -1));
                    writer.writeInt(fieldsScale == null ? -1 : fieldsScale.getOrDefault(field.getKey(), -1));
                }
            }
        }
        else
            writer.writeInt(0);

        // Aliases
        Map<String, String> aliases = qryEntity.getAliases();

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
        Collection<QueryIndex> indexes = qryEntity.getIndexes();

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
     * @param idx Index.
     */
    private static void writeQueryIndex(BinaryRawWriter writer, QueryIndex idx) {
        assert idx != null;

        writer.writeString(idx.getName());
        writeEnumByte(writer, idx.getIndexType());
        writer.writeInt(idx.getInlineSize());

        LinkedHashMap<String, Boolean> fields = idx.getFields();

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
     * @param ver Client version.
     */
    @SuppressWarnings("deprecation")
    public static void writeIgniteConfiguration(BinaryRawWriter w, IgniteConfiguration cfg, 
        ClientListenerProtocolVersion ver) {
        assert w != null;
        assert cfg != null;

        w.writeBoolean(true);
        w.writeBoolean(cfg.isClientMode());
        w.writeIntArray(cfg.getIncludeEventTypes());
        w.writeBoolean(true);
        w.writeLong(cfg.getMetricsExpireTime());
        w.writeBoolean(true);
        w.writeInt(cfg.getMetricsHistorySize());
        w.writeBoolean(true);
        w.writeLong(cfg.getMetricsLogFrequency());
        w.writeBoolean(true);
        w.writeLong(cfg.getMetricsUpdateFrequency());
        w.writeBoolean(true);
        w.writeInt(cfg.getNetworkSendRetryCount());
        w.writeBoolean(true);
        w.writeLong(cfg.getNetworkSendRetryDelay());
        w.writeBoolean(true);
        w.writeLong(cfg.getNetworkTimeout());
        w.writeString(cfg.getWorkDirectory());
        w.writeString(cfg.getLocalHost());
        w.writeBoolean(true);
        w.writeBoolean(cfg.isDaemon());
        w.writeBoolean(true);
        w.writeLong(cfg.getFailureDetectionTimeout());
        w.writeBoolean(true);
        w.writeLong(cfg.getClientFailureDetectionTimeout());
        w.writeBoolean(true);
        w.writeLong(cfg.getLongQueryWarningTimeout());
        w.writeBoolean(true);
        w.writeBoolean(cfg.isActiveOnStart());
        w.writeBoolean(true);
        w.writeBoolean(cfg.isAuthenticationEnabled());

        if (cfg.getSqlSchemas() == null)
            w.writeInt(-1);
        else {
            w.writeInt(cfg.getSqlSchemas().length);

            for (String schema : cfg.getSqlSchemas())
                w.writeString(schema);
        }

        w.writeObject(cfg.getConsistentId());

        // Thread pools.
        w.writeBoolean(true);
        w.writeInt(cfg.getPublicThreadPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getStripedPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getServiceThreadPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getSystemThreadPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getAsyncCallbackPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getManagementThreadPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getDataStreamerThreadPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getUtilityCacheThreadPoolSize());
        w.writeBoolean(true);
        w.writeInt(cfg.getQueryThreadPoolSize());

        CacheConfiguration[] cacheCfg = cfg.getCacheConfiguration();

        if (cacheCfg != null) {
            w.writeInt(cacheCfg.length);

            for (CacheConfiguration ccfg : cacheCfg)
                writeCacheConfiguration(w, ccfg, ver);
        }
        else
            w.writeInt(0);

        writeDiscoveryConfiguration(w, cfg.getDiscoverySpi());

        CommunicationSpi comm = cfg.getCommunicationSpi();

        if (comm instanceof TcpCommunicationSpi) {
            w.writeBoolean(true);
            TcpCommunicationSpi tcp = (TcpCommunicationSpi) comm;

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
            w.writeBoolean(bc.getNameMapper() instanceof BinaryBasicNameMapper &&
                    ((BinaryBasicNameMapper)(bc.getNameMapper())).isSimpleName());
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
            w.writeLong(tx.getTxTimeoutOnPartitionMapExchange());
        }
        else
            w.writeBoolean(false);

        EventStorageSpi evtStorageSpi = cfg.getEventStorageSpi();

        if (evtStorageSpi == null)
            w.writeByte((byte) 0);
        else if (evtStorageSpi instanceof NoopEventStorageSpi)
            w.writeByte((byte) 1);
        else if (evtStorageSpi instanceof MemoryEventStorageSpi) {
            w.writeByte((byte) 2);

            w.writeLong(((MemoryEventStorageSpi)evtStorageSpi).getExpireCount());
            w.writeLong(((MemoryEventStorageSpi)evtStorageSpi).getExpireAgeMs());
        }

        writeMemoryConfiguration(w, cfg.getMemoryConfiguration());

        writeSqlConnectorConfiguration(w, cfg.getSqlConnectorConfiguration());

        writeClientConnectorConfiguration(w, cfg.getClientConnectorConfiguration());

        w.writeBoolean(cfg.getClientConnectorConfiguration() != null);

        writePersistentStoreConfiguration(w, cfg.getPersistentStoreConfiguration());

        writeDataStorageConfiguration(w, cfg.getDataStorageConfiguration());

        writeSslContextFactory(w, cfg.getSslContextFactory());

        FailureHandler failureHnd = cfg.getFailureHandler();

        if (failureHnd instanceof NoOpFailureHandler) {
            w.writeBoolean(true);

            w.writeByte((byte) 0);
        }
        else if (failureHnd instanceof StopNodeFailureHandler) {
            w.writeBoolean(true);

            w.writeByte((byte) 1);
        }
        else if (failureHnd instanceof StopNodeOrHaltFailureHandler) {
            w.writeBoolean(true);

            w.writeByte((byte) 2);

            w.writeBoolean(((StopNodeOrHaltFailureHandler)failureHnd).tryStop());

            w.writeLong(((StopNodeOrHaltFailureHandler)failureHnd).timeout());
        } else
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

            boolean isMcast = finder instanceof TcpDiscoveryMulticastIpFinder;

            w.writeByte((byte)(isMcast ? 2 : 1));

            Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

            w.writeInt(addrs.size());

            for (InetSocketAddress a : addrs)
                w.writeString(a.toString());

            if (isMcast) {
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
        else
            w.writeBoolean(false);

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
        w.writeLong(tcp.getStatisticsPrintFrequency());
        w.writeLong(tcp.getIpFinderCleanFrequency());
        w.writeInt(tcp.getThreadPriority());
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
    public static void writeEnumInt(BinaryRawWriter w, Enum e) {
        w.writeInt(e == null ? 0 : e.ordinal());
    }

    /**
     * Writes enum as int.
     *
     * @param w Writer.
     * @param e Enum.
     */
    public static void writeEnumInt(BinaryRawWriter w, Enum e, Enum def) {
        assert def != null;

        w.writeInt(e == null ? def.ordinal() : e.ordinal());
    }

    /**
     * Reads the plugin configuration.
     *
     * @param cfg Ignite configuration to update.
     * @param in Reader.
     */
    private static void readPluginConfiguration(IgniteConfiguration cfg, BinaryRawReader in) {
        int cnt = in.readInt();

        if (cnt == 0)
            return;

        for (int i = 0; i < cnt; i++) {
            int plugCfgFactoryId = in.readInt();

            PlatformPluginConfigurationClosure plugCfg = pluginConfiguration(plugCfgFactoryId);

            plugCfg.apply(cfg, in);
        }
    }

    /**
     * Create PlatformPluginConfigurationClosure for the given factory ID.
     *
     * @param factoryId Factory ID.
     * @return PlatformPluginConfigurationClosure.
     */
    private static PlatformPluginConfigurationClosure pluginConfiguration(final int factoryId) {
        PlatformPluginConfigurationClosureFactory factory = AccessController.doPrivileged(
                new PrivilegedAction<PlatformPluginConfigurationClosureFactory>() {
                    @Override public PlatformPluginConfigurationClosureFactory run() {
                        for (PlatformPluginConfigurationClosureFactory factory :
                                ServiceLoader.load(PlatformPluginConfigurationClosureFactory.class)) {
                            if (factory.id() == factoryId)
                                return factory;
                        }

                        return null;
                    }
                });

        if (factory == null) {
            throw new IgniteException("PlatformPluginConfigurationClosureFactory is not found " +
                    "(did you put into the classpath?): " + factoryId);
        }

        return factory.create();
    }

    /**
     * Reads the plugin configuration.
     *
     * @param cfg Ignite configuration to update.
     * @param in Reader.
     */
    private static void readCachePluginConfiguration(CacheConfiguration cfg, BinaryRawReader in) {
        int plugCfgFactoryId = in.readInt();

        in.readInt(); // skip size.

        PlatformCachePluginConfigurationClosure plugCfg = cachePluginConfiguration(plugCfgFactoryId);

        plugCfg.apply(cfg, in);
    }

    /**
     * Create PlatformCachePluginConfigurationClosure for the given factory ID.
     *
     * @param factoryId Factory ID.
     * @return PlatformCachePluginConfigurationClosure.
     */
    private static PlatformCachePluginConfigurationClosure cachePluginConfiguration(final int factoryId) {
        PlatformCachePluginConfigurationClosureFactory factory = AccessController.doPrivileged(
                new PrivilegedAction<PlatformCachePluginConfigurationClosureFactory>() {
                    @Override public PlatformCachePluginConfigurationClosureFactory run() {
                        for (PlatformCachePluginConfigurationClosureFactory factory :
                                ServiceLoader.load(PlatformCachePluginConfigurationClosureFactory.class)) {
                            if (factory.id() == factoryId)
                                return factory;
                        }

                        return null;
                    }
                });

        if (factory == null) {
            throw new IgniteException("PlatformPluginConfigurationClosureFactory is not found " +
                    "(did you put into the classpath?): " + factoryId);
        }

        return factory.create();
    }

    /**
     * Reads the memory configuration.
     *
     * @param in Reader
     * @return Config.
     */
    @SuppressWarnings("deprecation")
    private static MemoryConfiguration readMemoryConfiguration(BinaryRawReader in) {
        MemoryConfiguration res = new MemoryConfiguration();

        res.setSystemCacheInitialSize(in.readLong())
                .setSystemCacheMaxSize(in.readLong())
                .setPageSize(in.readInt())
                .setConcurrencyLevel(in.readInt())
                .setDefaultMemoryPolicyName(in.readString());

        int cnt = in.readInt();

        if (cnt > 0) {
            MemoryPolicyConfiguration[] plcs = new MemoryPolicyConfiguration[cnt];

            for (int i = 0; i < cnt; i++) {
                MemoryPolicyConfiguration cfg = new MemoryPolicyConfiguration();

                cfg.setName(in.readString())
                        .setInitialSize(in.readLong())
                        .setMaxSize(in.readLong())
                        .setSwapFilePath(in.readString())
                        .setPageEvictionMode(DataPageEvictionMode.values()[in.readInt()])
                        .setEvictionThreshold(in.readDouble())
                        .setEmptyPagesPoolSize(in.readInt())
                        .setMetricsEnabled(in.readBoolean())
                        .setSubIntervals(in.readInt())
                        .setRateTimeInterval(in.readLong());

                plcs[i] = cfg;
            }

            res.setMemoryPolicies(plcs);
        }

        return res;
    }

    /**
     * Writes the memory configuration.
     *
     * @param w Writer.
     * @param cfg Config.
     */
    @SuppressWarnings("deprecation")
    private static void writeMemoryConfiguration(BinaryRawWriter w, MemoryConfiguration cfg) {
        if (cfg == null) {
            w.writeBoolean(false);
            return;
        }

        w.writeBoolean(true);

        w.writeLong(cfg.getSystemCacheInitialSize());
        w.writeLong(cfg.getSystemCacheMaxSize());
        w.writeInt(cfg.getPageSize());
        w.writeInt(cfg.getConcurrencyLevel());
        w.writeString(cfg.getDefaultMemoryPolicyName());

        MemoryPolicyConfiguration[] plcs = cfg.getMemoryPolicies();

        if (plcs != null) {
            w.writeInt(plcs.length);

            for (MemoryPolicyConfiguration plc : plcs) {
                w.writeString(plc.getName());
                w.writeLong(plc.getInitialSize());
                w.writeLong(plc.getMaxSize());
                w.writeString(plc.getSwapFilePath());
                w.writeInt(plc.getPageEvictionMode().ordinal());
                w.writeDouble(plc.getEvictionThreshold());
                w.writeInt(plc.getEmptyPagesPoolSize());
                w.writeBoolean(plc.isMetricsEnabled());
                w.writeInt(plc.getSubIntervals());
                w.writeLong(plc.getRateTimeInterval());
            }
        }
        else
            w.writeInt(0);
    }

    /**
     * Reads the SQL connector configuration.
     *
     * @param in Reader.
     * @return Config.
     */
    @SuppressWarnings("deprecation")
    private static SqlConnectorConfiguration readSqlConnectorConfiguration(BinaryRawReader in) {
        return new SqlConnectorConfiguration()
                .setHost(in.readString())
                .setPort(in.readInt())
                .setPortRange(in.readInt())
                .setSocketSendBufferSize(in.readInt())
                .setSocketReceiveBufferSize(in.readInt())
                .setTcpNoDelay(in.readBoolean())
                .setMaxOpenCursorsPerConnection(in.readInt())
                .setThreadPoolSize(in.readInt());
    }

    /**
     * Writes the SQL connector configuration.
     *
     * @param w Writer.
     */
    @SuppressWarnings("deprecation")
    private static void writeSqlConnectorConfiguration(BinaryRawWriter w, SqlConnectorConfiguration cfg) {
        assert w != null;

        if (cfg != null) {
            w.writeBoolean(true);

            w.writeString(cfg.getHost());
            w.writeInt(cfg.getPort());
            w.writeInt(cfg.getPortRange());
            w.writeInt(cfg.getSocketSendBufferSize());
            w.writeInt(cfg.getSocketReceiveBufferSize());
            w.writeBoolean(cfg.isTcpNoDelay());
            w.writeInt(cfg.getMaxOpenCursorsPerConnection());
            w.writeInt(cfg.getThreadPoolSize());
        } else
            w.writeBoolean(false);
    }

    /**
     * Reads the client connector configuration.
     *
     * @param in Reader.
     * @return Config.
     */
    private static ClientConnectorConfiguration readClientConnectorConfiguration(BinaryRawReader in) {
        return new ClientConnectorConfiguration()
                .setHost(in.readString())
                .setPort(in.readInt())
                .setPortRange(in.readInt())
                .setSocketSendBufferSize(in.readInt())
                .setSocketReceiveBufferSize(in.readInt())
                .setTcpNoDelay(in.readBoolean())
                .setMaxOpenCursorsPerConnection(in.readInt())
                .setThreadPoolSize(in.readInt())
                .setIdleTimeout(in.readLong())
                .setThinClientEnabled(in.readBoolean())
                .setOdbcEnabled(in.readBoolean())
                .setJdbcEnabled(in.readBoolean());
    }

    /**
     * Writes the client connector configuration.
     *
     * @param w Writer.
     */
    private static void writeClientConnectorConfiguration(BinaryRawWriter w, ClientConnectorConfiguration cfg) {
        assert w != null;

        if (cfg != null) {
            w.writeBoolean(true);

            w.writeString(cfg.getHost());
            w.writeInt(cfg.getPort());
            w.writeInt(cfg.getPortRange());
            w.writeInt(cfg.getSocketSendBufferSize());
            w.writeInt(cfg.getSocketReceiveBufferSize());
            w.writeBoolean(cfg.isTcpNoDelay());
            w.writeInt(cfg.getMaxOpenCursorsPerConnection());
            w.writeInt(cfg.getThreadPoolSize());
            w.writeLong(cfg.getIdleTimeout());

            w.writeBoolean(cfg.isThinClientEnabled());
            w.writeBoolean(cfg.isOdbcEnabled());
            w.writeBoolean(cfg.isJdbcEnabled());
        } else {
            w.writeBoolean(false);
        }
    }

    /**
     * Reads the persistence store connector configuration.
     *
     * @param in Reader.
     * @return Config.
     */
    @SuppressWarnings("deprecation")
    private static PersistentStoreConfiguration readPersistentStoreConfiguration(BinaryRawReader in) {
        return new PersistentStoreConfiguration()
                .setPersistentStorePath(in.readString())
                .setCheckpointingFrequency(in.readLong())
                .setCheckpointingPageBufferSize(in.readLong())
                .setCheckpointingThreads(in.readInt())
                .setLockWaitTime((int) in.readLong())
                .setWalHistorySize(in.readInt())
                .setWalSegments(in.readInt())
                .setWalSegmentSize(in.readInt())
                .setWalStorePath(in.readString())
                .setWalArchivePath(in.readString())
                .setWalMode(WALMode.fromOrdinal(in.readInt()))
                .setWalBufferSize(in.readInt())
                .setWalFlushFrequency((int) in.readLong())
                .setWalFsyncDelayNanos(in.readLong())
                .setWalRecordIteratorBufferSize(in.readInt())
                .setAlwaysWriteFullPages(in.readBoolean())
                .setMetricsEnabled(in.readBoolean())
                .setSubIntervals(in.readInt())
                .setRateTimeInterval(in.readLong())
                .setCheckpointWriteOrder(CheckpointWriteOrder.fromOrdinal(in.readInt()))
                .setWriteThrottlingEnabled(in.readBoolean());
    }

    /**
     * Reads the data storage configuration.
     *
     * @param in Reader.
     * @return Config.
     */
    private static DataStorageConfiguration readDataStorageConfiguration(BinaryRawReader in) {
        DataStorageConfiguration res = new DataStorageConfiguration()
                .setStoragePath(in.readString())
                .setCheckpointFrequency(in.readLong())
                .setCheckpointThreads(in.readInt())
                .setLockWaitTime((int) in.readLong())
                .setWalHistorySize(in.readInt())
                .setWalSegments(in.readInt())
                .setWalSegmentSize(in.readInt())
                .setWalPath(in.readString())
                .setWalArchivePath(in.readString())
                .setWalMode(WALMode.fromOrdinal(in.readInt()))
                .setWalThreadLocalBufferSize(in.readInt())
                .setWalFlushFrequency((int) in.readLong())
                .setWalFsyncDelayNanos(in.readLong())
                .setWalRecordIteratorBufferSize(in.readInt())
                .setAlwaysWriteFullPages(in.readBoolean())
                .setMetricsEnabled(in.readBoolean())
                .setMetricsSubIntervalCount(in.readInt())
                .setMetricsRateTimeInterval(in.readLong())
                .setCheckpointWriteOrder(CheckpointWriteOrder.fromOrdinal(in.readInt()))
                .setWriteThrottlingEnabled(in.readBoolean())
                .setWalCompactionEnabled(in.readBoolean())
                .setSystemRegionInitialSize(in.readLong())
                .setSystemRegionMaxSize(in.readLong())
                .setPageSize(in.readInt())
                .setConcurrencyLevel(in.readInt())
                .setWalAutoArchiveAfterInactivity(in.readLong());

        int cnt = in.readInt();

        if (cnt > 0) {
            DataRegionConfiguration[] regs = new DataRegionConfiguration[cnt];

            for (int i = 0; i < cnt; i++) {
                regs[i] = readDataRegionConfiguration(in);
            }

            res.setDataRegionConfigurations(regs);
        }

        if (in.readBoolean()) {
            res.setDefaultDataRegionConfiguration(readDataRegionConfiguration(in));
        }

        return res;
    }

    /**
     * Reads the SSL context factory.
     *
     * @param in Reader.
     * @return Config.
     */
    private static SslContextFactory readSslContextFactory(BinaryRawReader in) {
        SslContextFactory f = new SslContextFactory();

        f.setKeyAlgorithm(in.readString());

        f.setKeyStoreType(in.readString());
        f.setKeyStoreFilePath(in.readString());
        String pwd = in.readString();
        if (pwd != null)
            f.setKeyStorePassword(pwd.toCharArray());

        f.setProtocol(in.readString());

        f.setTrustStoreType(in.readString());
        String path = in.readString();
        if (path != null)
            f.setTrustStoreFilePath(path);
        else
            f.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        pwd = in.readString();
        if (pwd != null)
            f.setTrustStorePassword(pwd.toCharArray());

        return f;
    }

    /**
     * Writes the persistent store configuration.
     *
     * @param w Writer.
     */
    @SuppressWarnings("deprecation")
    private static void writePersistentStoreConfiguration(BinaryRawWriter w, PersistentStoreConfiguration cfg) {
        assert w != null;

        if (cfg != null) {
            w.writeBoolean(true);

            w.writeString(cfg.getPersistentStorePath());
            w.writeLong(cfg.getCheckpointingFrequency());
            w.writeLong(cfg.getCheckpointingPageBufferSize());
            w.writeInt(cfg.getCheckpointingThreads());
            w.writeLong(cfg.getLockWaitTime());
            w.writeInt(cfg.getWalHistorySize());
            w.writeInt(cfg.getWalSegments());
            w.writeInt(cfg.getWalSegmentSize());
            w.writeString(cfg.getWalStorePath());
            w.writeString(cfg.getWalArchivePath());
            w.writeInt(cfg.getWalMode().ordinal());
            w.writeInt(cfg.getWalBufferSize());
            w.writeLong(cfg.getWalFlushFrequency());
            w.writeLong(cfg.getWalFsyncDelayNanos());
            w.writeInt(cfg.getWalRecordIteratorBufferSize());
            w.writeBoolean(cfg.isAlwaysWriteFullPages());
            w.writeBoolean(cfg.isMetricsEnabled());
            w.writeInt(cfg.getSubIntervals());
            w.writeLong(cfg.getRateTimeInterval());
            w.writeInt(cfg.getCheckpointWriteOrder().ordinal());
            w.writeBoolean(cfg.isWriteThrottlingEnabled());

        } else
            w.writeBoolean(false);
    }

    /**
     * Writes the data storage configuration.
     *
     * @param w Writer.
     */
    private static void writeDataStorageConfiguration(BinaryRawWriter w, DataStorageConfiguration cfg) {
        assert w != null;

        if (cfg != null) {
            w.writeBoolean(true);

            w.writeString(cfg.getStoragePath());
            w.writeLong(cfg.getCheckpointFrequency());
            w.writeInt(cfg.getCheckpointThreads());
            w.writeLong(cfg.getLockWaitTime());
            w.writeInt(cfg.getWalHistorySize());
            w.writeInt(cfg.getWalSegments());
            w.writeInt(cfg.getWalSegmentSize());
            w.writeString(cfg.getWalPath());
            w.writeString(cfg.getWalArchivePath());
            w.writeInt(cfg.getWalMode().ordinal());
            w.writeInt(cfg.getWalThreadLocalBufferSize());
            w.writeLong(cfg.getWalFlushFrequency());
            w.writeLong(cfg.getWalFsyncDelayNanos());
            w.writeInt(cfg.getWalRecordIteratorBufferSize());
            w.writeBoolean(cfg.isAlwaysWriteFullPages());
            w.writeBoolean(cfg.isMetricsEnabled());
            w.writeInt(cfg.getMetricsSubIntervalCount());
            w.writeLong(cfg.getMetricsRateTimeInterval());
            w.writeInt(cfg.getCheckpointWriteOrder().ordinal());
            w.writeBoolean(cfg.isWriteThrottlingEnabled());
            w.writeBoolean(cfg.isWalCompactionEnabled());
            w.writeLong(cfg.getSystemRegionInitialSize());
            w.writeLong(cfg.getSystemRegionMaxSize());
            w.writeInt(cfg.getPageSize());
            w.writeInt(cfg.getConcurrencyLevel());
            w.writeLong(cfg.getWalAutoArchiveAfterInactivity());

            if (cfg.getDataRegionConfigurations() != null) {
                w.writeInt(cfg.getDataRegionConfigurations().length);

                for (DataRegionConfiguration d : cfg.getDataRegionConfigurations()) {
                    writeDataRegionConfiguration(w, d);
                }
            } else {
                w.writeInt(0);
            }

            if (cfg.getDefaultDataRegionConfiguration() != null) {
                w.writeBoolean(true);
                writeDataRegionConfiguration(w, cfg.getDefaultDataRegionConfiguration());
            } else {
                w.writeBoolean(false);
            }
        } else {
            w.writeBoolean(false);
        }
    }

    /**
     * Writes the data region configuration.
     *
     * @param w Writer.
     */
    private static void writeDataRegionConfiguration(BinaryRawWriter w, DataRegionConfiguration cfg) {
        assert w != null;
        assert cfg != null;

        w.writeString(cfg.getName());
        w.writeBoolean(cfg.isPersistenceEnabled());
        w.writeLong(cfg.getInitialSize());
        w.writeLong(cfg.getMaxSize());
        w.writeString(cfg.getSwapPath());
        w.writeInt(cfg.getPageEvictionMode().ordinal());
        w.writeDouble(cfg.getEvictionThreshold());
        w.writeInt(cfg.getEmptyPagesPoolSize());
        w.writeBoolean(cfg.isMetricsEnabled());
        w.writeInt(cfg.getMetricsSubIntervalCount());
        w.writeLong(cfg.getMetricsRateTimeInterval());
        w.writeLong(cfg.getCheckpointPageBufferSize());
    }

    /**
     * Writes the SSL context factory.
     *
     * @param w Writer.
     * @param factory SslContextFactory.
     */
    private static void writeSslContextFactory(BinaryRawWriter w, Factory<SSLContext> factory) {
        assert w != null;

        if (!(factory instanceof SslContextFactory)) {
            w.writeBoolean(false);
            return;
        }

        w.writeBoolean(true);

        SslContextFactory sslCtxFactory = (SslContextFactory)factory;

        w.writeString(sslCtxFactory.getKeyAlgorithm());

        w.writeString(sslCtxFactory.getKeyStoreType());
        w.writeString(sslCtxFactory.getKeyStoreFilePath());
        w.writeString(new String(sslCtxFactory.getKeyStorePassword()));

        w.writeString(sslCtxFactory.getProtocol());

        w.writeString(sslCtxFactory.getTrustStoreType());
        w.writeString(sslCtxFactory.getTrustStoreFilePath());
        w.writeString(new String(sslCtxFactory.getTrustStorePassword()));
    }

    /**
     * Reads the data region configuration.
     *
     * @param r Reader.
     */
    private static DataRegionConfiguration readDataRegionConfiguration(BinaryRawReader r) {
        assert r != null;

        return new DataRegionConfiguration()
                .setName(r.readString())
                .setPersistenceEnabled(r.readBoolean())
                .setInitialSize(r.readLong())
                .setMaxSize(r.readLong())
                .setSwapPath(r.readString())
                .setPageEvictionMode(DataPageEvictionMode.fromOrdinal(r.readInt()))
                .setEvictionThreshold(r.readDouble())
                .setEmptyPagesPoolSize(r.readInt())
                .setMetricsEnabled(r.readBoolean())
                .setMetricsSubIntervalCount(r.readInt())
                .setMetricsRateTimeInterval(r.readLong())
                .setCheckpointPageBufferSize(r.readLong());
    }

    /**
     * Reads the plugin configuration.
     *
     * @param cfg Ignite configuration to update.
     * @param in Reader.
     */
    private static void readLocalEventListeners(IgniteConfiguration cfg, BinaryRawReader in) {
        int cnt = in.readInt();

        if (cnt == 0) {
            return;
        }

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>(cnt);

        for (int i = 0; i < cnt; i++) {
            int[] types = in.readIntArray();

            lsnrs.put(new PlatformLocalEventListener(i), types);
        }

        cfg.setLocalEventListeners(lsnrs);
    }



    /**
     * Private constructor.
     */
    private PlatformConfigurationUtils() {
        // No-op.
    }
}
