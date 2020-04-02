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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;

import static org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils.readQueryEntity;
import static org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils.writeEnumInt;
import static org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils.writeQueryEntity;

/**
 * Cache configuration serializer.
 */
public class ClientCacheConfigurationSerializer {
    /** Name. */
    private static final short NAME = 0;

    /** Common properties. */
    private static final short CACHE_MODE = 1;

    /** */
    private static final short ATOMICITY_MODE = 2;

    /** */
    private static final short BACKUPS = 3;

    /** */
    private static final short WRITE_SYNCHRONIZATION_MODE = 4;

    /** */
    private static final short COPY_ON_READ = 5;

    /** */
    private static final short READ_FROM_BACKUP = 6;

    /** Memory settings. */
    private static final short DATA_REGION_NAME = 100;

    /** */
    private static final short ONHEAP_CACHE_ENABLED = 101;

    /** SQL. */
    private static final short QUERY_ENTITIES = 200;

    /** */
    private static final short QUERY_PARALLELISM = 201;

    /** */
    private static final short QUERY_DETAIL_METRICS_SIZE = 202;

    /** */
    private static final short SQL_SCHEMA = 203;

    /** */
    private static final short SQL_INDEX_MAX_INLINE_SIZE = 204;

    /** */
    private static final short SQL_ESCAPE_ALL = 205;

    /** */
    private static final short MAX_QUERY_ITERATORS_COUNT = 206;

    /** Rebalance. */
    private static final short REBALANCE_MODE = 300;

    /** */
    private static final short REBALANCE_DELAY = 301;

    /** */
    private static final short REBALANCE_TIMEOUT = 302;

    /** */
    private static final short REBALANCE_BATCH_SIZE = 303;

    /** */
    private static final short REBALANCE_BATCHES_PREFETCH_COUNT = 304;

    /** */
    private static final short REBALANCE_ORDER = 305;

    /** */
    private static final short REBALANCE_THROTTLE = 306;

    /** Advanced. */
    private static final short GROUP_NAME = 400;

    /** */
    private static final short KEY_CONFIGURATION = 401;

    /** */
    private static final short DEFAULT_LOCK_TIMEOUT = 402;

    /** */
    private static final short MAX_CONCURRENT_ASYNC_OPERATIONS = 403;

    /** */
    private static final short PARTITION_LOSS_POLICY = 404;

    /** */
    private static final short EAGER_TTL = 405;

    /** */
    private static final short STATISTICS_ENABLED = 406;

    /** */
    private static final short EXPIRY_POLICY = 407;

    /**
     * Writes the cache configuration.
     * @param writer Writer.
     * @param cfg Configuration.
     * @param protocolContext Client protocol context.
     */
    static void write(BinaryRawWriterEx writer, CacheConfiguration cfg, ClientProtocolContext protocolContext) {
        assert writer != null;
        assert cfg != null;

        // Reserve for length.
        int pos = writer.reserveInt();

        writeEnumInt(writer, cfg.getAtomicityMode(), CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE);
        writer.writeInt(cfg.getBackups());
        writeEnumInt(writer, cfg.getCacheMode(), CacheConfiguration.DFLT_CACHE_MODE);
        writer.writeBoolean(cfg.isCopyOnRead());
        writer.writeString(cfg.getDataRegionName());
        writer.writeBoolean(cfg.isEagerTtl());
        writer.writeBoolean(cfg.isStatisticsEnabled());
        writer.writeString(cfg.getGroupName());
        writer.writeLong(cfg.getDefaultLockTimeout());
        writer.writeInt(cfg.getMaxConcurrentAsyncOperations());
        writer.writeInt(cfg.getMaxQueryIteratorsCount());
        writer.writeString(cfg.getName());
        writer.writeBoolean(cfg.isOnheapCacheEnabled());
        writer.writeInt(cfg.getPartitionLossPolicy().ordinal());
        writer.writeInt(cfg.getQueryDetailMetricsSize());
        writer.writeInt(cfg.getQueryParallelism());
        writer.writeBoolean(cfg.isReadFromBackup());
        writer.writeInt(cfg.getRebalanceBatchSize());
        writer.writeLong(cfg.getRebalanceBatchesPrefetchCount());
        writer.writeLong(cfg.getRebalanceDelay());
        writeEnumInt(writer, cfg.getRebalanceMode(), CacheConfiguration.DFLT_REBALANCE_MODE);
        writer.writeInt(cfg.getRebalanceOrder());
        writer.writeLong(cfg.getRebalanceThrottle());
        writer.writeLong(cfg.getRebalanceTimeout());
        writer.writeBoolean(cfg.isSqlEscapeAll());
        writer.writeInt(cfg.getSqlIndexMaxInlineSize());
        writer.writeString(cfg.getSqlSchema());
        writeEnumInt(writer, cfg.getWriteSynchronizationMode());

        CacheKeyConfiguration[] keys = cfg.getKeyConfiguration();

        if (keys != null) {
            writer.writeInt(keys.length);

            for (CacheKeyConfiguration key : keys) {
                writer.writeString(key.getTypeName());
                writer.writeString(key.getAffinityKeyFieldName());
            }
        } else {
            writer.writeInt(0);
        }

        //noinspection unchecked
        Collection<QueryEntity> qryEntities = cfg.getQueryEntities();

        if (qryEntities != null) {
            writer.writeInt(qryEntities.size());

            for (QueryEntity e : qryEntities)
                writeQueryEntity(writer, e, protocolContext);
        } else
            writer.writeInt(0);

        if (protocolContext.isExpirationPolicySupported())
            PlatformConfigurationUtils.writeExpiryPolicyFactory(writer, cfg.getExpiryPolicyFactory());

        // Write length (so that part of the config can be skipped).
        writer.writeInt(pos, writer.out().position() - pos - 4);
    }

    /**
     * Reads the cache configuration.
     *
     * @param reader Reader.
     * @param protocolContext Client protocol context.
     * @return Configuration.
     */
    static CacheConfiguration read(BinaryRawReader reader, ClientProtocolContext protocolContext) {
        reader.readInt();  // Skip length.

        short propCnt = reader.readShort();

        CacheConfiguration cfg = new CacheConfiguration();

        for (int i = 0; i < propCnt; i++) {
            short code = reader.readShort();

            switch (code) {
                case ATOMICITY_MODE:
                    cfg.setAtomicityMode(CacheAtomicityMode.fromOrdinal(reader.readInt()));
                    break;

                case BACKUPS:
                    cfg.setBackups(reader.readInt());
                    break;

                case CACHE_MODE:
                    cfg.setCacheMode(CacheMode.fromOrdinal(reader.readInt()));
                    break;

                case COPY_ON_READ:
                    cfg.setCopyOnRead(reader.readBoolean());
                    break;

                case DATA_REGION_NAME:
                    cfg.setDataRegionName(reader.readString());
                    break;

                case EAGER_TTL:
                    cfg.setEagerTtl(reader.readBoolean());
                    break;

                case EXPIRY_POLICY:
                    cfg.setExpiryPolicyFactory(PlatformConfigurationUtils.readExpiryPolicyFactory(reader));
                    break;

                case STATISTICS_ENABLED:
                    cfg.setStatisticsEnabled(reader.readBoolean());
                    break;

                case GROUP_NAME:
                    cfg.setGroupName(reader.readString());
                    break;

                case DEFAULT_LOCK_TIMEOUT:
                    cfg.setDefaultLockTimeout(reader.readLong());
                    break;

                case MAX_CONCURRENT_ASYNC_OPERATIONS:
                    cfg.setMaxConcurrentAsyncOperations(reader.readInt());
                    break;

                case MAX_QUERY_ITERATORS_COUNT:
                    cfg.setMaxQueryIteratorsCount(reader.readInt());
                    break;

                case NAME:
                    cfg.setName(reader.readString());
                    break;

                case ONHEAP_CACHE_ENABLED:
                    cfg.setOnheapCacheEnabled(reader.readBoolean());
                    break;

                case PARTITION_LOSS_POLICY:
                    cfg.setPartitionLossPolicy(PartitionLossPolicy.fromOrdinal((byte) reader.readInt()));
                    break;

                case QUERY_DETAIL_METRICS_SIZE:
                    cfg.setQueryDetailMetricsSize(reader.readInt());
                    break;

                case QUERY_PARALLELISM:
                    cfg.setQueryParallelism(reader.readInt());
                    break;

                case READ_FROM_BACKUP:
                    cfg.setReadFromBackup(reader.readBoolean());
                    break;

                case REBALANCE_BATCH_SIZE:
                    cfg.setRebalanceBatchSize(reader.readInt());
                    break;

                case REBALANCE_BATCHES_PREFETCH_COUNT:
                    cfg.setRebalanceBatchesPrefetchCount(reader.readLong());
                    break;

                case REBALANCE_DELAY:
                    cfg.setRebalanceDelay(reader.readLong());
                    break;

                case REBALANCE_MODE:
                    cfg.setRebalanceMode(CacheRebalanceMode.fromOrdinal(reader.readInt()));
                    break;

                case REBALANCE_ORDER:
                    cfg.setRebalanceOrder(reader.readInt());
                    break;

                case REBALANCE_THROTTLE:
                    cfg.setRebalanceThrottle(reader.readLong());
                    break;

                case REBALANCE_TIMEOUT:
                    cfg.setRebalanceTimeout(reader.readLong());
                    break;

                case SQL_ESCAPE_ALL:
                    cfg.setSqlEscapeAll(reader.readBoolean());
                    break;

                case SQL_INDEX_MAX_INLINE_SIZE:
                    cfg.setSqlIndexMaxInlineSize(reader.readInt());
                    break;

                case SQL_SCHEMA:
                    cfg.setSqlSchema(reader.readString());
                    break;

                case WRITE_SYNCHRONIZATION_MODE:
                    cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.fromOrdinal(reader.readInt()));
                    break;

                case KEY_CONFIGURATION:
                    int keyCnt = reader.readInt();

                    if (keyCnt > 0) {
                        CacheKeyConfiguration[] keys = new CacheKeyConfiguration[keyCnt];

                        for (int j = 0; j < keyCnt; j++) {
                            keys[j] = new CacheKeyConfiguration(reader.readString(), reader.readString());
                        }

                        cfg.setKeyConfiguration(keys);
                    }
                    break;

                case QUERY_ENTITIES:
                    int qryEntCnt = reader.readInt();

                    if (qryEntCnt > 0) {
                        Collection<QueryEntity> entities = new ArrayList<>(qryEntCnt);

                        for (int j = 0; j < qryEntCnt; j++)
                            entities.add(readQueryEntity(reader, protocolContext));

                        cfg.setQueryEntities(entities);
                    }
                    break;
            }
        }

        return cfg;
    }
}
