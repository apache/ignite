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
import org.apache.ignite.binary.BinaryRawWriter;
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

import static org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils.readQueryEntity;
import static org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils.writeEnumInt;
import static org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils.writeQueryEntity;

/**
 * Cache configuration serializer.
 */
public class ClientCacheConfigurationSerializer {
    /**
     * Writes the cache configuration.
     * @param writer Writer.
     * @param cfg Configuration.
     */
    static void write(BinaryRawWriterEx writer, CacheConfiguration cfg) {
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
        writer.writeBoolean(cfg.isInvalidate());
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
                writeQueryEntity(writer, e);
        } else
            writer.writeInt(0);

        // Write length (so that part of the config can be skipped).
        writer.writeInt(pos, writer.out().position() - pos);
    }

    /**
     * Reads the cache configuration.
     *
     * @param reader Reader.
     * @return Configuration.
     */
    static CacheConfiguration read(BinaryRawReader reader) {
        reader.readInt();  // Skip length.

        CacheConfiguration cfg = new CacheConfiguration()
                .setAtomicityMode(CacheAtomicityMode.fromOrdinal(reader.readInt()))
                .setBackups(reader.readInt())
                .setCacheMode(CacheMode.fromOrdinal(reader.readInt()))
                .setCopyOnRead(reader.readBoolean())
                .setDataRegionName(reader.readString())
                .setEagerTtl(reader.readBoolean())
                .setStatisticsEnabled(reader.readBoolean())
                .setGroupName(reader.readString())
                .setInvalidate(reader.readBoolean())
                .setDefaultLockTimeout(reader.readLong())
                .setMaxConcurrentAsyncOperations(reader.readInt())
                .setMaxQueryIteratorsCount(reader.readInt())
                .setName(reader.readString())
                .setOnheapCacheEnabled(reader.readBoolean())
                .setPartitionLossPolicy(PartitionLossPolicy.fromOrdinal((byte)reader.readInt()))
                .setQueryDetailMetricsSize(reader.readInt())
                .setQueryParallelism(reader.readInt())
                .setReadFromBackup(reader.readBoolean())
                .setRebalanceBatchSize(reader.readInt())
                .setRebalanceBatchesPrefetchCount(reader.readLong())
                .setRebalanceDelay(reader.readLong())
                .setRebalanceMode(CacheRebalanceMode.fromOrdinal(reader.readInt()))
                .setRebalanceOrder(reader.readInt())
                .setRebalanceThrottle(reader.readLong())
                .setRebalanceTimeout(reader.readLong())
                .setSqlEscapeAll(reader.readBoolean())
                .setSqlIndexMaxInlineSize(reader.readInt())
                .setSqlSchema(reader.readString())
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.fromOrdinal(reader.readInt()));

        // Key configuration.
        int keyCnt = reader.readInt();

        if (keyCnt > 0) {
            CacheKeyConfiguration[] keys = new CacheKeyConfiguration[keyCnt];

            for (int i = 0; i < keyCnt; i++) {
                keys[i] = new CacheKeyConfiguration(reader.readString(), reader.readString());
            }

            cfg.setKeyConfiguration(keys);
        }

        // Query entities.
        int qryEntCnt = reader.readInt();

        if (qryEntCnt > 0) {
            Collection<QueryEntity> entities = new ArrayList<>(qryEntCnt);

            for (int i = 0; i < qryEntCnt; i++)
                entities.add(readQueryEntity(reader));

            cfg.setQueryEntities(entities);
        }

        return cfg;
    }
}
