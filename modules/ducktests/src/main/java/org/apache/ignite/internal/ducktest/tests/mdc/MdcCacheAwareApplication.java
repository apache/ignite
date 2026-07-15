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

package org.apache.ignite.internal.ducktest.tests.mdc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.MdcAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.topology.MdcTopologyValidator;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.ducktest.utils.Utils.getEnum;

/**
 * Base class for MDC test applications.
 * Encapsulates the cache configuration (with {@link MdcTopologyValidator} and
 * {@link MdcAffinityBackupFilter}) so that the generator, the checkers and the load
 * applications always operate on an identically configured cache.
 * <p>
 * Supported cache parameters (all optional unless stated otherwise):
 * <ul>
 *     <li>{@code cacheName} - cache name;</li>
 *     <li>{@code backups} - number of backups; {@code (backups + 1)} must be divisible by {@code dcsNum};</li>
 *     <li>{@code mainDc} - main data center for the topology validator (2 DC mode);</li>
 *     <li>{@code datacenters} - full DC set for majority-based validation (odd DC count mode),
 *         takes precedence over {@code mainDc};</li>
 *     <li>{@code dcsNum} - number of data centers, default 2;</li>
 *     <li>{@code cacheMode} - {@link CacheMode}, default {@code PARTITIONED};</li>
 *     <li>{@code atomicity} - {@link CacheAtomicityMode}, default {@code ATOMIC};</li>
 *     <li>{@code writeSync} - {@link CacheWriteSynchronizationMode}, default {@code FULL_SYNC}.
 *         Note: MDC-aware local reads require a mode other than {@code PRIMARY_SYNC};</li>
 *     <li>{@code readFromBackup} - default {@code true}, required for DC-local reads;</li>
 *     <li>{@code partitions} - affinity partitions number, default 512.</li>
 * </ul>
 */
public abstract class MdcCacheAwareApplication extends IgniteAwareApplication {
    /** Table name of the SQL-enabled MDC cache. The SQL schema is the quoted cache name. */
    protected static final String SQL_TABLE = "LOAD";

    /** */
    protected static final String DFLT_CACHE_NAME = "default";

    /** */
    protected static final CacheMode DFLT_CACHE_MODE = PARTITIONED;

    /** One backup: with the default 2 DCs, {@code (backups + 1) / dcsNum} = 1 copy per DC. */
    protected static final int DFLT_BACKUPS = 1;

    /** */
    protected static final int DFLT_DCS_NUM = 2;

    /** */
    protected static final int DFLT_PARTITIONS = 512;

    /** */
    protected static final CacheAtomicityMode DFLT_ATOMICITY_MODE = ATOMIC;

    /** */
    protected static final CacheWriteSynchronizationMode DFLT_WRITE_SYNC = FULL_SYNC;

    /**
     * @param jNode Parameters.
     * @return Cache configured with the MDC topology validator and backup filter.
     */
    protected IgniteCache<Integer, IndexedDataRecord> mdcCache(JsonNode jNode) {
        return ignite.getOrCreateCache(this.<IndexedDataRecord>mdcCacheConfiguration(jNode));
    }

    /**
     * Same MDC cache configuration with a {@link QueryEntity} on top, so the cache is
     * queryable via SQL: {@code SELECT _VAL FROM "<cacheName>".LOAD WHERE _KEY = ?}.
     *
     * @param jNode Parameters.
     * @return SQL-enabled cache configured with the MDC topology validator and backup filter.
     */
    protected IgniteCache<Integer, Integer> mdcSqlCache(JsonNode jNode) {
        CacheConfiguration<Integer, Integer> cacheCfg = mdcCacheConfiguration(jNode);

        cacheCfg.setQueryEntities(Collections.singletonList(
            new QueryEntity(Integer.class, Integer.class).setTableName(SQL_TABLE)));

        return ignite.getOrCreateCache(cacheCfg);
    }

    /**
     * @param jNode Parameters.
     * @return MDC cache configuration compiled from the application parameters.
     */
    protected <V> CacheConfiguration<Integer, V> mdcCacheConfiguration(JsonNode jNode) {
        String cacheName = jNode.path("cacheName").asText(DFLT_CACHE_NAME);
        int backups = jNode.path("backups").asInt(DFLT_BACKUPS);
        int partitions = jNode.path("partitions").asInt(DFLT_PARTITIONS);

        CacheAtomicityMode atomicity = getEnum(jNode, "atomicity", DFLT_ATOMICITY_MODE);
        CacheWriteSynchronizationMode writeSync = getEnum(jNode, "writeSync", DFLT_WRITE_SYNC);
        CacheMode cacheMode = getEnum(jNode, "cacheMode", DFLT_CACHE_MODE);

        boolean readFromBackup = jNode.path("readFromBackup").asBoolean(true);

        int dcsNum = jNode.path("dcsNum").asInt(DFLT_DCS_NUM);

        MdcTopologyValidator topValidator = new MdcTopologyValidator();

        if (jNode.hasNonNull("datacenters")) {
            Set<String> dcs = new HashSet<>();

            jNode.get("datacenters").forEach(dc -> dcs.add(dc.asText()));

            topValidator.setDatacenters(dcs);
        }
        else
            topValidator.setMainDatacenter(jNode.path("mainDc").asText());

        return new CacheConfiguration<Integer, V>()
            .setName(cacheName)
            .setTopologyValidator(topValidator)
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicity)
            .setWriteSynchronizationMode(writeSync)
            .setBackups(backups)
            .setReadFromBackup(readFromBackup)
            .setAffinity(new RendezvousAffinityFunction()
                .setPartitions(partitions)
                .setAffinityBackupFilter(new MdcAffinityBackupFilter(dcsNum, backups)));
    }

    /**
     * @param cacheName Cache name.
     * @return Existing cache. The cache must have been created by the generator beforehand.
     */
    protected IgniteCache<Integer, IndexedDataRecord> existingCache(String cacheName) {
        return ignite.cache(cacheName);
    }

    /**
     * @return Data center id this client belongs to (passed via {@link IgniteSystemProperties#IGNITE_DATA_CENTER_ID}).
     */
    protected static String dcId() {
        return IgniteSystemProperties.getString(IGNITE_DATA_CENTER_ID);
    }
}
