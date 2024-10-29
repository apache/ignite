/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import com.google.common.collect.ImmutableList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Non-stable topology tests. */
@RunWith(Parameterized.class)
public class UnstableTopologyTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String POI_CACHE_NAME = "POI_CACHE";

    /** */
    private static final String POI_SCHEMA_NAME = "DOMAIN";

    /** */
    private static final String POI_TABLE_NAME = "POI";

    /** */
    private static final String POI_CLASS_NAME = "PointOfInterest";

    /** */
    private static final String ID_FIELD_NAME = "id";

    /** */
    private static final String NAME_FIELD_NAME = "name";

    /** */
    private static final String LATITUDE_FIELD_NAME = "latitude";

    /** */
    private static final String LONGITUDE_FIELD_NAME = "longitude";

    /** */
    private static final int NUM_ENTITIES = 2_000;

    /** Test parameters. */
    @Parameterized.Parameters(name = "awaitExchange={0}, idxSlowDown={1}")
    public static List<Object[]> parameters() {
        return ImmutableList.of(
            new Object[]{true, true},
            new Object[]{true, false},
            new Object[]{false, false},
            new Object[]{false, true}
        );
    }

    /** */
    @Parameterized.Parameter()
    public boolean awaitExchange;

    /** */
    @Parameterized.Parameter(1)
    public boolean idxSlowDown;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. We don't need to start anything.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (idxSlowDown)
            cfg.setIndexingSpi(new BlockingIndexingSpi());

        cfg.setCacheConfiguration(new CacheConfiguration<>(POI_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setSqlSchema(POI_SCHEMA_NAME)
            .setQueryEntities(Collections.singletonList(queryEntity()))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class));

        cfg.setClusterStateOnStart(ClusterState.ACTIVE);

        return cfg;
    }

    /** */
    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(ID_FIELD_NAME, Integer.class.getName());
        fields.put(NAME_FIELD_NAME, String.class.getName());
        fields.put(LATITUDE_FIELD_NAME, Double.class.getName());
        fields.put(LONGITUDE_FIELD_NAME, Double.class.getName());

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setKeyFieldName(ID_FIELD_NAME)
            .setValueType(POI_CLASS_NAME)
            .setTableName(POI_TABLE_NAME)
            .setFields(fields)
            .setIndexes(Collections.singletonList(
                new QueryIndex(NAME_FIELD_NAME, QueryIndexType.SORTED).setName(NAME_FIELD_NAME + "_idx")
            ));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        super.afterTest();
    }

    /**
     * Checks sql execution correctness on changing (+1 node in parallel with request) topology.
     */
    @Test
    public void testSelectCorrectnessOnUnstableTopology() throws Exception {
        startGrids(2);

        loadData(grid(1), 0, NUM_ENTITIES);

        startGrid(2);

        if (awaitExchange)
            awaitPartitionMapExchange(true, true, null);
        else
            awaitPartitionMapExchange();

        G.allGrids().forEach(g -> assertQuery(g, "SELECT * FROM " + POI_SCHEMA_NAME + '.' + POI_TABLE_NAME)
            .resultSize(NUM_ENTITIES).check());
    }

    /** */
    private void loadData(Ignite node, int start, int end) {
        try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(POI_CACHE_NAME)) {
            Random rnd = ThreadLocalRandom.current();

            for (int i = start; i < end; i++) {
                BinaryObject bo = node.binary().builder(POI_CLASS_NAME)
                    .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
                    .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .build();

                streamer.addData(i, bo);
            }
        }
    }

    /**
     * Simple blocking indexing SPI.
     */
    private static class BlockingIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(
            @Nullable String cacheName,
            Collection<Object> params,
            @Nullable IndexingQueryFilter filters
        ) throws IgniteSpiException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void store(
            @Nullable String cacheName,
            Object key,
            Object val,
            long expirationTime
        ) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
            doSleep(50L);
        }
    }
}
