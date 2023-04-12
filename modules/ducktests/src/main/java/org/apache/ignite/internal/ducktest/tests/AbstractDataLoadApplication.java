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

package org.apache.ignite.internal.ducktest.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/** Basic appliction for creating test caches and loading test data. */
public abstract class AbstractDataLoadApplication extends IgniteAwareApplication {
    /** */
    private static final String VAL_TYPE = "org.apache.ignite.ducktest.DataBinary";

    /** Basic application config. */
    private Config cfg;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        parseConfig(jsonNode);

        createCaches();

        markInitialized();

        loadData();

        markFinished();
    }

    /**
     * Parses json into application config.
     *
     * @param jsonNode Json received along with the application start command.
     */
    protected void parseConfig(JsonNode jsonNode) {
        cfg = parseConfig(jsonNode, Config.class);
    }

    /** Loads data. */
    protected abstract void loadData();

    /**
     * @param key Cache key.
     * @return Cache entry value.
     */
    protected final BinaryObject cacheEntryValue(int key) {
        BinaryObjectBuilder builder = ignite.binary().builder(VAL_TYPE);

        byte[] data = new byte[cfg.entrySize];

        ThreadLocalRandom.current().nextBytes(data);

        builder.setField("key", key);
        builder.setField("data", data);

        for (int j = 0; j < cfg.indexCount; j++) {
            byte[] indexedBytes = new byte[100];

            ThreadLocalRandom.current().nextBytes(indexedBytes);

            builder.setField("bytes" + j, indexedBytes);
        }

        return builder.build();
    }

    /** @return Test cache name. */
    protected final String cacheName(int idx) {
        return "test-cache-" + idx;
    }

    /**
     * Converts Json-represented config into generic config.
     *
     * @param json Json to parse.
     * @param cfgCls Config class to build.
     * @param <C> Config generic.
     * @return Config.
     */
    protected static <C> C parseConfig(JsonNode json, Class<C> cfgCls) {
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        C cfg;

        try {
            cfg = objMapper.treeToValue(json, cfgCls);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to parse config.", e);
        }

        return cfg;
    }

    /** Creates test caches. */
    private void createCaches() {
        for (int i = 1; i <= cfg.cacheCount; i++) {
            CacheConfiguration<Integer, BinaryObject> ccfg = new CacheConfiguration<Integer, BinaryObject>()
                .setName(cacheName(i))
                .setAtomicityMode(cfg.transactional ? CacheAtomicityMode.TRANSACTIONAL : CacheAtomicityMode.ATOMIC)
                .setBackups(cfg.backups);

            if (cfg.indexCount > 0) {
                QueryEntity qe = new QueryEntity();
                List<QueryIndex> qi = new ArrayList<>(cfg.indexCount);

                qe.setKeyType(Integer.class.getName());
                qe.setValueType(VAL_TYPE);

                for (int j = 0; j < cfg.indexCount; j++) {
                    String field = "bytes" + j;

                    qe.addQueryField(field, byte[].class.getName(), null);
                    qi.add(new QueryIndex(field));
                }

                qe.setIndexes(qi);

                ccfg.setQueryEntities(Collections.singleton(qe));
            }

            ignite.getOrCreateCache(ccfg);
        }
    }

    /** The base configuration. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Config {
        /** Count of test caches to create. */
        private int cacheCount;

        /** Count of backups in test caches. */
        private int backups;

        /** If {@code true} then test caches are transactional, otherwise atomic. */
        private boolean transactional;

        /** Count of indexes in test caches. */
        private int indexCount;

        /** Test cache entry size. */
        private int entrySize;
    }
}
