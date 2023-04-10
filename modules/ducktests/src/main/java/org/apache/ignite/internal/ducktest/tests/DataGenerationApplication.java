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
import java.util.function.Function;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.transactions.Transaction;

/**
 * Application generates cache data by specified parameters.
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** Max streamer data size. */
    private static final int MAX_STREAMER_DATA_SIZE = 100_000_000;

    /** */
    public static final String VAL_TYPE = "org.apache.ignite.ducktest.DataBinary";

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int backups = jsonNode.get("backups").asInt();
        int cacheCnt = jsonNode.get("cacheCount").asInt();
        int entrySize = jsonNode.get("entrySize").asInt();
        int from = jsonNode.get("from").asInt();
        int to = jsonNode.get("to").asInt();

        int idxCnt = jsonNode.has("indexCount") ? jsonNode.get("indexCount").asInt() : 0;
        boolean transactional = jsonNode.has("transactional") && jsonNode.get("transactional").asBoolean();

        createCaches(cacheCnt, transactional, backups, idxCnt);

        markInitialized();

        Function<Integer, BinaryObject> entryBuilder = key -> entry(ignite.binary().builder(VAL_TYPE), key, entrySize, idxCnt);

        generateCacheData(cacheCnt, entryBuilder, entrySize, from, to);

        markFinished();
    }

    /**
     * Create test caches.
     *
     * @param cacheCnt Caches count.
     * @param transactional Create transactional caches or atomic.
     * @param backups Backups count.
     * @param idxCnt Indexes count.
     */
    protected void createCaches(int cacheCnt, boolean transactional, int backups, int idxCnt) {
        for (int i = 1; i <= cacheCnt; i++) {
            CacheConfiguration<Integer, BinaryObject> ccfg = new CacheConfiguration<Integer, BinaryObject>(cache(i))
                .setAtomicityMode(transactional ? CacheAtomicityMode.TRANSACTIONAL : CacheAtomicityMode.ATOMIC)
                .setBackups(backups);

            if (idxCnt > 0) {
                QueryEntity qe = new QueryEntity();
                List<QueryIndex> qi = new ArrayList<>(idxCnt);

                qe.setKeyType(Integer.class.getName());
                qe.setValueType(VAL_TYPE);

                for (int j = 0; j < idxCnt; j++) {
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

    /**
     * @param cacheCnt Cache count.
     * @param entrySize Entry size.
     * @param from From key.
     * @param to To key.
     */
    protected void generateCacheData(int cacheCnt, Function<Integer, BinaryObject> entryBld, int entrySize, int from, int to) {
        int flushEach = MAX_STREAMER_DATA_SIZE / entrySize + (MAX_STREAMER_DATA_SIZE % entrySize == 0 ? 0 : 1);
        int logEach = (to - from) / 10;

        for (int c = 1; c <= cacheCnt; c++) {
            String cacheName = cache(c);

            try (IgniteDataStreamer<Integer, BinaryObject> stmr = ignite.dataStreamer(cacheName)) {
                for (int i = from; i < to; i++) {
                    stmr.addData(i, entryBld.apply(i));

                    if ((i - from + 1) % logEach == 0 && log.isDebugEnabled())
                        log.debug("Streamed " + (i - from + 1) + " entries into " + cacheName);

                    if (i % flushEach == 0)
                        stmr.flush();
                }
            }

            log.info(cacheName + " data generated [entryCnt=" + (from - to) + ", from=" + from + ", to=" + to + "]");
        }
    }

    /**
     * @param cacheIdx Cache index.
     * @return Cache name.
     */
    protected String cache(int cacheIdx) {
        return "test-cache-" + cacheIdx;
    }

    /**
     * @param cache Cache name.
     * @param key Entry key.
     * @param entryBuilder Entry builder.
     */
    private void runTx(String cache, int key, Function<Integer, BinaryObject> entryBuilder) {
        try (Transaction tx = ignite.transactions().txStart()) {
            ignite.cache(cache).put(key, entryBuilder.apply(key));

            tx.commit();
        }
    }

    /**
     * @param builder Entry builder.
     * @param key Entry key.
     * @param entrySize Entry size.
     * @param idxCnt Entry indexed values.
     * @return Build entry.
     */
    private BinaryObject entry(BinaryObjectBuilder builder, int key, int entrySize, int idxCnt) {
        byte[] data = new byte[entrySize];

        ThreadLocalRandom.current().nextBytes(data);

        builder.setField("key", key);
        builder.setField("data", data);

        for (int j = 0; j < idxCnt; j++) {
            byte[] indexedBytes = new byte[100];

            ThreadLocalRandom.current().nextBytes(indexedBytes);

            builder.setField("bytes" + j, indexedBytes);
        }

        return builder.build();
    }
}
