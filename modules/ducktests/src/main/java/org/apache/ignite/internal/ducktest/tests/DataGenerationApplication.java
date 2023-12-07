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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

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

        int idxCnt = 0;

        if (jsonNode.has("indexCount"))
            idxCnt = jsonNode.get("indexCount").asInt();

        byte[] dataPattern = null;

        if (jsonNode.has("dataPatternBase64"))
            dataPattern = Optional.ofNullable(jsonNode.get("dataPatternBase64").asText(null))
                .map(b64 -> Base64.getDecoder().decode(b64))
                .orElse(null);

        markInitialized();

        for (int i = 1; i <= cacheCnt; i++) {
            CacheConfiguration<Integer, BinaryObject> ccfg = new CacheConfiguration<Integer, BinaryObject>("test-cache-" + i)
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

            IgniteCache<Integer, BinaryObject> cache = ignite.getOrCreateCache(ccfg);

            generateCacheData(cache.getName(), entrySize, from, to, idxCnt, dataPattern);
        }

        markFinished();
    }

    /**
     * @param cacheName Cache name.
     * @param entrySize Entry size.
     * @param from From key.
     * @param to To key.
     * @param dataPattern If not-null pattern is used to fill the entry data field.
     *                    It is filled with random data otherwise.
     */
    private void generateCacheData(String cacheName, int entrySize, int from, int to, int idxCnt, byte[] dataPattern) {
        int flushEach = MAX_STREAMER_DATA_SIZE / entrySize + (MAX_STREAMER_DATA_SIZE % entrySize == 0 ? 0 : 1);
        int logEach = (to - from) / 10;

        BinaryObjectBuilder builder = ignite.binary().builder(VAL_TYPE);

        byte[] data;

        if (dataPattern != null) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream(entrySize);

            int curPos = 0;

            while (curPos < entrySize) {
                int len = Math.min(dataPattern.length, entrySize - curPos);

                buf.write(dataPattern, 0, len);

                curPos += len;
            }

            assert buf.size() == entrySize;

            data = buf.toByteArray();
        }
        else {
            data = new byte[entrySize];
            ThreadLocalRandom.current().nextBytes(data);
        }

        try (IgniteDataStreamer<Integer, BinaryObject> stmr = ignite.dataStreamer(cacheName)) {
            for (int i = from; i < to; i++) {
                builder.setField("key", i);
                builder.setField("data", data);

                for (int j = 0; j < idxCnt; j++) {
                    byte[] indexedBytes = new byte[100];

                    ThreadLocalRandom.current().nextBytes(indexedBytes);

                    builder.setField("bytes" + j, indexedBytes);
                }

                stmr.addData(i, builder.build());

                if ((i - from + 1) % logEach == 0 && log.isDebugEnabled())
                    log.debug("Streamed " + (i - from + 1) + " entries into " + cacheName);

                if (i % flushEach == 0)
                    stmr.flush();
            }
        }

        log.info(cacheName + " data generated [entryCnt=" + (from - to) + ", from=" + from + ", to=" + to + "]");
    }
}
