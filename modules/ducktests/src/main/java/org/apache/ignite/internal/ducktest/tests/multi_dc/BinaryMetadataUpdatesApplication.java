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

package org.apache.ignite.internal.ducktest.tests.multi_dc;

import java.util.concurrent.ThreadLocalRandom;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Application continuously writes to cache with fixed rps to update binary metadata and
 * trigger discovery messages within cluster.
 */
public class BinaryMetadataUpdatesApplication extends IgniteAwareApplication {
    /** */
    private static final String TEST_CACHE_NAME = "replicated-cache";

    /** */
    private static final int RANDOM_INTEGER_BOUND = 1_000;

    /** */
    private static final int RANDOM_BYTE_ARRAY_SIZE = 3;

    /** */
    private static final String ENTRY_VERSION_FIELD_NAME = "version";

    /** */
    private static final String BINARY_TYPE_NAME = "TestBinaryType";

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        markInitialized();

        int putCnt = loadUntilTerminated();

        checkData(putCnt);

        markFinished();
    }

    /** */
    private int loadUntilTerminated() {
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(getCacheConfiguration());
        cache = cache.withKeepBinary();

        int key = 0;

        long startNs = System.nanoTime();

        while (!terminated()) {
            Object val = getBinaryObject(key);

            cache.put(key, val);

            key++;
        }

        long endNs = System.nanoTime();

        double durationSec = (endNs - startNs) / 1_000_000_000.0;
        double rps = key / durationSec;

        log.info("Data load is finished [putCnt={}, rps={}]", key, rps);

        recordResult("putCnt", key);
        recordResult("rps", (long)rps);

        return key;
    }

    /** */
    private void checkData(int putCnt) {
        IgniteCache<Object, Object> cache = ignite.cache(TEST_CACHE_NAME).withKeepBinary();

        assert cache.size() == putCnt : "Unexpected cache size [putCnt=" + putCnt + ", size=" + cache.size() + "]";

        for (int i = 0; i < putCnt; i++)
            assert cache.get(i) != null : "Expected value is absent [key=" + i + "]";

        log.info("Data check is successful");
    }

    /** */
    private BinaryObject getBinaryObject(int idx) {
        Object val = getRandomObject();

        BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

        int entryVer = idx + 1;
        String entryNewFieldName = "field" + entryVer;

        builder.setField(ENTRY_VERSION_FIELD_NAME, entryVer);
        builder.setField(entryNewFieldName, val);

        return builder.build();
    }

    /** */
    private static Object getRandomObject() {
        int fieldTypeIdx = ThreadLocalRandom.current().nextInt(4);

        switch (fieldTypeIdx % 4) {
            case 0:
                return getRandomInteger();
            case 1:
                return getRandomString();
            case 2:
                return getRandomByteArray();
            case 3:
                return new Object();
        }

        throw new RuntimeException("Error generating random object");
    }

    /** */
    private static String getRandomString() {
        return "str-" + getRandomInteger();
    }

    /** */
    private static int getRandomInteger() {
        return ThreadLocalRandom.current().nextInt(RANDOM_INTEGER_BOUND);
    }

    /** */
    private static byte[] getRandomByteArray() {
        byte[] res = new byte[RANDOM_BYTE_ARRAY_SIZE];
        ThreadLocalRandom.current().nextBytes(res);
        return res;
    }

    /** */
    private static CacheConfiguration<Object, Object> getCacheConfiguration() {
        return new CacheConfiguration<>(TEST_CACHE_NAME).setCacheMode(REPLICATED);
    }
}
