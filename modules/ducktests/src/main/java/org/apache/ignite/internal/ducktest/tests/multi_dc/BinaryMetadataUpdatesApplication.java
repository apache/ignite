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

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Application continuously writes to cache to update binary metadata and trigger discovery messages within cluster.
 */
public class BinaryMetadataUpdatesApplication extends IgniteAwareApplication {
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
        String cacheName = jsonNode.get("cacheName").asText();

        long pacing = Optional.ofNullable(jsonNode.get("pacing"))
            .map(JsonNode::asLong)
            .orElse(0L);

        log.info("Test props: cacheName={} pacing={}ms", cacheName, pacing);

        log.info("Node name: {} starting cache operations.", ignite.name());

        markInitialized();

        IgniteCache<Object, Object> cache = ignite.cache(cacheName).withKeepBinary();

        long idx = 0;

        while (!terminated() || idx < Long.MAX_VALUE) {
            Object val = getBinaryObject(idx);

            cache.put(idx, val);

            idx++;

            Thread.sleep(pacing);
        }

        for (long i = 0; i < idx; i++)
            assert cache.get(i) != null : "Expected value is absent [key=" + i + ", putCnt=" + idx + "]";

        log.info("Finished cache operations [node={}, putCnt={}]", ignite.name(), idx);

        recordResult("putCnt", idx);

        markFinished();
    }

    /** */
    private BinaryObject getBinaryObject(long idx) {
        Object val = getRandomObject();

        BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

        long entryVer = idx + 1;
        String entryNewFieldName = "field" + entryVer;

        builder.setField(ENTRY_VERSION_FIELD_NAME, entryVer);
        builder.setField(entryNewFieldName, val);

        return builder.build();
    }

    /** */
    private static Object getRandomObject() {
        int fieldTypeIdx = ThreadLocalRandom.current().nextInt(5);

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
}
