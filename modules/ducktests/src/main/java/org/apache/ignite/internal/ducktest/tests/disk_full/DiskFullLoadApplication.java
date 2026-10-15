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

package org.apache.ignite.internal.ducktest.tests.disk_full;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Puts values under growing keys, with a new binary type every 1000 puts, so the partitions, the WAL and the binary
 * metadata keep growing. Retries failed puts and records {@code progress-><puts> <errors><-} once a second.
 */
public class DiskFullLoadApplication extends IgniteAwareApplication {
    /** Value size, in bytes. */
    private static final int VALUE_SIZE = 1024;

    /** Puts per new binary type. */
    private static final int BINARY_TYPE_EVERY = 1000;

    /** Successful puts after which the application reports it is initialized. */
    private static final int WARM_UP_PUTS = 1000;

    /** Pause after a failed put, in milliseconds. */
    private static final long ERROR_PAUSE = 200;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        ClientCache<Long, Object> cache = client.getOrCreateCache(jsonNode.get("cacheName").asText());

        byte[] val = new byte[VALUE_SIZE];

        long key = 0;
        long puts = 0;
        long errs = 0;
        long nextReport = System.currentTimeMillis() + 1000;

        while (active()) {
            try {
                if (key % BINARY_TYPE_EVERY == 0)
                    cache.put(key, client.binary().builder("DiskFullType" + key).setField("val", val).build());
                else
                    cache.put(key, val);

                key++;
                puts++;

                // The first record comes before the marker, so the test can read the counters once start() returns.
                if (puts == WARM_UP_PUTS) {
                    recordResult("progress", puts + " " + errs);

                    markInitialized();
                }
            }
            catch (Exception e) {
                errs++;

                log.warn("Put failed [key=" + key + ", err=" + e + ']');

                Thread.sleep(ERROR_PAUSE);
            }

            long now = System.currentTimeMillis();

            if (now >= nextReport) {
                recordResult("progress", puts + " " + errs);

                nextReport = now + 1000;
            }
        }

        markFinished();
    }
}
