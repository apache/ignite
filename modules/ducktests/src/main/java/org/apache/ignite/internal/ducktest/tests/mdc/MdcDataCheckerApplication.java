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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;

/**
 * Verifies that all entries written by {@link MdcDataGeneratorApplication} are readable
 * and hold expected values. Intended to be run from a client in each DC after the
 * network partition: reads must succeed everywhere, even in the read-only DC.
 */
public class MdcDataCheckerApplication extends MdcCacheAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        String cacheName = jNode.get("cacheName").asText();
        int from = jNode.path("from").asInt(0);
        int to = jNode.path("to").asInt(10_000);

        markInitialized();
        waitForActivation();

        IgniteCache<Integer, IndexedDataRecord> cache = mdcCache(cacheName);

        log.info("Data check started [dc=" + dcId() + ", cache=" + cache.getName() +
            ", from=" + from + ", to=" + to + "]");

        int missed = 0;
        int corrupted = 0;

        for (int i = from; i < to && !terminated(); i++) {
            IndexedDataRecord obj = cache.get(i);

            if (obj == null) {
                missed++;

                log.error("Entry is missed [dc=" + dcId() + ", key=" + i + "]");
            }
            else if (!obj.equals(new IndexedDataRecord(i))) {
                corrupted++;

                log.error("Entry is corrupted [dc=" + dcId() + ", key=" + i + ", val=" + obj + "]");
            }
        }

        if (missed > 0 || corrupted > 0) {
            throw new IllegalStateException("Data check failed [dc=" + dcId() +
                ", missed=" + missed + ", corrupted=" + corrupted + "]");
        }

        log.info("Data check passed [dc=" + dcId() + ", entries=" + (to - from) + "]");

        markFinished();
    }
}
