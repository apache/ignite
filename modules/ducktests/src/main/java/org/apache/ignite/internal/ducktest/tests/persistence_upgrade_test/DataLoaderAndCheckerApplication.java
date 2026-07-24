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

package org.apache.ignite.internal.ducktest.tests.persistence_upgrade_test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Loads/checks the data.
 */
public class DataLoaderAndCheckerApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        boolean check = jNode.get("check").asBoolean();
        int backups = jNode.path("backups").asInt(0);
        int entryCnt = jNode.path("entryCount").asInt(10_000);

        markInitialized();
        waitForActivation();

        CacheConfiguration<Integer, IndexedDataRecord> cacheCfg = new CacheConfiguration<>("cache");

        cacheCfg.setBackups(backups);

        IgniteCache<Integer, IndexedDataRecord> cache = ignite.getOrCreateCache(cacheCfg);

        log.info(check ? "Checking..." : " Preparing...");

        for (int i = 0; i < entryCnt; i++) {
            IndexedDataRecord obj = new IndexedDataRecord(i);

            if (!check)
                cache.put(i, obj);
            else
                assert cache.get(i).equals(obj);
        }

        log.info(check ? "Checked." : " Prepared.");

        while (!terminated())
            U.sleep(100); // Keeping node alive.

        markFinished();
    }
}
