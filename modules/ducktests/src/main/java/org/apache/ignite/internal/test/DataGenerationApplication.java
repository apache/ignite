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

package org.apache.ignite.internal.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.test.utils.IgniteAwareApplication;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /**
     * @param ignite Ignite.
     */
    public DataGenerationApplication(Ignite ignite) {
        super(ignite);
    }

    /** {@inheritDoc} */
    @Override protected void run(String[] args) {
        log.info("Creating cache...");

        IgniteCache<Integer, Integer> cache = ignite.createCache(args[0]);

        try (IgniteDataStreamer<Integer, Integer> stmr = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < Integer.parseInt(args[1]); i++) {
                stmr.addData(i, i);

                if (i % 10_000 == 0)
                    log.info("Streamed " + i + " entries");
            }
        }

        markSyncExecutionComplete();
    }
}
