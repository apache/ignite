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

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.transactions.Transaction;

import java.util.function.Function;

/**
 * Application generates cache data by specified parameters.
 */
public class ContinuousDataGenerationApplication extends DataGenerationApplication {
    /** {@inheritDoc} */
    @Override protected void generateCacheData(
        int cacheCnt,
        Function<Integer, BinaryObject> entryBld,
        int entrySize,
        int from,
        int to
    ) {
        for (int i = from; i < to; i++) {
            if (terminated())
                break;

            for (int c = 1; c <= cacheCnt; c++) {
                try (Transaction tx = ignite.transactions().txStart()) {
                    ignite.cache(cache(c)).put(i, entryBld.apply(i));

                    tx.commit();
                }
            }

            if ((i * cacheCnt) % 5_000 == 0)
                log.info("Run " + (i * cacheCnt) + " transactions.");
        }
    }
}
