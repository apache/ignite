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

package org.apache.ignite.examples.streaming.marketdata;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.examples.*;

import java.util.*;

/**
 * Periodically query popular numbers from the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start streaming using {@link StreamMarketData}.</li>
 *     <li>Start querying top performing instruments using {@link QueryTopInstruments}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class QueryTopInstruments {
    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            IgniteCache<String, Instrument> instCache = ignite.getOrCreateCache(CacheConfig.instrumentCache());

            // Select top 3 instruments.
            SqlFieldsQuery top3qry = new SqlFieldsQuery(
                "select symbol, (latest - open) from Instrument order by (latest - open) desc limit 3");

            // Select total profit.
            SqlFieldsQuery profitQry = new SqlFieldsQuery("select sum(latest - open) from Instrument");

            // Query top 3 best performing instruments every 5 seconds.
            while (true) {
                // Execute queries.
                List<List<?>> top3 = instCache.query(top3qry).getAll();
                List<List<?>> profit = instCache.query(profitQry).getAll();

                List<?> row = profit.get(0);

                if (row.get(0) != null)
                    System.out.printf("Total profit: %.2f%n", row.get(0));

                // Print top 10 words.
                ExamplesUtils.printQueryResults(top3);

                Thread.sleep(5000);
            }
        }
    }
}
