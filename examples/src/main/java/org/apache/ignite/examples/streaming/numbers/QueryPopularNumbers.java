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

package org.apache.ignite.examples.streaming.numbers;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.examples.*;

import java.util.*;

/**
 * Periodically query popular numbers from the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start streaming using {@link StreamRandomNumbers}.</li>
 *     <li>Start querying popular numbers using {@link QueryPopularNumbers}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class QueryPopularNumbers {
    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            // The cache is configured with sliding window holding 1 second of the streaming data.
            IgniteCache<Integer, Long> stmCache = ignite.getOrCreateCache(CacheConfig.randomNumbersCache());

            // Select top 10 words.
            SqlFieldsQuery top10Qry = new SqlFieldsQuery("select _key, _val from Long order by _val desc limit 10");

            // Select average, min, and max counts among all the words.
            SqlFieldsQuery statsQry = new SqlFieldsQuery("select avg(_val), min(_val), max(_val) from Long");

            // Query top 10 popular numbers every 5 seconds.
            while (true) {
                // Execute queries.
                List<List<?>> top10 = stmCache.query(top10Qry).getAll();
                List<List<?>> stats = stmCache.query(statsQry).getAll();

                // Print average count.
                List<?> row = stats.get(0);

                if (row.get(0) != null)
                    System.out.printf("Query results [avg=%.2f, min=%d, max=%d]%n", row.get(0), row.get(1), row.get(2));

                // Print top 10 words.
                ExamplesUtils.printQueryResults(top10);

                Thread.sleep(5000);
            }
        }
    }
}
