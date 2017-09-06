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

package org.apache.ignite.examples.streaming.wordcount;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityUuid;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;

/**
 * Periodically query popular numbers from the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup}.</li>
 *     <li>Start streaming using {@link StreamWords}.</li>
 *     <li>Start querying popular words using {@link QueryWords}.</li>
 * </ul>
 */
public class QueryWords {
    /**
     * Schedules words query execution.
     *
     * @param args Command line arguments (none required).
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            CacheConfiguration<AffinityUuid, String> cfg = CacheConfig.wordCache();

            // The cache is configured with sliding window holding 1 second of the streaming data.
            try (IgniteCache<AffinityUuid, String> stmCache = ignite.getOrCreateCache(cfg)) {
                // Select top 10 words.
                SqlFieldsQuery top10Qry = new SqlFieldsQuery(
                    "select _val, count(_val) as cnt from String group by _val order by cnt desc limit 10",
                    true /*collocated*/
                );

                // Select average, min, and max counts among all the words.
                SqlFieldsQuery statsQry = new SqlFieldsQuery(
                    "select avg(cnt), min(cnt), max(cnt) from (select count(_val) as cnt from String group by _val)");

                // Query top 10 popular numbers every 5 seconds.
                while (true) {
                    // Execute queries.
                    List<List<?>> top10 = stmCache.query(top10Qry).getAll();
                    List<List<?>> stats = stmCache.query(statsQry).getAll();

                    // Print average count.
                    List<?> row = stats.get(0);

                    if (row.get(0) != null)
                        System.out.printf("Query results [avg=%d, min=%d, max=%d]%n",
                            row.get(0), row.get(1), row.get(2));

                    // Print top 10 words.
                    ExamplesUtils.printQueryResults(top10);

                    Thread.sleep(5000);
                }
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(cfg.getName());
            }
        }
    }
}