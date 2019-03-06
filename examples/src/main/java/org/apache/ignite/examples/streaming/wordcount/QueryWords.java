/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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