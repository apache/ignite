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

package org.apache.ignite.internal.metric;

import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class RegexpMetricFilterTest extends AbstractExporterSpiTest {
    /** */
    public static final String CONFIG = "modules/spring/src/test/config/metric/regexp-filter-config.xml";

    /** */
    @Test
    public void testFilter() throws Exception {
        try (Ignite ignite = Ignition.start(CONFIG)) {
            ListeningTestLogger log = U.field(ignite.configuration().getGridLogger(), "impl");

            LogListener filtered = LogListener.matches(s -> s.contains(FILTERED_PREFIX)).build();

            log.registerListener(filtered);

            Set<String> expectedMetrics = new GridConcurrentHashSet<>(asList(
                "other.prefix.test = 42",
                "other.prefix.test2 = 43",
                "other.prefix2.test3 = 44"
            ));

            log.registerListener(s -> expectedMetrics.removeIf(s::contains));

            createAdditionalMetrics((IgniteEx)ignite);

            assertTrue(waitForCondition(expectedMetrics::isEmpty, EXPORT_TIMEOUT * 10));

            assertFalse(filtered.check());
        }
    }
}
