/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.tests.persistence;

import java.util.Collection;
import org.apache.ignite.compatibility.framework.persistence.IgnitePdsCompatibilityAbstractTest;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLatestNodeScenario;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLegacyNodeScenario;
import org.apache.ignite.compatibility.persistence.scenarios.latest.LatestNodeTtlMigrationScenarioImpl;
import org.apache.ignite.compatibility.persistence.scenarios.legacy.LegacyNodeTtlMigrationScenarioImpl;
import org.junit.runners.Parameterized;

/**
 *
 */
public class PdsCompatibilityWithTtlMigrationTest extends IgnitePdsCompatibilityAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Duration sec. */
    private static final int DURATION_SEC = 10;

    /** Entries count. */
    private static final int ENTRIES_COUNT = 100;

    /**
     * TTL migration to per-partition basis works only with version 2.2.0.
     * For versions greater than 2.4 this test doesn't make sense.
     *
     * @return Test data.
     */
    @Parameterized.Parameters(name = "{0} vs {1}")
    public static Collection<Object[]> testData() {
        return testExactVersions("2.2.0");
    }

    /** {@inheritDoc} */
    @Override protected PdsCompatibilityLegacyNodeScenario legacyScenario() {
        return new LegacyNodeTtlMigrationScenarioImpl(CACHE_NAME, DURATION_SEC, ENTRIES_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected PdsCompatibilityLatestNodeScenario latestScenario() {
        return new LatestNodeTtlMigrationScenarioImpl(CACHE_NAME, DURATION_SEC, ENTRIES_COUNT);
    }
}
