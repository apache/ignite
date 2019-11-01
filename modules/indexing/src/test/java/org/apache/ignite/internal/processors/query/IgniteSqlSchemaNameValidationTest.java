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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;

/** Verifies sql schema name validation. */
public class IgniteSqlSchemaNameValidationTest extends AbstractIndexingCommonTest {
    /** */
    private static final List<String> INVALID_SCHEMA_NAMES = Arrays.asList(
        GridTestUtils.randomString(new Random(), 10_000_000, 10_000_000),
        "",
        "\t",
        "\n"
    );

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    public void testCreateCacheWithIllegalSchemaName() throws Exception {
        System.out.println("https://ggsystems.atlassian.net/browse/GG-25421");

        if (true)
            return;

        IgniteEx ignite = startGrid(0);

        for (String name : INVALID_SCHEMA_NAMES) {
            GridTestUtils.assertThrowsWithCause(
                () -> ignite.createCache(new CacheConfiguration<>("test_cache").setSqlSchema(name)),
                IllegalArgumentException.class
            );
        }
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    public void testStartNodeWithIllegalSchemaNameCfg() {
        System.out.println("https://ggsystems.atlassian.net/browse/GG-25421");

        if (true)
            return;

        for (String name : INVALID_SCHEMA_NAMES) {
            GridTestUtils.assertThrowsWithCause(
                () -> startGrid(getConfiguration().setSqlSchemas(name)),
                IllegalArgumentException.class
            );
        }
    }
}
