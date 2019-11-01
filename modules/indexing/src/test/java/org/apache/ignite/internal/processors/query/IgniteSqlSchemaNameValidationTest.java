/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

/** Verifies sql schema name validation. */
public class IgniteSqlSchemaNameValidationTest extends AbstractIndexingCommonTest {
    /** */
    private static final List<String> INVALID_SCHEMA_NAMES = Arrays.asList(
        GridTestUtils.randomString(new Random(), 10_000_000, 10_000_000),
        "",
        "\t",
        "\n"
    );

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-25421")
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testCreateCacheWithIllegalSchemaName() throws Exception {
        IgniteEx ignite = startGrid(0);

        for (String name : INVALID_SCHEMA_NAMES) {
            GridTestUtils.assertThrowsWithCause(
                () -> ignite.createCache(new CacheConfiguration<>("test_cache").setSqlSchema(name)),
                IllegalArgumentException.class
            );
        }
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-25421")
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testStartNodeWithIllegalSchemaNameCfg() {
        for (String name : INVALID_SCHEMA_NAMES) {
            GridTestUtils.assertThrowsWithCause(
                () -> startGrid(getConfiguration().setSqlSchemas(name)),
                IllegalArgumentException.class
            );
        }
    }
}
