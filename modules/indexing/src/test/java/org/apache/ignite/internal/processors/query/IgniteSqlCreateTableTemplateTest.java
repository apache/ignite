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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Ensures that SQL queries work for tables created dynamically based on a template.
 */
public class IgniteSqlCreateTableTemplateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setIgniteInstanceName(name);

        CacheConfiguration defaultCacheConfiguration = new CacheConfiguration();

        defaultCacheConfiguration.setName("DEFAULT_TEMPLATE*");

        CacheConfiguration customCacheConfiguration = new CacheConfiguration();

        customCacheConfiguration.setName("CUSTOM_TEMPLATE*");

        MockAffinityKeyMapper customAffinityMapper = new MockAffinityKeyMapper();

        customCacheConfiguration.setAffinityMapper(customAffinityMapper);

        configuration.setCacheConfiguration(defaultCacheConfiguration, customCacheConfiguration);

        return configuration;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /**
     * Tests select statement works on a table with BinaryObject as a primary key.
     */
    @Test
    public void testSelectForTableWithDataInsertedWithKeyValueAPI() {
        Ignite ignite = grid();
        IgniteCache cache = ignite.getOrCreateCache("test");

        createTable(cache, "PERSON", "DEFAULT_TEMPLATE");

        createTable(cache, "ORGANIZATION", "DEFAULT_TEMPLATE");

        BinaryObjectBuilder keyBuilder = ignite.binary().builder("PERSON_KEY");

        keyBuilder.setField("ID", 1);
        keyBuilder.setField("AFF_PERSON", 2);

        BinaryObjectBuilder valueBuilder = ignite.binary().builder("PERSON_VALUE");
        valueBuilder.setField("NAME", "test");

        ignite.cache("PERSON_CACHE").withKeepBinary().put(keyBuilder.build(), valueBuilder.build());

        keyBuilder = ignite.binary().builder("ORGANIZATION_KEY");

        keyBuilder.setField("ID", 1);
        keyBuilder.setField("AFF_ORGANIZATION", 2);

        valueBuilder = ignite.binary().builder("ORGANIZATION_VALUE");

        valueBuilder.setField("NAME", "test");

        ignite.cache("ORGANIZATION_CACHE").withKeepBinary().put(keyBuilder.build(), valueBuilder.build());

        assertEquals(1, ignite.cache("PERSON_CACHE").query(
            new SqlFieldsQuery("Select NAME from PERSON where ID = 1")).getAll().size()
        );

        assertEquals(1, ignite.cache("PERSON_CACHE").query(
            new SqlFieldsQuery("Select NAME from PERSON where AFF_PERSON = 2")).getAll().size()
        );

        assertEquals(1, ignite.cache("ORGANIZATION_CACHE").query(
            new SqlFieldsQuery("Select NAME from ORGANIZATION where AFF_ORGANIZATION = 2")).getAll().size()
        );

    }

    /**
     * Creates table based on a template.
     *
     * @param cache Cache.
     */
    private void createTable(IgniteCache cache, String tableName, String template) {
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %1$s(\n" +
                "  ID                INT        NOT NULL,\n" +
                "  AFF_%1$s        INT        NOT NULL,\n" +
                "  NAME              VARCHAR2(100),\n" +
                "  PRIMARY KEY (ID, AFF_%1$s)\n" +
                ") with \"TEMPLATE=%2$s,KEY_TYPE=%1$s_KEY, AFFINITY_KEY=AFF_%1$s, CACHE_NAME=%1$s_CACHE, " +
                    "VALUE_TYPE=%1$s_VALUE, ATOMICITY=TRANSACTIONAL\";", tableName, template);

        cache.query(new SqlFieldsQuery(sql).setSchema("PUBLIC"));
    }

    /**
     * When template has custom affinity mapper.
     * then cache created via CREATE TABLE command should have the same affinity mapper.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCustomAffinityKeyMapperIsNotOverwritten() {
        Ignite ignite = grid();

        IgniteCache cache = ignite.getOrCreateCache("test");

        createTable(cache, "CUSTOM", "CUSTOM_TEMPLATE");

        assertTrue(ignite.getOrCreateCache("CUSTOM_CACHE").getConfiguration(
            CacheConfiguration.class).getAffinityMapper() instanceof MockAffinityKeyMapper);
    }

    /**
     * Mock affinity mapper implementation.
     */
    @SuppressWarnings("deprecation")
    private static class MockAffinityKeyMapper implements AffinityKeyMapper {
        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // no-op
        }
    }
}