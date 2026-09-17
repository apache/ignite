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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Tests handling of duplicate key and value classes configured through {@link CacheConfiguration#setIndexedTypes}. */
@SuppressWarnings("unchecked")
public class DuplicateKeyValueClassesSelfTest extends AbstractIndexingCommonTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(CACHE_NAME);
    }

    /** Checks that the same key class can be used with different value classes. */
    @Test
    public void testDuplicateKeyClass() {
        CacheConfiguration ccfg = new CacheConfiguration()
            .setName(CACHE_NAME)
            .setIndexedTypes(UUID.class, Clazz1.class, UUID.class, Clazz2.class);

        grid(0).createCache(ccfg);

        Collection<QueryEntity> entities = grid(0).context().cache().cacheConfiguration(CACHE_NAME).getQueryEntities();

        assertEquals(2, entities.size());

        Set<String> valTypes = new HashSet<>();

        for (QueryEntity entity : entities) {
            assertEquals(UUID.class.getName(), entity.getKeyType());

            valTypes.add(entity.getValueType());
        }

        Set<String> expValTypes = new HashSet<>(Arrays.asList(Clazz1.class.getName(), Clazz2.class.getName()));

        assertEquals(expValTypes, valTypes);
    }

    /**
     * Checks that conflicting key types configured for the same value class are rejected instead of silently
     * discarding one of the query entity configurations.
     */
    @Test
    public void testConflictingKeyTypesForSameValueClass() {
        CacheConfiguration ccfg = new CacheConfiguration()
            .setName(CACHE_NAME);

        String msg = String.format("Failed to merge query entities due to conflicting metadata " +
            "[cacheName=%s, property=keyType, existingValue=%s, incomingValue=%s]",
            CACHE_NAME, UUID.class.getName(), String.class.getName());

        assertThrows(
            log,
            () -> ccfg.setIndexedTypes(UUID.class, Clazz1.class, String.class, Clazz1.class),
            CacheException.class,
            msg
        );
    }

    /**
     * Class 1.
     */
    private static class Clazz1 {
        /** ID. */
        @QuerySqlField(index = true)
        int id;
    }

    /**
     * Class 2.
     */
    private static class Clazz2 {
        /** ID. */
        @QuerySqlField(index = true)
        int id;
    }
}
