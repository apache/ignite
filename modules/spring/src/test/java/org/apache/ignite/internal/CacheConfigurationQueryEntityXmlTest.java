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

package org.apache.ignite.internal;

import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.beans.PropertyAccessException;
import org.springframework.beans.PropertyBatchUpdateException;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Tests that Spring XML configuration cannot mix indexed types and explicit query entities. */
public class CacheConfigurationQueryEntityXmlTest extends GridCommonAbstractTest {
    /** */
    private static final String CONFIG_DIR = "modules/spring/src/test/config/query-entities/";

    /** */
    private static final String MIXED_QUERY_ENTITIES_API_ERROR =
        "Query entities can be configured either with setIndexedTypes or setQueryEntities, " +
            "but not both [cacheName=query-entity-xml-cache]";

    /** Verifies that query entities cannot be configured after indexed types through Spring XML. */
    @Test
    public void testQueryEntitiesAfterIndexedTypesFails() {
        assertMixedConfigurationFails(
            "indexed-types-then-query-entities.xml",
            "queryEntities"
        );
    }

    /** Verifies that indexed types cannot be configured after query entities through Spring XML. */
    @Test
    public void testIndexedTypesAfterQueryEntitiesFails() {
        assertMixedConfigurationFails(
            "query-entities-then-indexed-types.xml",
            "indexedTypes"
        );
    }

    /** */
    private void assertMixedConfigurationFails(String fileName, String propName) {
        Throwable err = assertThrows(
            log,
            () -> Ignition.loadSpringBean(CONFIG_DIR + fileName, "cacheConfiguration"),
            IgniteException.class,
            null
        );

        PropertyBatchUpdateException batchErr = X.cause(err, PropertyBatchUpdateException.class);
        assertNotNull(batchErr);

        assertEquals(1, batchErr.getExceptionCount());

        PropertyAccessException propErr = batchErr.getPropertyAccessException(propName);
        assertNotNull(propErr);

        CacheException cause = X.cause(propErr, CacheException.class);
        assertNotNull(cause);

        assertEquals(MIXED_QUERY_ENTITIES_API_ERROR, cause.getMessage());
    }
}
