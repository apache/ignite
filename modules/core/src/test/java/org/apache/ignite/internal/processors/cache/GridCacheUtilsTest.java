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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/** Tests for {@link GridCacheUtils}. */
public class GridCacheUtilsTest extends GridCommonAbstractTest {
    /** Verifies that patching a cache configuration does not modify query entities of the original configuration. */
    @Test
    public void testPatchCacheConfigurationDoesNotModifyOriginalQueryEntities() {
        CacheConfiguration<Object, Object> oldCfg = new CacheConfiguration<>("TEST_CACHE");

        oldCfg.setQueryEntities(emptyList());

        Collection<QueryEntity> oldEntities = oldCfg.getQueryEntities();

        assertTrue(oldEntities.isEmpty());
        assertTrue(oldEntities instanceof ArrayList);

        QueryEntity newEntity = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName());

        CacheConfiguration<Object, Object> patchedCfg =
            GridCacheUtils.patchCacheConfiguration(
                oldCfg,
                singletonList(newEntity),
                "PUBLIC",
                false,
                1
            );

        assertTrue(oldCfg.getQueryEntities().isEmpty());

        assertEquals(1, patchedCfg.getQueryEntities().size());
        assertSame(newEntity, patchedCfg.getQueryEntities().iterator().next());

        assertNotSame(oldEntities, patchedCfg.getQueryEntities());
    }
}
