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

package org.apache.ignite.internal.processors.cache;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * A test for {@link QueryEntity} initialization with incorrect query field name
 */
public class IncorrectQueryEntityTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        QueryEntity queryEntity = new QueryEntity(Object.class.getName(), Object.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("exceptionOid", Object.class.getName());

        queryEntity.setFields(fields);

        Set<String> keyFields = new HashSet<>();

        keyFields.add("exceptionOid");

        queryEntity.setKeyFields(keyFields);

        dfltCacheCfg.setQueryEntities(F.asList(queryEntity));

        cfg.setCacheConfiguration(dfltCacheCfg);

        return cfg;
    }

    /**
     * Grid must be stopped with property initialization exception.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectQueryField() throws Exception {
        try {
            startGrid();
        }
        catch (Exception exception) {
            if (!exception.getMessage().contains(
                QueryUtils.propertyInitializationExceptionMessage(
                    Object.class, Object.class, "exceptionOid", Object.class)))
                fail("property initialization exception must be thrown, but got " + exception.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
