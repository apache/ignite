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

package org.apache.ignite.internal.systemview;

import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.systemview.JmxSystemViewExporterSpi;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.managers.systemview.SystemViewMBean.VIEWS;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_TBL_COLS_VIEW;

/**
 * Tests {@link JmxSystemViewExporterSpi}.
 */
public class JmxExporterSpiTest extends GridCommonAbstractTest {
    /** @throws Exception If failed. */
    @Test
    public void testTableColumns() throws Exception {
        String tableName = "TEST";

        IgniteEx ignite = startGrid(getConfiguration().setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setQueryEntities(F.asList(
                    new QueryEntity()
                        .setTableName(tableName)
                        .setKeyFieldName("ID")
                        .setValueType(Integer.class.getName())
                        .addQueryField("ID", Integer.class.getName(), null)))));

        Map<String, String> expTypes = new HashMap<>();

        expTypes.put("_KEY", Integer.class.getName());
        expTypes.put("_VAL", Integer.class.getName());
        expTypes.put("ID", Integer.class.getName());

        TabularDataSupport columns =
            (TabularDataSupport)metricRegistry(ignite.name(), VIEWS, SQL_TBL_COLS_VIEW).getAttribute(VIEWS);

        columns.values().stream().map(data -> (CompositeData)data)
            .filter(data -> tableName.equals(data.get("tableName")))
            .forEach(data -> {
                String columnName = (String)data.get("columnName");

                assertTrue("Unexpected column: " + columnName, expTypes.containsKey(columnName));
                assertEquals(expTypes.remove(columnName), data.get("type"));
            });

        assertTrue("Expected columns: " + expTypes.keySet(), expTypes.isEmpty());
    }
}
