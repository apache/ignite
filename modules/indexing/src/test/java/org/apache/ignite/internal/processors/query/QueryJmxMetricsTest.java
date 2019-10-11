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

import javax.management.ObjectName;
import java.util.Set;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.junit.Test;

/**
 * Tests for local query execution in lazy mode.
 */
public class QueryJmxMetricsTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new JmxMetricExporterSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();

        fillGrid();
    }

    /**
     * Initial fill the the grid with caches, data and queries.
     */
    private void fillGrid() {
        grid().createCache(new CacheConfiguration<>()
            .setName("test0")
            .setGroupName("grp0"));

        grid().createCache(new CacheConfiguration<>()
            .setName("test1"));

        grid().context().query().querySqlFields(new SqlFieldsQuery("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL VARCHAR)"), false);

        for (int i = 0; i < 10; ++i) {
            grid().cache("test0").put(i, "val" + i);
            grid().cache("test1").put("key" + i, i);

            grid().context().query().querySqlFields(
                new SqlFieldsQuery("INSERT INTO  TEST VALUES (?, ?)").setArgs(i, "val" + i), false);
        }

        grid().context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM TEST"), false).getAll();

        grid().context().query().querySqlFields(
            new SqlFieldsQuery("CREATE INDEX IDX0 ON TEST(VAL)"), false).getAll();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void testJmxAllMBeanInfo() throws Exception {
        Set<ObjectName> names = grid().configuration().getMBeanServer().queryNames(new ObjectName("*:*"), null);

        log.info("Available beans: " + names.size());

        boolean errors = false;

        for (ObjectName name : names) {
            try {
                grid().configuration().getMBeanServer().getMBeanInfo(name);
            }
            catch (Exception e) {
                log.error("Error on: " + name.getCanonicalName(), e);

                errors = true;
            }
        }

        assertFalse("There are errors at the MBeanInfo creation, see log above", errors);
    }
}
