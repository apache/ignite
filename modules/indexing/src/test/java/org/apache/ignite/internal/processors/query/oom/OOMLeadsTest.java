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

package org.apache.ignite.internal.processors.query.oom;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Out of memory handling. */
public class OOMLeadsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList("-Xmx64m", "-Xms64m");
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        IgniteProcessProxy.killAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** Check correct handling for Out of memory. */
    @Test
    public void testOOMQueryHandling() throws Exception {
        startGrids(2);

        // stop local jvm node
        stopGrid(0);

        // check non oom sql processed correctly
        runQuery("select x, space(100+x) as av from system_range(1, 1) group by av");

        // oom lead sql
        assertThrows(null, () ->
                runQuery("select x, space(10000000+x) as av from system_range(1, 1000) group by av"),
                IgniteException.class, "Out of memory");

        IgniteEx grd = grid(1);

        assertTrue(GridTestUtils.waitForCondition(() -> !((IgniteProcessProxy)grd).getProcess().getProcess().isAlive(), 10_000));
    }

    /** */
    private void runQuery(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(
                "jdbc:ignite:thin://127.0.0.1:10800..10850/")) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
    }
}
