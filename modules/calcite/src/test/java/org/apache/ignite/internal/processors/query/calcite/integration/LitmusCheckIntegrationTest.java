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
package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;

import static org.apache.logging.log4j.Level.DEBUG;

/** Calcite litmus related tests. */
public class LitmusCheckIntegrationTest extends AbstractBasicIntegrationTest {
/*    *//** {@inheritDoc} *//*
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    *//** {@inheritDoc} *//*
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }*/

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        setCalciteLoggerDebugLevel();
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    @Test
    public void testLitmusLowerCost() {
        sql("create table t11 (c1 int, c2 int, c3 int)");
        sql("create table t22 (c1 int, c2 int, c3 int)");
        sql("create index t11_idx on t11 (c3, c2, c1)");
        sql("create index t22_idx on t22 (c3, c2, c1)");

        assertQuery("SELECT distinct p.c1 FROM t11 cd left join " +
            "t22 p ON p.c2 = cd.c2 WHERE cd.c2 = 1;").resultSize(0).check();
    }

    /**
     * Sets the log level for logger ({@link #log}) to {@link Level#DEBUG}. The log level will be resetted to
     * default in {@link #afterTest()}.
     */
    protected final void setCalciteLoggerDebugLevel() {
        String logName = "org.apache.calcite.plan.RelOptPlanner";

        Configurator.setLevel(logName, DEBUG);
    }
}
