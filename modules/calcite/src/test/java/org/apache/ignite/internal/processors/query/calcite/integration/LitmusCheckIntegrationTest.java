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
import org.junit.Test;

import static org.apache.logging.log4j.Level.DEBUG;

/** Calcite litmus related tests. */
public class LitmusCheckIntegrationTest extends AbstractBasicIntegrationTest {
    /** Logger whose {@link Level#DEBUG} severity enables additional Calcite litmus checks. */
    private static final String PLANNER_LOG_NAME = "org.apache.calcite.plan.RelOptPlanner";

    /** Level of the planner logger before the test. */
    private Level prevLevel;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        prevLevel = LoggerContext.getContext(false).getConfiguration().getLoggerConfig(PLANNER_LOG_NAME).getLevel();

        // Some calcite litmus related checks are enabled only with DEBUG severity logging.
        Configurator.setLevel(PLANNER_LOG_NAME, DEBUG);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Restore the level, otherwise every test executed later in the same JVM plans with the litmus checks enabled.
        Configurator.setLevel(PLANNER_LOG_NAME, prevLevel);

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** Check no calcite litmus exception is raised. */
    @Test
    public void testLitmusLowerCost() {
        sql("create table t11 (c1 int, c2 int, c3 int)");
        sql("create table t22 (c1 int, c2 int, c3 int)");
        sql("create index t11_idx on t11 (c3, c2, c1)");
        sql("create index t22_idx on t22 (c3, c2, c1)");

        assertQuery("SELECT distinct p.c1 FROM t11 cd left join " +
            "t22 p ON p.c2 = cd.c2 WHERE cd.c2 = 1;").resultSize(0).check();
    }
}
