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

package org.apache.ignite.gatling.action.cache;

import io.gatling.app.Gatling;
import io.gatling.core.config.GatlingPropertiesBuilder;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.junit.Test;

import java.util.Properties;

public class ThinClientTest extends AbstractThinClientTest {

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
    }

    protected void run(String simulationClass) {
        Properties sysProperties = System.getProperties();
        sysProperties.setProperty("host", clientHost(grid(0).cluster().localNode()));
        sysProperties.setProperty("port", String.valueOf(clientPort(grid(0).cluster().localNode())));

        GatlingPropertiesBuilder gatlingPropertiesBuilder = new GatlingPropertiesBuilder();
        gatlingPropertiesBuilder.simulationClass(simulationClass);
        gatlingPropertiesBuilder.noReports();

        Gatling.fromMap(gatlingPropertiesBuilder.build());
    }
    @Test
    public void getPutTest() {
        run("org.apache.ignite.gatling.action.cache.GetSimulation");
    }

    @Test
    public void sqlTest() {
        run("org.apache.ignite.gatling.action.cache.SqlSimulation");
    }
}

