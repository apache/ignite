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

package org.apache.ignite.internal.processors.rest;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.query.VisorQueryTask;
import org.apache.ignite.internal.visor.query.VisorQueryTaskArg;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.configuration.WALMode.NONE;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test REST with enabled authentication.
 */
public class JettyRestPropagationSecurityContextTest extends JettyRestProcessorCommonSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        super.beforeTestsStarted();

        // We need to activate cluster.
        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
    }

    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(NONE);

        cfg.setDataStorageConfiguration(dsCfg)
           .setAuthenticationEnabled(true)
            .setPluginProviders(new TestSecurityPluginProvider("server", "server", ALLOW_ALL, false,
                new TestSecurityData("client", "client", SecurityPermissionSetBuilder.create()
                    .defaultAllowAll(false)
                    .appendTaskPermissions("org.apache.ignite.internal.visor.query.VisorQueryTask", TASK_EXECUTE)
                    .appendTaskPermissions("org.apache.ignite.internal.visor.compute.VisorGatewayTask", TASK_EXECUTE)
                    .build()
                )));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void restClientShouldPropagateSecurityContext() throws Exception {
        IgniteEx node1 = grid(0);

        TestSecurityProcessor secPrc = U.field(node1.context().security(), "secPrc");
        List<TestSecurityProcessor.AuthorizeRecord> records = secPrc.getAuthorizeRecords();
        records.clear();

        Map<String, String> pie = new JettyRestProcessorAbstractSelfTest.VisorGatewayArgument(VisorQueryTask.class)
            .forNode(node1.localNode())
            .argument(VisorQueryTaskArg.class, "pie", URLEncoder.encode("select * from pie", StandardCharsets.UTF_8.name()),
                false, false, false, false, 1);

        pie.put("user", "client");
        pie.put("password", "client");

        content(pie);

        for (TestSecurityProcessor.AuthorizeRecord record : records)
            assertEquals("client", record.getLogin());
    }
}
