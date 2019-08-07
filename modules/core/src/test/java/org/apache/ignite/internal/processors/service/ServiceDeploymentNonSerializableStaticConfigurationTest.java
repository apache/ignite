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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/** */
public class ServiceDeploymentNonSerializableStaticConfigurationTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_SERVICE_NAME = "nonSerializableService";

    /** */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** */
    @Before
    public void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName(TEST_SERVICE_NAME);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new NonSerializableService());

        cfg.setServiceConfiguration(srvcCfg);

        return cfg;
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testNonSerializableStaticServiceValidationFailure() throws Exception {
        LogListener lsnr = LogListener
            .matches(s -> s.startsWith("Failed to marshal service with configured marshaller [name=" + TEST_SERVICE_NAME))
            .atLeast(2)
            .build();

        log.registerListener(lsnr);

        try {
            startGrid(0);

            IgniteEx ignite = startGrid(1);

            assertEquals(2, ignite.context().discovery().topologyVersion());

            assertTrue(lsnr.check());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class NonSerializableService implements Service {
        /** */
        @SuppressWarnings("unused")
        private NonSerializableObject nonSerializableField = new NonSerializableObject();

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }

    /** */
    private static class NonSerializableObject {
    }
}
