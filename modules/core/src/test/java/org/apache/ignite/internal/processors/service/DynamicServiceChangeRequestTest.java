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

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests of {@link DynamicServiceChangeRequest}.
 */
public class DynamicServiceChangeRequestTest {
    /** */
    private static final String TEST_SERVICE_NAME = "testServiceName";

    /** */
    @Test
    public void deploymentRequest() {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setMaxPerNodeCount(1);

        IgniteUuid srvcId = IgniteUuid.randomUuid();

        DynamicServiceChangeRequest depReq = DynamicServiceChangeRequest.deploymentRequest(srvcId, cfg);

        assertTrue(depReq.deploy());
        assertEquals(srvcId, depReq.serviceId());
        assertEquals(cfg, depReq.configuration());
    }

    /** */
    @Test
    public void undeploymentRequest() {
        IgniteUuid srvcId = IgniteUuid.randomUuid();

        DynamicServiceChangeRequest undepReq = DynamicServiceChangeRequest.undeploymentRequest(srvcId);

        assertTrue(undepReq.undeploy());
        assertEquals(srvcId, undepReq.serviceId());
    }

    /** */
    @Test
    public void serviceId() {
        IgniteUuid srvcId = IgniteUuid.randomUuid();

        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(srvcId);

        assertEquals(srvcId, req.serviceId());
    }

    /** */
    @Test
    public void configuration() {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setTotalCount(10);

        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(IgniteUuid.randomUuid());

        req.configuration(cfg);

        assertEquals(cfg, req.configuration());
    }

    /** */
    @Test
    public void deployFlag() {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(IgniteUuid.randomUuid());

        assertFalse(req.deploy());

        req.markDeploy();

        assertTrue(req.deploy());
    }

    /** */
    @Test
    public void undeployFlag() {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(IgniteUuid.randomUuid());

        assertFalse(req.undeploy());

        req.markUndeploy();

        assertTrue(req.undeploy());
    }
}