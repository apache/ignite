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

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for package-private service implementation.
 */
public class GridServicePackagePrivateSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPackagePrivateService() throws Exception {
        try {
            Ignite server = startGrid("server");

            server.services().deployClusterSingleton("my-service", MyServiceFactory.create());

            Ignite client = startClientGrid("client");

            MyService svc = client.services().serviceProxy("my-service", MyService.class, true);

            assertEquals(42, svc.hello());
        }
        finally {
            stopAllGrids();
        }
    }
}
