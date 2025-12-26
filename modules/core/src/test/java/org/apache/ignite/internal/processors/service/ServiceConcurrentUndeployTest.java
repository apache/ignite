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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests concurrent deploy/undeploy services.
 */
public class ServiceConcurrentUndeployTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void test() throws Exception {
        try (IgniteEx ignite = startGrid(0); IgniteEx client0 = startClientGrid(1); IgniteEx client1 = startClientGrid(2)) {
            for (int i = 0; i < 3; i++) {
                IgniteFuture<Void> fut = client0.services().deployNodeSingletonAsync(
                    "myservice",
                    new LongInitializedTestService(ThreadLocalRandom.current().nextLong(1001))
                );

                fut.get();

                // 1. Each client sees deployed service.
                // 2. Each client sends request to undeploy service.
                // 3. On second undeploy error throws.
                IgniteFuture<Void> fut0 = client0.services().cancelAllAsync();
                IgniteFuture<Void> fut1 = client1.services().cancelAllAsync();

                fut0.get();
                fut1.get();
            }
        }
    }
}
