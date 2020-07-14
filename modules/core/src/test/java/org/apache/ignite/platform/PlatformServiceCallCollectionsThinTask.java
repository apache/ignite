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

package org.apache.ignite.platform;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Test invoke {@link PlatformService} methods with collections and arrays as arguments and return type from
 * java thin client.
 */
public class PlatformServiceCallCollectionsThinTask extends AbstractPlatformServiceCallTask {
    /** {@inheritDoc} */
    @Override ComputeJobAdapter createJob(String svcName) {
        return new PlatformServiceCallCollectionsThinJob(svcName);
    }

    /** */
    static class PlatformServiceCallCollectionsThinJob extends
        PlatformServiceCallCollectionsTask.PlatformServiceCallCollectionsJob {
        /** Thin client. */
        IgniteClient client;

        /**
         * @param srvcName Service name.
         */
        PlatformServiceCallCollectionsThinJob(String srvcName) {
            super(srvcName);
        }

        /** {@inheritDoc} */
        @Override TestPlatformService serviceProxy() {
            return client.services().serviceProxy(srvcName, TestPlatformService.class);
        }

        /** {@inheritDoc} */
        @Override void runTest() {
            client = startClient();

            try {
                super.runTest();
            }
            finally {
                U.close(client, ignite.log().getLogger(getClass()));
            }
        }
    }
}
