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

package org.apache.ignite.internal.ducktest.tests.disk_full;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Deploys {@link StdoutFloodServiceImpl} on every server node, records {@code lines-><n><-}, the lines the service
 * wrote, once a second, and cancels the service on stop.
 */
public class StdoutFloodApplication extends IgniteAwareApplication {
    /** Service name. */
    private static final String SVC_NAME = "stdout-flood";

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        ignite.services(ignite.cluster().forServers()).deployNodeSingleton(SVC_NAME, new StdoutFloodServiceImpl());

        StdoutFloodService svc = ignite.services().serviceProxy(SVC_NAME, StdoutFloodService.class, false);

        recordResult("lines", svc.lines());

        markInitialized();

        while (active()) {
            Thread.sleep(1000);

            recordResult("lines", svc.lines());
        }

        ignite.services().cancel(SVC_NAME);

        markFinished();
    }
}
