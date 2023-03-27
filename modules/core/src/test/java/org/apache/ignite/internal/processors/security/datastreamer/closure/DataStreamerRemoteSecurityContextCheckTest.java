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

package org.apache.ignite.internal.processors.security.datastreamer.closure;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.stream.StreamVisitor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing operation security context when the closure of DataStreamer is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts DataStreamer's closure. That closure is executed on
 * 'check' node and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification that operation
 * security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class DataStreamerRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testDataStreamer() {
        runAndCheck(() -> {
                try (IgniteDataStreamer<Integer, Integer> strm = Ignition.localIgnite().dataStreamer(CACHE_NAME)) {
                    strm.receiver(StreamVisitor.from(new ExecRegisterAndForwardAdapter<>(OPERATION_CHECK, endpointIds())));

                    strm.addData(primaryKey(grid(SRV_CHECK)), 100);
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToRun() {
        return Collections.singletonList(SRV_RUN);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToCheck() {
        return Collections.singletonList(SRV_CHECK);
    }
}
