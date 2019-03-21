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

package org.apache.ignite.internal.processor.security.datastreamer.closure;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteRunnable;
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
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_RUN, allowAllPermissionSet());

        startGrid(SRV_CHECK, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .expect(SRV_RUN, 1)
            .expect(SRV_CHECK, 1)
            .expect(SRV_ENDPOINT, 1)
            .expect(CLNT_ENDPOINT, 1);
    }

    /**
     *
     */
    @Test
    public void testDataStreamer() {
        IgniteRunnable checkCase = () -> {
            register();

            dataStreamer(Ignition.localIgnite());
        };

        runAndCheck(grid(SRV_INITIATOR), checkCase);
        runAndCheck(grid(CLNT_INITIATOR), checkCase);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> nodesToRun() {
        return Collections.singletonList(nodeId(SRV_RUN));
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> nodesToCheck() {
        return Collections.singletonList(nodeId(SRV_CHECK));
    }

    /**
     * @param node Node.
     */
    private void dataStreamer(Ignite node) {
        try (IgniteDataStreamer<Integer, Integer> strm = node.dataStreamer(CACHE_NAME)) {
            strm.receiver(StreamVisitor.from(new DataStreamClosure(endpoints())));

            strm.addData(prmKey(grid(SRV_CHECK)), 100);
        }
    }

    /** */
    static class DataStreamClosure extends BroadcastRunner implements
        IgniteBiInClosure<IgniteCache<Integer, Integer>, Map.Entry<Integer, Integer>> {
        /** {@inheritDoc} */
        public DataStreamClosure(Collection<UUID> endpoints) {
            super(endpoints);
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries, Map.Entry<Integer, Integer> entry) {
            registerAndBroadcast();
        }
    }
}
