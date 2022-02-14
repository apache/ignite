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

package org.apache.ignite.kubernetes.discovery;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.ThinClientKubernetesAddressFinder;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** Test that thin client connects to cluster with {@link ThinClientKubernetesAddressFinder}. */
public class TestClusterClientConnection extends KubernetesDiscoveryAbstractTest {
    /** */
    @Test
    public void testClientConnectsToCluster() throws Exception {
        mockServerResponse();

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(), false);

        IgniteEx crd = startGrid(cfg);
        String crdAddr = crd.localNode().addresses().iterator().next();

        mockServerResponse(crdAddr);

        ClientConfiguration ccfg = new ClientConfiguration();
        ccfg.setAddressesFinder(new ThinClientKubernetesAddressFinder(prepareConfiguration()));

        IgniteClient client = Ignition.startClient(ccfg);

        ClientCache cache = client.createCache("cache");
        cache.put(1, 2);
        assertEquals(2, cache.get(1));
    }
}
