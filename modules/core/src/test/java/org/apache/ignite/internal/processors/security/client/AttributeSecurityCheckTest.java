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

package org.apache.ignite.internal.processors.security.client;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.security.UserAttributesFactory;
import org.apache.ignite.internal.processors.security.impl.TestAttributeSecurityPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class AttributeSecurityCheckTest extends AdditionalSecurityCheckTest {
    /** */
    private static Map<String, Object> userAttrs;

    /** */
    private static final int TEST_KEY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name){
        return new TestAttributeSecurityPluginProvider(name, null, ALLOW_ALL,
            globalAuth, true, new Consumer<Map<String, Object>>() {
            @Override public void accept(Map<String, Object> map) {
                userAttrs = map;
            }
        }, clientData());
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration getGridClientConfiguration() {
        return super.getGridClientConfiguration()
            .setUserAttributes(userAttributes());
    }

    /** */
    @Test
    public void testClientInfoGridClientNotFail() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        ignite.cluster().state(ClusterState.ACTIVE);

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            assertTrue(client.connected());

            GridClientClusterState state = client.state();

            // Close a coordinator to force the client to send a CLUSTER_CURRENT_STATE message to the other node
            // in the state.state() statement.
            ignite.close();

            userAttrs = null;

            state.state();

            for (int i = 0; i < TEST_KEY_CNT; i++)
                assertEquals(userAttrs.get("key" + i), "val" + i);
        }
    }

    /**
     * @return User attributes.
     */
    private Map<String, String> userAttributes(){
        Map<String, String> attrs = new UserAttributesFactory().create();

        for (int i = 0; i < TEST_KEY_CNT; i++)
            attrs.put("key" + i, "val" + i);

        return attrs;
    }
}
