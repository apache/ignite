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

package org.apache.ignite.internal.processors.security.daemon;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/** 
 * Tests that daemon node can successfully join the cluster with security enabled and perform operations that require
 * authorization. 
 */
public class DaemonNodeBasicSecurityTest extends AbstractSecurityTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(
        String instanceName,
        AbstractTestSecurityPluginProvider pluginProv
    ) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setDaemon(instanceName.contains("daemon"))
            .setClusterStateOnStart(INACTIVE);
    }

    /** */
    @Test
    public void testDaemonNode() throws Exception {
        IgniteEx crd = startGridAllowAll("crd");

        IgniteEx daemonNode = startGridAllowAll("daemon");

        daemonNode.cluster().state(ACTIVE);

        assertEquals(ACTIVE, crd.cluster().state());
    }
}
