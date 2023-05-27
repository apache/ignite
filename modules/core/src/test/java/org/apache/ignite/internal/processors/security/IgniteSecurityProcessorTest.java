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

package org.apache.ignite.internal.processors.security;

import java.lang.reflect.Method;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridIoSecurityAwareMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 * Unit test for {@link IgniteSecurityProcessor}.
 */
public class IgniteSecurityProcessorTest extends AbstractSecurityTest {
    /** */
    private static ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(
        String instanceName,
        AbstractTestSecurityPluginProvider pluginProv
    ) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        listeningLog = new ListeningTestLogger();
    }

    /** Checks that {@link IgniteSecurityProcessor#withContext(UUID)} throws exception in case a node ID is unknown. */
    @Test
    public void testThrowIllegalStateExceptionIfNodeNotFoundInDiscoCache() throws Exception {
        IgniteEx srv = startGridAllowAll("srv");

        IgniteEx cli = startClientAllowAll("cli");

        Method getSpiMethod = GridManagerAdapter.class.getDeclaredMethod("getSpi");

        getSpiMethod.setAccessible(true);

        TcpCommunicationSpi spi = (TcpCommunicationSpi)getSpiMethod.invoke(cli.context().io());

        LogListener logPattern = LogListener
            .matches(s -> s.contains("Failed to obtain a security context."))
            .times(1)
            .build();

        listeningLog.registerListener(logPattern);

        spi.sendMessage(srv.localNode(), new GridIoSecurityAwareMessage(
            UUID.randomUUID(),
            PUBLIC_POOL,
            TOPIC_CACHE,
            TOPIC_CACHE.ordinal(),
            new AffinityTopologyVersion(),
            false,
            0,
            false
        ));

        GridTestUtils.waitForCondition(logPattern::check, getTestTimeout());
    }
}
