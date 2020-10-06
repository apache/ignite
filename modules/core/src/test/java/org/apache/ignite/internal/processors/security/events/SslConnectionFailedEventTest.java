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

package org.apache.ignite.internal.processors.security.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.ConnectionEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.events.EventType.EVT_SSL_CONNECTION_FAILED;

/**
 * Test for SSL connection failed event.
 */
@RunWith(JUnit4.class)
public class SslConnectionFailedEventTest extends AbstractSecurityTest {
    /** */
    List<Event> evts = new ArrayList<>();


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param instanceName Instance name.
     */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        // After starting 2 nodes we switch to not trusted certificate to initiate failed connection event
        String keyStorePathProp = instanceName.endsWith("2")
            ? "ssl.keystore.nottrusted.path"
            : "ssl.keystore.node01.path";

        SslContextFactory sslFactory = createSslContextFactory(keyStorePathProp);

        cfg.setSslContextFactory(sslFactory);

        int[] events = new int[]{EVT_SSL_CONNECTION_FAILED};

        IgnitePredicate<? extends Event> lsnr = (IgnitePredicate<Event>)evt -> {
            evts.add(evt);

            return true;
        };

        cfg.setIncludeEventTypes(events);
        cfg.setLocalEventListeners(Collections.singletonMap(lsnr, events));

        return cfg;
    }

    /** */
    private static SslContextFactory createSslContextFactory(String keyStorePathProp) {
        SslContextFactory sslCtxFactory = new SslContextFactory();
        sslCtxFactory.setKeyStoreFilePath(
            U.resolveIgnitePath(GridTestProperties.getProperty(keyStorePathProp)).getAbsolutePath());
        sslCtxFactory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        sslCtxFactory.setTrustStoreFilePath(
            U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.trustone.path")).getAbsolutePath());
        sslCtxFactory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        return sslCtxFactory;
    }

    /**
     *
     */
    @Test
    public void testNotTrusted() throws Exception {
        assertTrue(evts.isEmpty());

        try {
            startGrids(3);
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.hasCause(SSLException.class));
        }

        assertTrue(!evts.isEmpty());

        Event evt = evts.get(0);

        assertTrue(evt instanceof ConnectionEvent);
        assertNotNull(evt.message());
        assertTrue(evt.message().contains("SSL connection with remote node failed"));
    }
}
