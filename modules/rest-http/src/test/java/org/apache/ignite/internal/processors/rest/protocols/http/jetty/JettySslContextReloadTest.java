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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.ssl.SslContextReloadable.HTTP_REST;
import static org.apache.ignite.ssl.SslTestUtils.place;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.reloadFailure;
import static org.apache.ignite.ssl.SslTestUtils.servedSubject;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/** Tests {@code --ssl reload} on the Jetty connector serving HTTP REST. */
public class JettySslContextReloadTest extends GridCommonAbstractTest {
    /** System property the Jetty configuration reads the key store path from. */
    private static final String KEY_STORE_PROP = "IGNITE_TEST_JETTY_KEY_STORE";

    /** Jetty configuration serving TLS from the key store at {@link #KEY_STORE_PROP}. */
    private static final String JETTY_CFG = "modules/rest-http/src/test/resources/jetty-ssl-reload.xml";

    /** Key store Jetty runs on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(JETTY_CFG));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-jetty-ssl-reload-", ".jks");

        place("node01", keyStore);

        System.setProperty(KEY_STORE_PROP, keyStore.toString());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.clearProperty(KEY_STORE_PROP);

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /** The connector serves the rotated certificate, and a key store it cannot read leaves it on the last good one. */
    @Test
    public void testReload() throws Exception {
        IgniteEx g = startGrid(0);

        place("node02", keyStore);

        assertContains(log, reload(g), ": reloaded " + HTTP_REST);
        assertEquals("CN=node02", servedSubject(g.localNode().attribute(ATTR_REST_JETTY_PORT)));
        assertEquals("CN=node02", g.context().metric().registry("ssl.http.rest").findMetric("CertificateSubject").getAsString());

        Files.write(keyStore, "not a key store".getBytes());

        assertContains(log, reloadFailure(g), "failed on " + HTTP_REST);
        assertEquals("CN=node02", servedSubject(g.localNode().attribute(ATTR_REST_JETTY_PORT)));
    }

    /** A connector handed a ready-made context has nothing to read again, so its reload fails. */
    @Test
    public void testReadyMadeContextNotReloaded() throws Exception {
        SslContextFactory.Server factory = new SslContextFactory.Server();

        factory.setSslContext(SSLContext.getDefault());

        GridTestUtils.assertThrows(log, new JettySslContextReloadable(factory)::reload, IgniteException.class, "ready-made");
    }
}
