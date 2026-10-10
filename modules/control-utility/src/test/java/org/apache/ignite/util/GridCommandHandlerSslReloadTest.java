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

package org.apache.ignite.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadCommand;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.ssl.SslTestUtils.place;
import static org.apache.ignite.ssl.SslTestUtils.servedSubject;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/** Tests {@code --ssl reload} and {@code --ssl status} through the command line handler. */
public class GridCommandHandlerSslReloadTest extends GridCommandHandlerAbstractTest {
    /** Key store the nodes run on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** {@inheritDoc} */
    @Override protected boolean sslEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeTrue(cliCommandHandler());

        keyStore = Files.createTempFile("ignite-cli-ssl-reload-", ".jks");

        place("node01", keyStore);

        super.beforeTest();

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /** One command reloads every node, client ones included, status shows the new certificate, a broken store fails the command. */
    @Test
    public void testReloadAndStatus() throws Exception {
        assertNotNull("The reload must ask for confirmation", new SslReloadCommand().confirmationPrompt(new NoArg()));

        List<IgniteEx> nodes = List.of(startGrid(0), startGrid(1), startClientGrid(2));

        place("node02", keyStore);

        String out = executeCommand(EXIT_CODE_OK, "--ssl", "reload");

        for (IgniteEx node : nodes)
            assertContains(log, out, node.localNode().id() + ": reloaded " + transports(node) + "; serving subject=CN=node02");

        assertEquals("CN=node02", servedSubject(nodes.get(0).localNode().attribute(ATTR_REST_TCP_PORT)));

        out = executeCommand(EXIT_CODE_OK, "--ssl", "status");

        for (IgniteEx node : nodes)
            assertContains(log, out, node.localNode().id() + ": " + transports(node));

        assertContains(log, out, "serving subject=CN=node02");
        assertNotContains(log, out, "subject=CN=node01");
        assertContains(log, out, "last reload succeeded at");

        Files.write(keyStore, "not a key store".getBytes());

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR, "--ssl", "reload"), "Failed to initialize key store");
    }

    /** @return Transports of the node, all on the factory of the node; binary REST does not start on a client node. */
    private static String transports(IgniteEx node) {
        return (node.localNode().isClient() ? "" : "binary REST, ") + "client connector, communication, discovery";
    }
}
