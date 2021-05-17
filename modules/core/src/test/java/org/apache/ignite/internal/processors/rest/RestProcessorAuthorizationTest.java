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

package org.apache.ignite.internal.processors.rest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.client.CommonSecurityCheckTest;
import org.apache.ignite.internal.processors.security.impl.TestAuthorizationContextSecurityPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest.TEST_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Tests REST processor configuration via Ignite plugins functionality.
 */
public class RestProcessorAuthorizationTest extends CommonSecurityCheckTest {
    List<TestAuthorizationContextSecurityPluginProvider.TestAuthorizationContext> hndlr = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name) {
        return new TestAuthorizationContextSecurityPluginProvider(name, null, ALLOW_ALL,
            globalAuth, (authCtx) -> hndlr.add(authCtx), clientData());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDefaultRestProcessorInitialization() throws Exception {
        IgniteEx ignite = startGrid(0);

        assertEquals(ignite.context().rest().getClass(), GridRestProcessor.class);

        // Check with system permission
        executeCommand(GridRestCommand.GET_OR_CREATE_CACHE, "remoteClientName(0)", "DEFAULT_PWD");
    }

    /** */
    private void executeCommand(GridRestCommand cmd, String login, String pwd) throws IOException {
        String addr = "http://" + "127.0.0.1:8080" + "/ignite?cmd=" + cmd.key() +
            "&cacheName=" + TEST_CACHE +
            "&ignite.login=" + login + "&ignite.password=" + pwd;

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }
}
