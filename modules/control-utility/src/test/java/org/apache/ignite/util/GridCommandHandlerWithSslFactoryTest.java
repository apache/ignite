/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.util.List;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.Assume;

/**
 *
 */
public class GridCommandHandlerWithSslFactoryTest extends GridCommandHandlerWithSslTest {
    /** Keystore path. */
    private static final String KEYSTORE_PATH = "KEYSTORE_PATH";

    /** Keystore password. */
    private static final String KEYSTORE_PASSWORD = "KEYSTORE_PASSWORD";

    /** Custorm SSL factory used. */
    protected static volatile boolean factoryUsed;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(KEYSTORE_PATH, GridTestUtils.keyStorePath("node01"));
        System.setProperty(KEYSTORE_PASSWORD, GridTestUtils.keyStorePassword());

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeTrue(invoker.equalsIgnoreCase(CLI_INVOKER));

        super.beforeTest();
    }

    /** */
    @AfterClass
    public static void tearDown() {
        System.clearProperty(KEYSTORE_PATH);
        System.clearProperty(KEYSTORE_PASSWORD);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Assume.assumeTrue(invoker.equalsIgnoreCase(CLI_INVOKER));

        super.afterTest();

        assertTrue(factoryUsed);

        factoryUsed = false;
    }

    /** {@inheritDoc} */
    @Override protected void extendSslParams(List<String> params) {
        params.add("--ssl-factory");
        params.add(U.resolveIgnitePath("modules/control-utility/src/test/resources/ssl-factory-config.xml").getPath());
    }

    /** {@inheritDoc} */
    @Override protected Factory<SSLContext> sslFactory() {
        Factory<SSLContext> factory = super.sslFactory();

        assertFalse(factory instanceof SslFactory);

        return factory;
    }

    /**
     * Custom SSL Factory.
     */
    public static class SslFactory extends SslContextFactory {
        /** {@inheritDoc} */
        @Override public SSLContext create() {
            factoryUsed = true;

            return super.create();
        }
    }

    /** {@inheritDoc} */
    @Override public void testCleaningGarbageAfterCacheDestroyedAndNodeStop_ControlConsoleUtil() throws Exception {
        super.testCleaningGarbageAfterCacheDestroyedAndNodeStop_ControlConsoleUtil();

        factoryUsed = true; // It's an SSL free test. Setting to `true` to avoid fail on check.
    }
}
