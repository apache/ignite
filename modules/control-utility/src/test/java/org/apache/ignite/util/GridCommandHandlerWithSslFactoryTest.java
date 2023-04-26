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
import javax.net.ssl.SSLContext;
import org.apache.ignite.ssl.SslContextFactory;

/**
 *
 */
public class GridCommandHandlerWithSslFactoryTest extends GridCommandHandlerWithSslTest {
    /** Custorm SSL factory used. */
    protected static volatile boolean factoryUsed;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assertTrue(factoryUsed);

        factoryUsed = false;
    }

    /** {@inheritDoc} */
    @Override protected void addSslParams(List<String> params) {
        super.addSslParams(params);

        params.add("--ssl-factory");
        params.add("ssl-factory-config.xml");
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
