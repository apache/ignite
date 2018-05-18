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

package org.apache.ignite.internal.benchmarks.jmh;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;

/**
 * Benchmark utils.
 */
public class GridBenchmarkUtils {
    /** */
    private final static String SSL_KEYSTORE_PATH = "/modules/clients/src/test/keystore/server.jks";

    /** */
    private final static String SSL_KEYSTORE_PASSWORD = "123456";

    /**
     * Creates test-purposed SSL context factory from test key store with disabled trust manager.
     *
     * @return SSL context factory used in test.
     */
    public static Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(
            U.resolveIgnitePath(SSL_KEYSTORE_PATH).getAbsolutePath());

        factory.setKeyStorePassword(SSL_KEYSTORE_PASSWORD.toCharArray());

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }
}
