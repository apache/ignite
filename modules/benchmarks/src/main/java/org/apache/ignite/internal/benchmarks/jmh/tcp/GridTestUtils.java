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

package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.io.File;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Copy from org.apache.ignite.testframework.GridTestUtils.
 */
public final class GridTestUtils {
    /**
     * Creates test-purposed SSL context factory from test key store with disabled trust manager.
     *
     * @return SSL context factory used in test.
     */
    public static Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(
            U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.path")).getAbsolutePath());

        factory.setKeyStorePassword(GridTestProperties.getProperty("ssl.keystore.password").toCharArray());

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * Gets file representing the path passed in. First the check is made if path is absolute. If not, then the check is
     * made if path is relative to ${IGNITE_HOME}. If both checks fail, then {@code null} is returned, otherwise file
     * representing path is returned. <p> See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is
     * retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path, or {@code null} if file cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static File resolveIgnitePath(String path) {
        return resolvePath(null, path);
    }

    /**
     * @param igniteHome Optional ignite home path.
     * @param path Path to resolve.
     * @return Resolved path, or {@code null} if file cannot be resolved.
     */
    @Nullable private static File resolvePath(@Nullable String igniteHome, String path) {
        File file = new File(path).getAbsoluteFile();

        if (!file.exists()) {
            String home = igniteHome != null ? igniteHome : U.getIgniteHome();

            if (home == null)
                return null;

            file = new File(home, path);

            return file.exists() ? file : null;
        }

        return file;
    }
}
