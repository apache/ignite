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

package org.apache.ignite.internal.util.nio.ssl;

import org.apache.ignite.IgniteCheckedException;

/**
 * A component whose TLS/SSL context (certificates) can be hot-reloaded at runtime without a node restart. New
 * connections established after the reload use the updated certificates, while sessions that were already
 * established keep using their existing engines and are not interrupted.
 */
public interface SslContextReloadable {
    /**
     * Rebuilds the SSL context from the configured factory (re-reading the key and trust stores from disk) and
     * applies it so that new connections use the updated certificates.
     *
     * @return {@code true} if SSL is enabled for this component and the context was reloaded; {@code false} if SSL
     *      is not configured (no-op).
     * @throws IgniteCheckedException If the SSL context could not be reloaded (for example, the new key store is
     *      invalid). The previously active context is kept in this case.
     */
    public boolean reloadSslContext() throws IgniteCheckedException;
}
