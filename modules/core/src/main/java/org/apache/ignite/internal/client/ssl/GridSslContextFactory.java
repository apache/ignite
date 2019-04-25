/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.ssl;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

/**
 * This interface provides creation of SSL context both for server and client use.
 * <p>
 * Usually, it is enough to configure context from a particular key and trust stores, this functionality is provided
 * in {@link GridSslBasicContextFactory}.
 * @deprecated Use {@link Factory} instead.
 */
@Deprecated
public interface GridSslContextFactory {
    /**
     * Creates SSL context based on factory settings.
     *
     * @return Initialized SSL context.
     * @throws SSLException If SSL context could not be created.
     */
    public SSLContext createSslContext() throws SSLException;
}