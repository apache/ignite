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

package org.apache.ignite.internal.ssl;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Owns the SSL context built out of one configured factory, and hands it to everything that factory was given to.
 * <p>
 * A transport asks for the context whenever it opens a connection, so replacing the context here is what puts
 * rotated certificates in use: connections opened afterwards get the new one, established ones are not touched.
 * One provider stands for one factory, however many transports share it, so a rotation cannot leave them on
 * certificates read at different moments.
 */
public class SslContextProvider implements SslContextReloadable {
    /** Builds a context out of whatever the configured stores hold at the moment of the call. */
    private final Factory<SSLContext> factory;

    /** Transports served, sorted, as the reload command reports them. */
    private final Set<String> users = new ConcurrentSkipListSet<>();

    /** Whether any user connects nodes to each other, which is what makes a context worth checking before use. */
    private volatile boolean interNode;

    /** Context in use. */
    private volatile SSLContext ctx;

    /**
     * @param factory Factory to build the context with.
     */
    public SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        ctx = factory.create();
    }

    /**
     * @return Context to open the next connection with.
     */
    public SSLContext context() {
        return ctx;
    }

    /**
     * @param user Transport the context is handed to.
     * @param interNode Whether that transport connects nodes to each other.
     */
    public void addUser(String user, boolean interNode) {
        users.add(user);

        if (interNode)
            this.interNode = true;
    }

    /**
     * @return Transports this provider serves.
     */
    public Collection<String> users() {
        return Collections.unmodifiableCollection(users);
    }

    /** {@inheritDoc} */
    @Override public boolean reloadSslContext() throws IgniteCheckedException {
        SSLContext rebuilt = rebuild();

        if (rebuilt == null)
            return false;

        ctx = rebuilt;

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean checkSslContext() throws IgniteCheckedException {
        return rebuild() != null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable X509Certificate servedCertificate() {
        if (!interNode)
            return null;

        try {
            return SslContextValidator.validateInterNode(ctx);
        }
        catch (SSLException ignored) {
            // The certificate is reported next to what was reloaded, so a context that cannot tell simply says
            // nothing rather than failing the command.
            return null;
        }
    }

    /**
     * @return Context built from the stores as they are now, or {@code null} if the factory handed back the one
     *      already in use and there is therefore nothing to put in use.
     * @throws IgniteCheckedException If the context could not be built, or an inter-node transport would refuse it.
     */
    private @Nullable SSLContext rebuild() throws IgniteCheckedException {
        try {
            SSLContext rebuilt = factory.create();

            if (rebuilt == ctx)
                return null;

            if (interNode)
                SslContextValidator.validateInterNode(rebuilt);

            return rebuilt;
        }
        catch (SSLException e) {
            throw new IgniteCheckedException(e);
        }
    }
}
