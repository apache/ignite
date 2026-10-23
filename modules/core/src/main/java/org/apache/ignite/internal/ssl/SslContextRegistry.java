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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL contexts of a node: one provider per configured factory, with automatic renewals and their node-wide failure metric. */
public class SslContextRegistry extends GridProcessorAdapter {
    /** */
    public static final String COMMUNICATION = "communication";

    /** */
    public static final String DISCOVERY = "discovery";

    /** */
    public static final String CLIENT_CONNECTOR = "client connector";

    /** */
    public static final String BINARY_REST = "binary REST";

    /** */
    public static final String HTTP_REST = "HTTP REST";

    /** */
    private final Collection<SslContextProvider> providers = new CopyOnWriteArrayList<>();

    /**
     * Single-threaded executor for all renewals on this node, so a slow factory does not block a shared pool thread.
     * Its thread starts with the first renewal.
     */
    private final ScheduledExecutorService renewalExec;

    /** @param ctx Kernal context. */
    public SslContextRegistry(GridKernalContext ctx) {
        super(ctx);

        renewalExec = newSingleThreadScheduledExecutor("ssl-renewal", ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        ctx.metric().registry("ssl").register("ConsecutiveRenewalFailures",
            () -> providers.stream().mapToInt(SslContextProvider::renewalFailures).sum(),
            "Sum of unsuccessful automatic renewals across factories; each factory resets its count after a later-expiring chain.");
    }

    /**
     * @param factory Factory the transport is configured with.
     * @param transport Transport, one of the names above.
     * @return Context that delegates to the factory's current context. Transports configured with the same factory object share it,
     *      so reload updates the context for all of them.
     */
    public synchronized SSLContext register(Factory<SSLContext> factory, String transport) {
        SslContextProvider provider = providers.stream().filter(p -> p.factory() == factory).findFirst().orElse(null);

        if (provider == null) {
            provider = new SslContextProvider(factory);
            provider.addTransport(transport);
            provider.startRenewal(renewalExec, ctx.log(SslRenewal.class));
            warnIfExpired(provider);

            providers.add(provider);
        }
        else
            provider.addTransport(transport);

        return provider.sharedContext();
    }

    /** @param provider Newly registered provider. */
    private void warnIfExpired(SslContextProvider provider) {
        X509Certificate[] chain = provider.certificateChain();
        X509Certificate invalid = chain == null ? null : SslCertificates.invalidAt(chain, System.currentTimeMillis());

        if (invalid != null) {
            U.warn(log, "The TLS certificate chain in use is not valid now on " +
                provider.transports() + "; put a valid certificate in place and run control.sh --ssl reload [" +
                SslCertificates.describe(invalid) + ']');
        }
    }

    /** @return One provider per factory object, in registration order; sharing a factory never adds another provider. */
    public Collection<SslContextProvider> providers() {
        return Collections.unmodifiableCollection(providers);
    }

    /** Interrupts a renewal attempt in progress and waits for it to end. */
    @Override public void onKernalStop(boolean cancel) {
        renewalExec.shutdownNow();

        try {
            renewalExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
