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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.AbstractSslContextFactory;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL contexts of a node: one provider per configured factory, with the metrics of every transport and the automatic renewals. */
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
     * The one thread all renewals of the node run in, so that a renewal waiting for a slow factory takes no thread from a shared pool.
     * It starts with the first renewal.
     */
    private final ScheduledExecutorService renewalExec;

    /** @param ctx Kernal context. */
    public SslContextRegistry(GridKernalContext ctx) {
        super(ctx);

        renewalExec = newSingleThreadScheduledExecutor("ssl-renewal", ctx.igniteInstanceName());
    }

    /**
     * @param factory Factory the transport is configured with.
     * @param transport Transport, one of the names above.
     * @return Context that passes every call to the one in use at that moment. Transports configured with the same factory share it,
     *      so that a reload cannot leave them on certificates read at different moments.
     */
    public synchronized SSLContext register(Factory<SSLContext> factory, String transport) {
        SslContextProvider provider = providers.stream().filter(p -> p.factory() == factory).findFirst().orElse(null);

        boolean fresh = provider == null;

        if (fresh) {
            provider = new SslContextProvider(factory);

            providers.add(provider);
        }

        boolean nodesConnectedAlready = provider.nodesConnect();

        provider.addTransport(transport);

        if (fresh && factory instanceof AbstractSslContextFactory f && f.isRenewalEnabled()) {
            if (provider.servedChain() == null) {
                throw new IgniteException("Cannot tell which TLS certificate the SSL context factory serves, so it cannot be renewed " +
                    "automatically; for example, its key store has no private key [transports=" + transport + ']');
            }

            new SslRenewal(provider, f.getRenewBeforeFraction(), renewalExec, ctx.log(SslRenewal.class)).start();
        }

        if (provider.nodesConnect() && !nodesConnectedAlready && provider.servedChain() != null)
            warnIfPeersRefuse(provider);

        registerMetrics(transport, provider);

        return new CurrentSslContext(provider::context);
    }

    /** @param provider Provider that serves discovery or communication from now on. */
    private void warnIfPeersRefuse(SslContextProvider provider) {
        X509Certificate invalid = SslCertificates.invalidAt(provider.servedChain(), System.currentTimeMillis());

        if (invalid != null) {
            U.warn(log, "The TLS certificate chain in use is not valid now, so peers refuse new connections on " +
                provider.transports() + "; put a valid certificate in place and run control.sh --ssl reload [" +
                SslCertificates.describe(invalid) + ']');

            return;
        }

        try {
            SslCertificates.checkInterNodeHandshake(provider.context());
        }
        catch (SSLException e) {
            U.warn(log, "The node's own trust store does not accept its certificate, so control.sh --ssl reload and automatic " +
                "renewal will refuse every new certificate for " + provider.transports() + "; add the issuer of the node's " +
                "certificate to its trust store [" + SslCertificates.describe(provider.servedCertificate()) + ']', e);
        }
    }

    /** @return Providers in the order they were registered. */
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

    /**
     * @param transport Transport.
     * @param provider Provider serving it.
     */
    private void registerMetrics(String transport, SslContextProvider provider) {
        MetricRegistryImpl reg = ctx.metric().registry(metricName("ssl", transport.replace(' ', '.').toLowerCase()));

        reg.register("CertificateSubject", () -> Optional.ofNullable(provider.servedCertificate())
            .map(c -> c.getSubjectX500Principal().toString()).orElse(null), String.class,
            "Subject DN of the certificate presented on new connections.");

        reg.register("CertificateIssuer", () -> Optional.ofNullable(provider.servedCertificate())
            .map(c -> c.getIssuerX500Principal().toString()).orElse(null), String.class,
            "Issuer DN of the certificate presented on new connections.");

        reg.register("ChainNotAfter", () -> Optional.ofNullable(provider.servedChain()).map(SslCertificates::chainNotAfter).orElse(0L),
            "Earliest expiry time in the chain presented on new connections, in milliseconds; 0 if unknown.");

        reg.register("LastReloadSuccessTime", () -> provider.reloadOutcome().successTime(),
            "Time of the last successful reload, in milliseconds; 0 if none.");

        reg.register("LastReloadFailureReason", () -> provider.reloadOutcome().failureReason(), String.class,
            "Reason for the last failed reload; a successful reload clears it.");

        reg.register("ConsecutiveReloadFailures", () -> provider.reloadOutcome().failures(),
            "Failed reloads in a row since the last successful one.");

        reg.register("NextRenewalTime", () -> provider.nextRenewalTime(),
            "Time of the next automatic renewal, in milliseconds; 0 if none.");
    }
}
