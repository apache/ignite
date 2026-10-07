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

package org.apache.ignite.ssl;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;

/** Represents abstract implementation of SSL Context Factory that builds a new {@link SSLContext} on every {@link #create()}. */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Default share of the certificate lifetime left when a node renews it. */
    public static final double DFLT_RENEW_BEFORE_FRACTION = 0.15;

    /** Default pause after the first renewal attempt that puts no new certificate in use, in milliseconds. */
    public static final long DFLT_RENEWAL_RETRY_MIN_INTERVAL = 60_000L;

    /** Default longest pause between renewal attempts that put no new certificate in use, in milliseconds. */
    public static final long DFLT_RENEWAL_RETRY_MAX_INTERVAL = 3_600_000L;

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

    /** Whether a node renews the certificate by itself before it expires. */
    private boolean renewalEnabled;

    /** Share of the certificate lifetime left when a node renews it. */
    private double renewBeforeFraction = DFLT_RENEW_BEFORE_FRACTION;

    /** Maximum renewal window, in milliseconds; {@code 0} means no maximum. */
    private long renewBefore;

    /** Pause after the first renewal attempt that puts no new certificate in use, in milliseconds. */
    private long renewalRetryMinInterval = DFLT_RENEWAL_RETRY_MIN_INTERVAL;

    /** Longest pause between renewal attempts that put no new certificate in use, in milliseconds. */
    private long renewalRetryMaxInterval = DFLT_RENEWAL_RETRY_MAX_INTERVAL;

    /**
     * Gets protocol for secure transport.
     *
     * @return SSL protocol name.
     */
    public String getProtocol() {
        return proto;
    }

    /**
     * Sets protocol for secure transport. If not specified, {@link #DFLT_SSL_PROTOCOL} will be used.
     *
     * @param proto SSL protocol name.
     */
    public void setProtocol(String proto) {
        A.notNull(proto, "proto");

        this.proto = proto;
    }

    /**
     * Sets enabled cipher suites.
     *
     * @param cipherSuites enabled cipher suites.
     */
    public void setCipherSuites(String... cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    /**
     * Gets enabled cipher suites.
     *
     * @return enabled cipher suites
     */
    public String[] getCipherSuites() {
        return cipherSuites;
    }

    /**
     * Gets enabled protocols.
     *
     * @return Enabled protocols.
     */
    public String[] getProtocols() {
        return protocols;
    }

    /**
     * Sets enabled protocols.
     *
     * @param protocols Enabled protocols.
     */
    public void setProtocols(String... protocols) {
        this.protocols = protocols;
    }

    /** @return Whether a node renews the certificate by itself before it expires. */
    public boolean isRenewalEnabled() {
        return renewalEnabled;
    }

    /**
     * Sets whether a node renews the certificate by itself before it expires. Disabled by default.
     *
     * @param renewalEnabled Whether renewal is enabled.
     */
    public void setRenewalEnabled(boolean renewalEnabled) {
        this.renewalEnabled = renewalEnabled;
    }

    /** @return Share of the certificate lifetime, from its start to the earliest expiry in its chain, left when a node renews it. */
    public double getRenewBeforeFraction() {
        return renewBeforeFraction;
    }

    /**
     * Sets the share of the certificate lifetime, from its start to the earliest expiry in its chain, left when a node renews it;
     * greater than {@code 0} and less than {@code 1}. If not specified, {@link #DFLT_RENEW_BEFORE_FRACTION} is used.
     *
     * @param renewBeforeFraction Share of the lifetime.
     */
    public void setRenewBeforeFraction(double renewBeforeFraction) {
        this.renewBeforeFraction = renewBeforeFraction;
    }

    /** @return Maximum renewal window, in milliseconds; {@code 0} means no maximum. */
    public long getRenewBefore() {
        return renewBefore;
    }

    /**
     * Sets the maximum renewal window, in milliseconds, not negative: a node takes the smaller of it and the window from
     * {@link #getRenewBeforeFraction()}. {@code 0}, the default, means no maximum.
     *
     * @param renewBefore Maximum renewal window, in milliseconds.
     */
    public void setRenewBefore(long renewBefore) {
        this.renewBefore = renewBefore;
    }

    /** @return Pause after the first renewal attempt that puts no new certificate in use, in milliseconds. */
    public long getRenewalRetryMinInterval() {
        return renewalRetryMinInterval;
    }

    /**
     * Sets the pause, in milliseconds, after the first renewal attempt that puts no new certificate in use, because the factory hands back
     * the same one or fails. Must be positive. A node never renews more often than this. If not specified,
     * {@link #DFLT_RENEWAL_RETRY_MIN_INTERVAL} is used.
     *
     * @param renewalRetryMinInterval Pause, in milliseconds.
     */
    public void setRenewalRetryMinInterval(long renewalRetryMinInterval) {
        this.renewalRetryMinInterval = renewalRetryMinInterval;
    }

    /** @return Longest pause between renewal attempts that put no new certificate in use, in milliseconds. */
    public long getRenewalRetryMaxInterval() {
        return renewalRetryMaxInterval;
    }

    /**
     * Sets the longest pause between renewal attempts that put no new certificate in use, in milliseconds, not less than
     * {@link #getRenewalRetryMinInterval()}. If a quarter of the renewal window is shorter, a node takes it as the longest pause, but not
     * less than {@link #getRenewalRetryMinInterval()}. A node adds a random delay of up to half of every pause. If not specified,
     * {@link #DFLT_RENEWAL_RETRY_MAX_INTERVAL} is used.
     *
     * @param renewalRetryMaxInterval Pause, in milliseconds.
     */
    public void setRenewalRetryMaxInterval(long renewalRetryMaxInterval) {
        this.renewalRetryMaxInterval = renewalRetryMaxInterval;
    }

    /**
     * Creates SSL context based on factory settings.
     *
     * @return Initialized SSL context.
     * @throws SSLException If SSL context could not be created.
     */
    private SSLContext createSslContext() throws SSLException {
        checkParameters();

        KeyManager[] keyMgrs = createKeyManagers();

        TrustManager[] trustMgrs = createTrustManagers();

        try {
            SSLContext ctx = SSLContext.getInstance(proto);

            if (cipherSuites != null || protocols != null) {
                SSLParameters sslParameters = new SSLParameters();

                if (cipherSuites != null)
                    sslParameters.setCipherSuites(cipherSuites);

                if (protocols != null)
                    sslParameters.setProtocols(protocols);

                ctx = new SSLContextWrapper(ctx, sslParameters);
            }

            ctx.init(keyMgrs, trustMgrs, null);

            return ctx;
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported SSL protocol: " + proto, e);
        }
        catch (KeyManagementException e) {
            throw new SSLException("Failed to initialized SSL context.", e);
        }
    }

    /**
     * @param param Value.
     * @param name Name.
     * @throws SSLException If {@code null}.
     */
    protected void checkNullParameter(Object param, String name) throws SSLException {
        if (param == null)
            throw new SSLException("Failed to initialize SSL context (parameter cannot be null): " + name);
    }

    /**
     * Checks that all required parameters are set.
     *
     * @throws SSLException If any of required parameters is missing.
     */
    protected abstract void checkParameters() throws SSLException;

    /**
     * Called on every {@link #create()}: when a transport starts, on every {@code control.sh --ssl reload} and, with renewal enabled, in
     * the renewal window. Read the current key material each time: otherwise a reload or a renewal never sees a new certificate. Bound
     * every wait well below {@link #getRenewalRetryMinInterval()} and stop waiting when the thread is interrupted: all renewals of a node
     * run in one thread, and a call that hangs holds them up together with {@code --ssl reload}.
     *
     * @return Created Key Managers.
     * @throws SSLException If Key Managers could not be created.
     */
    protected abstract KeyManager[] createKeyManagers() throws SSLException;

    /**
     * Take the trusted authorities from a source local to the node, not from the service that issues the certificates: otherwise a reload
     * or a renewal accepts a certificate that other nodes do not trust yet.
     *
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /** {@inheritDoc} */
    @Override public SSLContext create() {
        try {
            return createSslContext();
        }
        catch (SSLException e) {
            throw new IgniteException(e);
        }
    }
}
