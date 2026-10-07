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
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/** Node component whose TLS certificates can be replaced at runtime, together with the outcome of its reloads. */
public abstract class SslContextReloadable {
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

    /** Guards the outcome fields, which are written together. */
    private final Object mux = new Object();

    /** Time of the last successful reload, {@code 0} if there was none. */
    private volatile long lastSuccessTime;

    /** Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    private volatile long lastFailureTime;

    /** Reason of the last failed reload since the last successful one. */
    private volatile String lastFailure;

    /** Failed reloads in a row since the last successful one. */
    private volatile int failures;

    /** Time of the next automatic renewal, {@code 0} if none is planned. */
    private volatile long nextRenewalTime;

    /** @return Transports served, as the commands and the node log name them. */
    public abstract Collection<String> transports();

    /**
     * Builds the certificates the configuration points at now, checks them, puts them in use for new connections and records the outcome.
     *
     * @throws IgniteCheckedException If they cannot be built or would be refused, or there is nothing to read again. The ones in use stay.
     */
    public void reload() throws IgniteCheckedException {
        try {
            rebuild();
        }
        catch (Throwable e) {
            onFailure(e);

            throw e;
        }

        onReloaded();
    }

    /**
     * Builds the certificates the configuration points at now, checks them and puts them in use for new connections.
     *
     * @throws IgniteCheckedException If they cannot be built or would be refused, or there is nothing to read again. The ones in use stay.
     */
    protected abstract void rebuild() throws IgniteCheckedException;

    /** @return Chain presented on new connections, own certificate first, or {@code null} if it is unknown. */
    public abstract @Nullable X509Certificate[] servedChain();

    /** @return Certificate presented on new connections, or {@code null} if it is unknown. */
    public @Nullable X509Certificate servedCertificate() {
        X509Certificate[] chain = servedChain();

        return chain == null ? null : chain[0];
    }

    /** Records a successful reload. */
    void onReloaded() {
        synchronized (mux) {
            lastSuccessTime = System.currentTimeMillis();
            lastFailureTime = 0;
            lastFailure = null;
            failures = 0;
        }
    }

    /** @param e Why the reload failed. */
    void onFailure(Throwable e) {
        String reason = SslCertificates.reason(e);

        synchronized (mux) {
            lastFailureTime = System.currentTimeMillis();
            lastFailure = reason;
            failures++;
        }
    }

    /** @return Time of the last successful reload, {@code 0} if there was none. */
    public long lastSuccessTime() {
        return lastSuccessTime;
    }

    /** @return Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    public long lastFailureTime() {
        return lastFailureTime;
    }

    /** @return Reason of the last failed reload since the last successful one, {@code null} if there was none. */
    public @Nullable String lastFailure() {
        return lastFailure;
    }

    /** @return Failed reloads in a row since the last successful one. */
    public int failures() {
        return failures;
    }

    /** @return Time of the next automatic renewal, {@code 0} if none is planned. */
    public long nextRenewalTime() {
        return nextRenewalTime;
    }

    /** @param nextRenewalTime Time of the next automatic renewal, {@code 0} if none is planned. */
    void nextRenewalTime(long nextRenewalTime) {
        this.nextRenewalTime = nextRenewalTime;
    }
}
