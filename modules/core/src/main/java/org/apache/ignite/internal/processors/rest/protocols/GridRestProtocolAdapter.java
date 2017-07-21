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

package org.apache.ignite.internal.processors.rest.protocols;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocol;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import sun.misc.BASE64Encoder;

/**
 * Abstract protocol adapter.
 */
public abstract class GridRestProtocolAdapter implements GridRestProtocol {
    /** UTF-8 charset. */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** Context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Secret key. */
    protected final String secretKey;

    /** Host used by this protocol. */
    protected InetAddress host;

    /** Port used by this protocol. */
    protected int port;

    /**
     * @param ctx Context.
     */
    @SuppressWarnings({"OverriddenMethodCallDuringObjectConstruction"})
    protected GridRestProtocolAdapter(GridKernalContext ctx) {
        assert ctx != null;
        assert ctx.config().getConnectorConfiguration() != null;

        this.ctx = ctx;

        log = ctx.log(getClass());

        secretKey = ctx.config().getConnectorConfiguration().getSecretKey();
    }

    /**
     * Authenticates current request.
     * <p>
     * Token consists of 2 parts separated by semicolon:
     * <ol>
     *     <li>Timestamp (time in milliseconds)</li>
     *     <li>Base64 encoded SHA1 hash of {1}:{secretKey}</li>
     * </ol>
     *
     * @param tok Authentication token.
     * @return {@code true} if authentication info provided in request is correct.
     */
    protected boolean authenticate(@Nullable String tok) {
        if (F.isEmpty(secretKey))
            return true;

        if (F.isEmpty(tok))
            return false;

        StringTokenizer st = new StringTokenizer(tok, ":");

        if (st.countTokens() != 2)
            return false;

        String ts = st.nextToken();
        String hash = st.nextToken();

        String s = ts + ':' + secretKey;

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            BASE64Encoder enc = new BASE64Encoder();

            md.update(s.getBytes(UTF_8));

            String compHash = enc.encode(md.digest());

            return hash.equalsIgnoreCase(compHash);
        }
        catch (NoSuchAlgorithmException e) {
            U.error(log, "Failed to check authentication signature.", e);
        }

        return false;
    }

    /**
     * @return Start information string.
     */
    protected String startInfo() {
        return "Command protocol successfully started [name=" + name() + ", host=" + host + ", port=" + port + ']';
    }

    /**
     * @return Stop information string.
     */
    protected String stopInfo() {
        return "Command protocol successfully stopped: " + name();
    }

    /**
     * @param cond Condition to check.
     * @param condDesc Error message.
     * @throws IgniteCheckedException If check failed.
     */
    protected final void assertParameter(boolean cond, String condDesc) throws IgniteCheckedException {
        if (!cond)
            throw new IgniteCheckedException("REST protocol parameter failed condition check: " + condDesc);
    }

    /**
     * @return Client configuration.
     */
    protected ConnectorConfiguration config() {
        return ctx.config().getConnectorConfiguration();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteBiTuple<String, Object>> getProperties() {
        try {
            // All addresses for wildcard endpoint, `null` without.
            IgniteBiTuple<Collection<String>, Collection<String>> addrs = host != null ?
                U.resolveLocalAddresses(host) : null;

            return port > 0 ?
                Arrays.asList(
                    F.<String, Object>t(getAddressPropertyName(), addrs.get1()),
                    F.<String, Object>t(getHostNamePropertyName(), addrs.get2()),
                    F.<String, Object>t(getPortPropertyName(), port)
                ) :
                Collections.<IgniteBiTuple<String, Object>>emptyList();
        }
        catch (IgniteCheckedException | IOException ignored) {
            return null;
        }
    }

    /**
     * Return node attribute name to store used address.
     *
     * @return Node attribute name.
     */
    protected abstract String getAddressPropertyName();

    /**
     * Return node attribute name to store used host name.
     *
     * @return Node attribute name.
     */
    protected abstract String getHostNamePropertyName();

    /**
     * Return node attribute name to store used port number.
     *
     * @return Node attribute name.
     */
    protected abstract String getPortPropertyName();

    /** {@inheritDoc} */
    @Override public void onKernalStart() {
        // No-op.
    }
}