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

package org.apache.ignite.internal.processors.rest.request;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Grid command request.
 */
public class GridRestRequest {
    /** Destination ID. */
    private UUID destId;

    /** Client ID. */
    private UUID clientId;

    /** Client network address. */
    private InetSocketAddress addr;

    /** Client credentials. */
    @GridToStringExclude
    private Object cred;

    /** Client session token. */
    private byte[] sesTok;

    /** Command. */
    private GridRestCommand cmd;

    /** */
    private AuthorizationContext authCtx;

    /** User attributes. */
    Map<String, String> userAttrs;

    /**
     * @return Destination ID.
     */
    public UUID destinationId() {
        return destId;
    }

    /**
     * @param destId Destination ID.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest destinationId(UUID destId) {
        this.destId = destId;

        return this;
    }

    /**
     * @return Command.
     */
    public GridRestCommand command() {
        return cmd;
    }

    /**
     * @param cmd Command.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest command(GridRestCommand cmd) {
        this.cmd = cmd;

        return this;
    }

    /**
     * Gets client ID that performed request.
     *
     * @return Client ID.
     */
    public UUID clientId() {
        return clientId;
    }

    /**
     * Sets client ID that performed request.
     *
     * @param clientId Client ID.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest clientId(UUID clientId) {
        this.clientId = clientId;

        return this;
    }

    /**
     * Gets client credentials for authentication process.
     *
     * @return Credentials.
     */
    public Object credentials() {
        return cred;
    }

    /**
     * Sets client credentials for authentication.
     *
     * @param cred Credentials.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest credentials(Object cred) {
        this.cred = cred;

        return this;
    }

    /**
     * Gets session token for already authenticated client.
     *
     * @return Session token.
     */
    public byte[] sessionToken() {
        return sesTok;
    }

    /**
     * Sets session token for already authenticated client.
     *
     * @param sesTok Session token.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest sessionToken(byte[] sesTok) {
        this.sesTok = sesTok;

        return this;
    }

    /**
     * @return Client address.
     */
    public InetSocketAddress address() {
        return addr;
    }

    /**
     * @param addr Client address.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest address(InetSocketAddress addr) {
        this.addr = addr;

        return this;
    }

    /**
     * @return Authorization context.
     */
    @Nullable public AuthorizationContext authorizationContext() {
        return authCtx;
    }

    /**
     * @param authCtx Authorization context.
     * @return This GridRestRequest for chaining.
     */
    public GridRestRequest authorizationContext(AuthorizationContext authCtx) {
        this.authCtx = authCtx;

        return this;
    }

    /**
     * Gets user attributes.
     *
     * @return User attributes.
     */
    public Map<String, String> userAttributes() {
        return userAttrs;
    }

    /**
     * Gets user attributes.
     *
     * @param userAttrs User attributes.
     */
    public void userAttributes(Map<String, String> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestRequest.class, this);
    }
}
