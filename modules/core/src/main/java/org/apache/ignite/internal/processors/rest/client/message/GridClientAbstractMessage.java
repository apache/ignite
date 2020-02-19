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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class provides implementation for commit message fields and cannot be used directly.
 */
public abstract class GridClientAbstractMessage implements GridClientMessage, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request ID (transient). */
    private transient long reqId;

    /** Client ID (transient). */
    private transient UUID id;

    /** Node ID (transient). */
    private transient UUID destId;

    /** Session token. */
    private byte[] sesTok;

    /** Login. */
    private String login;

    /** Password. */
    private String pwd;

    /** User attributes. */
    Map<String, String> userAttrs;

    /** {@inheritDoc} */
    @Override public long requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public UUID clientId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public void clientId(UUID id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public UUID destinationId() {
        return destId;
    }

    /** {@inheritDoc} */
    @Override public void destinationId(UUID destId) {
        this.destId = destId;
    }

    /**
     * @return Session token
     */
    @Override public byte[] sessionToken() {
        return sesTok;
    }

    /**
     * @param sesTok Session token.
     */
    @Override public void sessionToken(byte[] sesTok) {
        this.sesTok = sesTok;
    }

    /**
     * @return Login.
     */
    public String login() {
        return login;
    }

    /**
     * @param login New login.
     */
    public void login(String login) {
        this.login = login;
    }

    /**
     * @return Password.
     */
    public String password() {
        return pwd;
    }

    /**
     * @param pwd New password.
     */
    public void password(String pwd) {
        this.pwd = pwd;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> userAttributes() {
        return userAttrs;
    }

    /** {@inheritDoc} */
    @Override public void userAttributes(Map<String, String> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, sesTok);

        U.writeString(out, login);
        U.writeString(out, pwd);

        U.writeMap(out, userAttrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sesTok = U.readByteArray(in);

        login = U.readString(in);
        pwd = U.readString(in);

        userAttrs = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientAbstractMessage.class, this, super.toString());
    }
}
