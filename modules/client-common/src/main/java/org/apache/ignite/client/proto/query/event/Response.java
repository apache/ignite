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

package org.apache.ignite.client.proto.query.event;

import io.netty.util.internal.StringUtil;
import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * SQL listener response.
 */
public abstract class Response implements ClientMessage {
    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    private int status;

    /** Error. */
    private String err;

    /** Has results. */
    protected boolean hasResults;

    /**
     * Constructs successful response.
     */
    protected Response() {
        status = STATUS_SUCCESS;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    protected Response(int status, String err) {
        assert status != STATUS_SUCCESS;

        this.status = status;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        packer.packBoolean(hasResults);
        packer.packInt(status);

        if (StringUtil.isNullOrEmpty(err))
            packer.packNil();
        else
            packer.packString(err);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        hasResults = unpacker.unpackBoolean();
        status = unpacker.unpackInt();

        if (!unpacker.tryUnpackNil())
            err = unpacker.unpackString();
    }

    /**
     * Get the status.
     *
     * @return Status.
     */
    public int status() {
        return status;
    }

    /**
     * Set the status.
     *
     * @param status Status.
     */
    public void status(int status) {
        this.status = status;
    }

    /**
     * Gets the error.
     *
     * @return Error.
     */
    public String err() {
        return err;
    }

    /**
     * Set the error message.
     *
     * @param err Error.
     */
    public void err(String err) {
        this.err = err;
    }

    /**
     * Gets hasResults flag.
     *
     * @return Has results.
     */
    public boolean hasResults() {
        return hasResults;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Response.class, this);
    }
}
