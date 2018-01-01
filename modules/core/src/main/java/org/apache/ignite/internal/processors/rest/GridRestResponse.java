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

package org.apache.ignite.internal.processors.rest;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * JSON response. Getters and setters must conform to JavaBean standard.
 */
public class GridRestResponse implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Authentication failure. */
    public static final int STATUS_AUTH_FAILED = 2;

    /** Security check failed. */
    public static final int STATUS_SECURITY_CHECK_FAILED = 3;

    /** Success status. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int successStatus = STATUS_SUCCESS;

    /** Session token. */
    private byte[] sesTokBytes;

    /** Session token string representation. */
    private String sesTokStr;

    /** Error. */
    private String err;

    /** Response object. */
    @GridToStringInclude(sensitive = true)
    private Object obj;

    /**
     *
     */
    public GridRestResponse() {
        // No-op.
    }

    /**
     * Constructs successful rest response.
     *
     * @param obj Response object.
     */
    public GridRestResponse(Object obj) {
        successStatus = STATUS_SUCCESS;
        this.obj = obj;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public GridRestResponse(int status, @Nullable String err) {
        assert status != STATUS_SUCCESS;

        successStatus = status;
        this.err = err;
    }

    /**
     * @return Success flag.
     */
    public int getSuccessStatus() {
        return successStatus;
    }

    /**
     * @return Response object.
     */
    public Object getResponse() {
        return obj;
    }

    /**
     * @param obj Response object.
     */
    public void setResponse(@Nullable Object obj) {
        this.obj = obj;
    }

    /**
     * @return Error.
     */
    public String getError() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void setError(String err) {
        this.err = err;
    }

    /**
     * @return Session token for remote client.
     */
    public byte[] sessionTokenBytes() {
        return sesTokBytes;
    }

    /**
     * @param sesTokBytes Session token for remote client.
     */
    public void sessionTokenBytes(@Nullable byte[] sesTokBytes) {
        this.sesTokBytes = sesTokBytes;
    }

    /**
     * @return String representation of session token.
     */
    public String getSessionToken() {
        return sesTokStr;
    }

    /**
     * @param sesTokStr String representation of session token.
     */
    public void setSessionToken(@Nullable String sesTokStr) {
        this.sesTokStr = sesTokStr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(successStatus);
        U.writeByteArray(out, sesTokBytes);
        U.writeString(out, sesTokStr);
        U.writeString(out, err);
        out.writeObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        successStatus = in.readInt();
        sesTokBytes = U.readByteArray(in);
        sesTokStr = U.readString(in);
        err = U.readString(in);
        obj = in.readObject();
    }
}