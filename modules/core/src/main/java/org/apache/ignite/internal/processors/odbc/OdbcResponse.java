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
package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC protocol response.
 */
public class OdbcResponse {
    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    private final int status;

    /** Error. */
    private final String err;

    /** Response object. */
    @GridToStringInclude
    private final Object obj;

    /**
     * Constructs successful rest response.
     *
     * @param obj Response object.
     */
    public OdbcResponse(Object obj) {
        this.status = STATUS_SUCCESS;

        this.obj = obj;
        this.err = null;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public OdbcResponse(int status, @Nullable String err) {
        assert status != STATUS_SUCCESS;

        this.status = status;

        this.obj = null;
        this.err = err;
    }

    /**
     * @return Success flag.
     */
    public int status() {
        return status;
    }

    /**
     * @return Response object.
     */
    public Object response() {
        return obj;
    }

    /**
     * @return Error.
     */
    public String error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcResponse.class, this);
    }
}
