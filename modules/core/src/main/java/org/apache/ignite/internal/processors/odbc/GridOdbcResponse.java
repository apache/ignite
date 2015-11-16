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
public class GridOdbcResponse {

    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int successStatus = STATUS_SUCCESS;

    /** Error. */
    private String err;

    /** Response object. */
    @GridToStringInclude
    private Object obj;

    /**
     * Constructs successful rest response.
     *
     * @param obj Response object.
     */
    public GridOdbcResponse(Object obj) {
        successStatus = STATUS_SUCCESS;
        this.obj = obj;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public GridOdbcResponse(int status, @Nullable String err) {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridOdbcResponse.class, this);
    }
}
