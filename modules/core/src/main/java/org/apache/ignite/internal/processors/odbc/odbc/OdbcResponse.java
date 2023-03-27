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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener response.
 */
public class OdbcResponse extends ClientListenerResponse {
    /** Response object. */
    @GridToStringInclude
    private final Object obj;

    /**
     * Constructs successful rest response.
     *
     * @param obj Response object.
     */
    public OdbcResponse(Object obj) {
        super(STATUS_SUCCESS, null);

        this.obj = obj;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public OdbcResponse(int status, @Nullable String err) {
        super(status, err);

        assert status != STATUS_SUCCESS;

        obj = null;
    }

    /**
     * @return Response object.
     */
    public Object response() {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcResponse.class, this);
    }
}
