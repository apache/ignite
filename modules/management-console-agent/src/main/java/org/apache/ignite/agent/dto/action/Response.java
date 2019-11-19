/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.action;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for response
 */
public class Response {
    /** Response id. */
    private UUID id;

    /** Result. */
    private Object res;

    /** Error. */
    private ResponseError error;

    /** Status. */
    private ActionStatus status;

    /** Timestamp. */
    private long ts = System.currentTimeMillis();

    /**
     * @return Response id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Response id.
     * @return {@code This} for chaining method calls.
     */
    public Response setId(UUID id) {
        this.id = id;

        return this;
    }

    /**
     * @return Action result.
     */
    public Object getResult() {
        return res;
    }

    /**
     * @param res Response.
     * @return {@code This} for chaining method calls.
     */
    public Response setResult(Object res) {
        this.res = res;

        return this;
    }

    /**
     * @return Response error.
     */
    public ResponseError getError() {
        return error;
    }

    /**
     * @param error Response error.
     * @return {@code This} for chaining method calls.
     */
    public Response setError(ResponseError error) {
        this.error = error;

        return this;
    }

    /**
     * @return Action status.
     */
    public ActionStatus getStatus() {
        return status;
    }

    /**
     * @param status Action status.
     * @return {@code This} for chaining method calls.
     */
    public Response setStatus(ActionStatus status) {
        this.status = status;

        return this;
    }

    /**
     * @return Timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @param ts Timestamp.
     */
    public Response setTimestamp(long ts) {
        this.ts = ts;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Response.class, this);
    }
}
