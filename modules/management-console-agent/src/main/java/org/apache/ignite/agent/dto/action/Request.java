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
 * DTO for action request.
 */
public class Request {
    /** Request id. */
    private UUID id;

    /** Argument. */
    private Object arg;

    /** Action name. */
    private String act;

    /** Timestamp. */
    private long ts;

    /** Session ID. */
    private UUID sesId;

    /**
     * @return Request id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Request id.
     * @return {@code This} for chaining method calls.
     */
    public Request setId(UUID id) {
        this.id = id;

        return this;
    }

    /**
     * @return Action name.
     */
    public String getAction() {
        return act;
    }

    /**
     * @param act Action name.
     * @return {@code This} for chaining method calls.
     */
    public Request setAction(String act) {
        this.act = act;

        return this;
    }

    /**
     * @return Action argument.
     */
    public Object getArgument() {
        return arg;
    }

    /**
     * @param arg Action argument.
     * @return {@code This} for chaining method calls.
     */
    public Request setArgument(Object arg) {
        this.arg = arg;

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
    public Request setTimestamp(long ts) {
        this.ts = ts;

        return this;
    }

    /**
     * @return Session ID.
     */
    public UUID getSessionId() {
        return sesId;
    }

    /**
     * @param sesId Session id.
     */
    public Request setSessionId(UUID sesId) {
        this.sesId = sesId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Request.class, this);
    }
}
