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

package org.apache.ignite.console.websocket;

import java.util.Collection;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Handshake response for Web Console Agent.
 */
public class AgentHandshakeResponse {
    /** */
    private String err;

    /** */
    @GridToStringInclude
    private Set<String> tokens;

    /**
     * Constructor for deserialization.
     *
     * @param err Optional error message.
     * @param tokens Tokens.
     */
    @JsonCreator
    private AgentHandshakeResponse(@JsonProperty("error") String err, @JsonProperty("tokens") Set<String> tokens) {
        this.err = err;
        this.tokens = tokens;
    }

    /**
     * @param err Error to take message from.
     */
    public AgentHandshakeResponse(Throwable err) {
        this(err.getMessage(), null);
    }

    /**
     * @param toks Tokens.
     */
    public AgentHandshakeResponse(Set<String> toks) {
        this(null, toks);
    }

    /**
     * @return Error message.
     */
    public String getError() {
        return err;
    }

    /**
     * @return Tokens.
     */
    public Collection<String> getTokens() {
        return tokens;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AgentHandshakeResponse.class, this);
    }
}
