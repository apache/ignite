

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
