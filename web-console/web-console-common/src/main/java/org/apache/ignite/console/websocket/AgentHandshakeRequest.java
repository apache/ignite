

package org.apache.ignite.console.websocket;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;

/**
 * Handshake request from Web Console Agent.
 */
public class AgentHandshakeRequest {
    /** Version 8.8.0 Initial version. */
    public static final String VER_8_8_0 = "8.8.0";

    /** Current version. */
    public static final String CURRENT_VER = VER_8_8_0;

    /** Supported versions. */
    public static final Set<String> SUPPORTED_VERS = of(CURRENT_VER).collect(toSet());

    /** */
    private String ver;

    /** */
    @GridToStringInclude
    private Set<String> toks;

    /**
     * Default constructor for serialization.
     */
    public AgentHandshakeRequest() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param ver Agent version.
     * @param toks Tokens.
     */
    public AgentHandshakeRequest(
        String ver,
        Collection<String> toks
    ) {
        this.ver = ver;
        this.toks = new HashSet<>(toks);
    }

    /**
     * @return Agent version.
     */
    public String getVersion() {
        return ver;
    }

    /**
     * @param ver Agent version.
     */
    public void setVersion(String ver) {
        this.ver = ver;
    }

    /**
     * @return Tokens.
     */
    public Set<String> getTokens() {
        return toks;
    }

    /**
     * @param toks Tokens.
     */
    public void setTokens(Set<String> toks) {
        this.toks = toks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AgentHandshakeRequest.class, this);
    }
}
