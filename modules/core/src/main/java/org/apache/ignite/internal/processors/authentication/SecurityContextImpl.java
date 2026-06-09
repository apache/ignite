package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;

/** Represents {@link SecurityContext} implementation that ignores any security permission checks. */
public class SecurityContextImpl implements SecurityContext, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    SecuritySubjectImpl subj;

    /** Empty constructor for serialization purposes. */
    public SecurityContextImpl() {
        // No-op.
    }

    /** */
    public SecurityContextImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
        subj = new SecuritySubjectImpl(id, login, type, addr);
    }

    /** Creates {@link Message} of {@code context}. */
    public static SecurityContextImpl message(SecurityContext ctx) {
        assert ctx instanceof SecurityContextImpl;

        return (SecurityContextImpl)ctx;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject subject() {
        return subj;
    }
}
