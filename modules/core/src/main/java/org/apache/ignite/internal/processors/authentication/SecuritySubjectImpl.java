package org.apache.ignite.internal.processors.authentication;

import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.jetbrains.annotations.Nullable;

/**
 * Represents {@link SecuritySubject} implementation.
 */
public class SecuritySubjectImpl implements SecuritySubject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Security subject identifier. */
    @Order(0)
    UUID id;

    /** Security subject login. Is null if {@link SecuritySubjectImpl} is used as {@link Message}. */
    private @Nullable String login;

    /** Security subject type. Is null if {@link SecuritySubjectImpl} is used as {@link Message}. */
    private @Nullable SecuritySubjectType type;

    /** Security subject address. Is null if {@link SecuritySubjectImpl} is used as {@link Message}. */
    private @Nullable InetSocketAddress addr;

    /** Empty constructor for serialization purposes. */
    public SecuritySubjectImpl() {
        // No-op.
    }

    /** */
    public SecuritySubjectImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
        this.id = id;
        this.login = login;
        this.type = type;
        this.addr = addr;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String login() {
        return login;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubjectType type() {
        return type;
    }

    /** @inheritDoc} */
    @Override public InetSocketAddress address() {
        return addr;
    }

    /**{@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecuritySubjectImpl.class, this);
    }
}
