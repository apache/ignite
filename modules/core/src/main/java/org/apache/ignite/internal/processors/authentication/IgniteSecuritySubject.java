package org.apache.ignite.internal.processors.authentication;

import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;

/**
 * {@link SecuritySubject} implementation that contains description of Ignite node/remote client
 * and associated security data.
 */
public class IgniteSecuritySubject implements SecuritySubject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Id. */
    private final UUID id;

    /** Type. */
    private final SecuritySubjectType type;

    /** Login. */
    private final Object login;

    /** Address. */
    private final InetSocketAddress addr;

    /** User permissions. */
    private final SecurityPermissionSet perms;

    /**
     * @param id Id.
     * @param type Type.
     * @param login Login.
     * @param addr Address.
     * @param perms Permissions.
     */
    public IgniteSecuritySubject(
        UUID id,
        SecuritySubjectType type,
        Object login,
        InetSocketAddress addr,
        SecurityPermissionSet perms
    ) {
        this.id = id;
        this.type = type;
        this.login = login;
        this.addr = addr;
        this.perms = perms;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubjectType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public Object login() {
        return login;
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress address() {
        return addr;
    }

    /** {@inheritDoc} */
    @Override public SecurityPermissionSet permissions() {
        return perms;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSecuritySubject.class, this);
    }
}
