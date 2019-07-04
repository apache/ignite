package org.apache.ignite.internal.processors.security;

import java.security.Principal;
import java.util.Objects;

public class IgnitePrincipal implements Principal {

    private final String name;

    public IgnitePrincipal(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override public String getName() {
        return name;
    }

    @Override public int hashCode() {
        return name.hashCode();
    }

    @Override public boolean equals(Object obj) {
        if (obj instanceof IgnitePrincipal) {
            IgnitePrincipal other = (IgnitePrincipal)obj;

            return name.equals(other.name);
        }

        return false;
    }
}
