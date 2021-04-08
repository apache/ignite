package org.apache.ignite.cluster;

import java.security.BasicPermission;

public class IgniteClusterPermission extends BasicPermission {

    private static final long serialVersionUID = 0L;

    public IgniteClusterPermission(String name) {
        super(name);
    }
}
