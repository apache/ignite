package org.apache.ignite.plugin.security;

import java.security.Permissions;

public interface IgniteSecurityContext {

    // о как должно быть. по идее наш subject - аналог принципала в JAAS
    // для одного и того же субъекта, по идее, может быть разный набор разрешений.

    public SecuritySubject subject();

    public Permissions permissions();

}
