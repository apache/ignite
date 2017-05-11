/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.plugin.security;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class SecurityBasicPermissionSetV2 extends SecurityBasicPermissionSet implements SecurityPermissionSetV2 {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Service permissions. */
    @GridToStringInclude
    private Map<String, Collection<SecurityPermission>> srvcPerms = new HashMap<>();

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> servicePermissions() {
        return srvcPerms;
    }

    /** {@inheritDoc} */
    @Override public SecurityPermissionSet permissionsV1() {
        SecurityBasicPermissionSet perms = new SecurityBasicPermissionSet();

        perms.setDefaultAllowAll(defaultAllowAll());
        perms.setTaskPermissions(taskPermissions());
        perms.setCachePermissions(cachePermissions());
        perms.setSystemPermissions(systemPermissions());

        return perms;
    }

    /**
     * Setter for set service permission map.
     *
     * @param srvcPerms Service permissions.
     */
    public void setServicePermissions(Map<String, Collection<SecurityPermission>> srvcPerms) {
        A.notNull(this.srvcPerms, "srvcPerms");

        this.srvcPerms = srvcPerms;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof SecurityBasicPermissionSetV2))
            return false;

        SecurityBasicPermissionSetV2 other = (SecurityBasicPermissionSetV2)o;

        return super.equals(other) && F.eq(srvcPerms, other.srvcPerms);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * super.hashCode() + (srvcPerms != null ? srvcPerms.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecurityBasicPermissionSetV2.class, this);
    }
}
