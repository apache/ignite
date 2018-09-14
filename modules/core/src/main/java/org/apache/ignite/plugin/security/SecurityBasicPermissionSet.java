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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.security.SecurityUtils.compatibleServicePermissions;
import static org.apache.ignite.internal.processors.security.SecurityUtils.isSecurityCompatibilityMode;
import static org.apache.ignite.internal.processors.security.SecurityUtils.serializeVersion;

/**
 * Simple implementation of {@link SecurityPermissionSet} interface.
 * Provides convenient way to specify permission set in the XML configuration.
 */
public class SecurityBasicPermissionSet implements SecurityPermissionSet {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cache permissions. */
    @GridToStringInclude
    private Map<String, Collection<SecurityPermission>> cachePermissions = new HashMap<>();

    /** Task permissions. */
    @GridToStringInclude
    private Map<String, Collection<SecurityPermission>> taskPermissions = new HashMap<>();

    /** Service permissions. */
    @GridToStringInclude
    private transient Map<String, Collection<SecurityPermission>> servicePermissions = isSecurityCompatibilityMode()
            ? compatibleServicePermissions()
            : new HashMap<String, Collection<SecurityPermission>>();

    /** System permissions. */
    @GridToStringInclude
    private Collection<SecurityPermission> systemPermissions;

    /** Default allow all. */
    private boolean dfltAllowAll;

    /**
     * Setter for set cache permission map.
     *
     * @param cachePermissions Cache permissions.
     */
    public void setCachePermissions(Map<String, Collection<SecurityPermission>> cachePermissions) {
        A.notNull(cachePermissions, "cachePermissions");

        this.cachePermissions = cachePermissions;
    }

    /**
     * Setter for set task permission map.
     *
     * @param taskPermissions Task permissions.
     */
    public void setTaskPermissions(Map<String, Collection<SecurityPermission>> taskPermissions) {
        A.notNull(taskPermissions, "taskPermissions");

        this.taskPermissions = taskPermissions;
    }

    /**
     * Setter for set service permission map.
     *
     * @param servicePermissions Service permissions.
     */
    public void setServicePermissions(Map<String, Collection<SecurityPermission>> servicePermissions) {
        A.notNull(taskPermissions, "servicePermissions");

        this.servicePermissions = servicePermissions;
    }

    /**
     * Setter for set collection system permission.
     *
     * @param systemPermissions System permissions.
     */
    public void setSystemPermissions(Collection<SecurityPermission> systemPermissions) {
        this.systemPermissions = systemPermissions;
    }

    /**
     * Setter for set default allow all.
     *
     * @param dfltAllowAll Default allow all.
     */
    public void setDefaultAllowAll(boolean dfltAllowAll) {
        this.dfltAllowAll = dfltAllowAll;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> cachePermissions() {
        return cachePermissions;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
        return taskPermissions;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> servicePermissions() {
        return servicePermissions;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
        return systemPermissions;
    }

    /** {@inheritDoc} */
    @Override public boolean defaultAllowAll() {
        return dfltAllowAll;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof SecurityBasicPermissionSet))
            return false;

        SecurityBasicPermissionSet other = (SecurityBasicPermissionSet)o;

        return dfltAllowAll == other.dfltAllowAll &&
            F.eq(cachePermissions, other.cachePermissions) &&
            F.eq(taskPermissions, other.taskPermissions) &&
            F.eq(servicePermissions, other.servicePermissions) &&
            F.eq(systemPermissions, other.systemPermissions);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (dfltAllowAll ? 1 : 0);

        res = 31 * res + (cachePermissions != null ? cachePermissions.hashCode() : 0);
        res = 31 * res + (taskPermissions != null ? taskPermissions.hashCode() : 0);
        res = 31 * res + (servicePermissions != null ? servicePermissions.hashCode() : 0);
        res = 31 * res + (systemPermissions != null ? systemPermissions.hashCode() : 0);

        return res;
    }

    /**
     * @param out Out.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        if (serializeVersion() >= 2)
            U.writeMap(out, servicePermissions);
    }

    /**
     * @param in In.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (serializeVersion() >= 2)
            servicePermissions = U.readMap(in);

        if (servicePermissions == null) {
            // Allow all for compatibility mode
            if (serializeVersion() < 2)
                servicePermissions = compatibleServicePermissions();
            else
                servicePermissions = Collections.emptyMap();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecurityBasicPermissionSet.class, this);
    }
}
