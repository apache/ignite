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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
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
    @Order(0)
    Map<String, Set<SecurityPermission>> cachePermissions = new HashMap<>();

    /** Task permissions. */
    @GridToStringInclude
    @Order(1)
    Map<String, Set<SecurityPermission>> taskPermissions = new HashMap<>();

    /** Service permissions. */
    @GridToStringInclude
    @Order(2)
    transient Map<String, Set<SecurityPermission>> srvcPermissions = isSecurityCompatibilityMode()
            ? compatibleServicePermissions()
            : new HashMap<>();

    /** System permissions. */
    @GridToStringInclude
    @Order(3)
    Set<SecurityPermission> sysPermissions;

    /** Default allow all. */
    @Order(4)
    boolean dfltAllowAll;

    /**
     * Setter for set cache permission map.
     *
     * @param cachePermissions Cache permissions.
     */
    public void setCachePermissions(Map<String, Collection<SecurityPermission>> cachePermissions) {
        A.notNull(cachePermissions, "cachePermissions");

        this.cachePermissions = toHashSetMap(cachePermissions);
    }

    /**
     * Setter for set task permission map.
     *
     * @param taskPermissions Task permissions.
     */
    public void setTaskPermissions(Map<String, Collection<SecurityPermission>> taskPermissions) {
        A.notNull(taskPermissions, "taskPermissions");

        this.taskPermissions = toHashSetMap(taskPermissions);
    }

    /**
     * Setter for set service permission map.
     *
     * @param srvcPermissions Service permissions.
     */
    public void setServicePermissions(Map<String, Collection<SecurityPermission>> srvcPermissions) {
        A.notNull(taskPermissions, "servicePermissions");

        this.srvcPermissions = toHashSetMap(srvcPermissions);
    }

    /**
     * Copies content to a form with indemponent `hashCode` and `equals` results.
     *
     * @param cachePermissions Cache permissions.
     * @return Map with hash set of security permissions.
     */
    private Map<String, Set<SecurityPermission>> toHashSetMap(Map<String, Collection<SecurityPermission>> cachePermissions) {
        return cachePermissions.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> toHashSet(e.getValue())));
    }

    /** @return Hash set with permissions. */
    private Set<SecurityPermission> toHashSet(Collection<SecurityPermission> col) {
        return col instanceof HashSet<SecurityPermission> ? (HashSet<SecurityPermission>)col : new HashSet<>(col);
    }

    /**
     * Setter for set collection system permission.
     *
     * @param sysPermissions System permissions.
     */
    public void setSystemPermissions(Collection<SecurityPermission> sysPermissions) {
        this.sysPermissions = new HashSet<>(sysPermissions);
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
    @Override public Map<String, ? extends Collection<SecurityPermission>> cachePermissions() {
        return cachePermissions;
    }

    /** {@inheritDoc} */
    @Override public Map<String, ? extends Collection<SecurityPermission>> taskPermissions() {
        return taskPermissions;
    }

    /** {@inheritDoc} */
    @Override public Map<String, ? extends Collection<SecurityPermission>> servicePermissions() {
        return srvcPermissions;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
        return sysPermissions;
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
            Objects.equals(cachePermissions, other.cachePermissions) &&
            Objects.equals(taskPermissions, other.taskPermissions) &&
            Objects.equals(srvcPermissions, other.srvcPermissions) &&
            Objects.equals(sysPermissions, other.sysPermissions);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (dfltAllowAll ? 1 : 0);

        res = 31 * res + (cachePermissions != null ? cachePermissions.hashCode() : 0);
        res = 31 * res + (taskPermissions != null ? taskPermissions.hashCode() : 0);
        res = 31 * res + (srvcPermissions != null ? srvcPermissions.hashCode() : 0);
        res = 31 * res + (sysPermissions != null ? sysPermissions.hashCode() : 0);

        return res;
    }

    /**
     * @param out Out.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        if (serializeVersion() >= 2)
            U.writeMap(out, srvcPermissions);
    }

    /**
     * @param in In.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (serializeVersion() >= 2)
            srvcPermissions = U.readMap(in);

        if (srvcPermissions == null) {
            // Allow all for compatibility mode
            if (serializeVersion() < 2)
                srvcPermissions = compatibleServicePermissions();
            else
                srvcPermissions = Collections.emptyMap();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecurityBasicPermissionSet.class, this);
    }
}
