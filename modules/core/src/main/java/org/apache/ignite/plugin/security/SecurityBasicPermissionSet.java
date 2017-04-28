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

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Simple implementation of {@link SecurityPermissionSet} interface. Provides
 * convenient way to specify permission set in the XML configuration.
 */
public class SecurityBasicPermissionSet implements SecurityPermissionSet {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cache permissions. */
    private Map<String, Collection<SecurityPermission>> cachePerms = new HashMap<>();

    /** Task permissions. */
    private Map<String, Collection<SecurityPermission>> taskPerms = new HashMap<>();

    /** Service permissions. */
    private Map<String, Collection<SecurityPermission>> srvcPerms = new HashMap<>();

    /** System permissions. */
    private Collection<SecurityPermission> sysPerms = new ArrayList<>();

    /** Default allow all. */
    private boolean dfltAllowAll;

    /**
     * Setter for set cache permission map.
     *
     * @param cachePerms Cache permissions.
     */
    public void setCachePermissions(Map<String, Collection<SecurityPermission>> cachePerms) {
        this.cachePerms = cachePerms;
    }

    /**
     * Setter for set task permission map.
     *
     * @param taskPerms Task permissions.
     */
    public void setTaskPermissions(Map<String, Collection<SecurityPermission>> taskPerms) {
        this.taskPerms = taskPerms;
    }

    /**
     * Setter for set service permission map.
     *
     * @param srvcPerms Service permissions.
     */
    public void setServicePermissions(Map<String, Collection<SecurityPermission>> srvcPerms) {
        this.srvcPerms = srvcPerms;
    }

    /**
     * Setter for set collection  system permission.
     *
     * @param sysPerms System permissions.
     */
    public void setSystemPermissions(Collection<SecurityPermission> sysPerms) {
        this.sysPerms = sysPerms;
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
        return cachePerms;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
        return taskPerms;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> servicePermissions() {
        return srvcPerms;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
        return sysPerms;
    }

    /** {@inheritDoc} */
    @Override public boolean defaultAllowAll() {
        return dfltAllowAll;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecurityBasicPermissionSet.class, this);
    }
}
