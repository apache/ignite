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
import java.io.ObjectStreamField;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.security.SecurityUtils.compatibleServicePermissions;
import static org.apache.ignite.internal.processors.security.SecurityUtils.isSecurityCompatibilityMode;
import static org.apache.ignite.internal.processors.security.SecurityUtils.normalizeResourcePermissions;
import static org.apache.ignite.internal.processors.security.SecurityUtils.serializeVersion;
import static org.apache.ignite.internal.processors.security.SecurityUtils.toEnumSet;

/**
 * Simple implementation of {@link SecurityPermissionSet} interface.
 * Provides convenient way to specify permission set in the XML configuration.
 */
public class SecurityBasicPermissionSet implements SecurityPermissionSet {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ObjectStreamField[] serialPersistentFields = {
        new ObjectStreamField("dfltAllowAll", boolean.class),
        new ObjectStreamField("cachePermissions", Map.class),
        new ObjectStreamField("sysPermissions", Collection.class),
        new ObjectStreamField("taskPermissions", Map.class)
    };

    /** Cache permissions. */
    @GridToStringInclude
    @Order(0)
    Map<String, EnumSet<SecurityPermission>> cachePermissions = new HashMap<>();

    /** Task permissions. */
    @GridToStringInclude
    @Order(1)
    Map<String, EnumSet<SecurityPermission>> taskPermissions = new HashMap<>();

    /** Service permissions. */
    @GridToStringInclude
    @Order(2)
    Map<String, EnumSet<SecurityPermission>> srvcPermissions = isSecurityCompatibilityMode()
        ? compatibleServicePermissions()
        : new HashMap<>();

    /** System permissions. */
    @GridToStringInclude
    @Order(3)
    @Nullable EnumSet<SecurityPermission> sysPermissions;

    /** Default allow all. */
    @Order(4)
    boolean dfltAllowAll;

    /**
     * Setter for set cache permission map.
     *
     * @param cachePermissions Cache permissions.
     */
    public void setCachePermissions(Map<String, EnumSet<SecurityPermission>> cachePermissions) {
        this.cachePermissions = checkPermissions(cachePermissions, "cachePermissions");
    }

    /**
     * Setter for set task permission map.
     *
     * @param taskPermissions Task permissions.
     */
    public void setTaskPermissions(Map<String, EnumSet<SecurityPermission>> taskPermissions) {
        this.taskPermissions = checkPermissions(taskPermissions, "taskPermissions");
    }

    /**
     * Setter for set service permission map.
     *
     * @param srvcPermissions Service permissions.
     */
    public void setServicePermissions(Map<String, EnumSet<SecurityPermission>> srvcPermissions) {
        this.srvcPermissions = checkPermissions(srvcPermissions, "servicePermissions");
    }

    /**
     * Setter for set collection system permission.
     *
     * @param sysPermissions System permissions.
     */
    public void setSystemPermissions(@Nullable EnumSet<SecurityPermission> sysPermissions) {
        this.sysPermissions = sysPermissions;
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
    @Override public Map<String, EnumSet<SecurityPermission>> cachePermissions() {
        return cachePermissions;
    }

    /** {@inheritDoc} */
    @Override public Map<String, EnumSet<SecurityPermission>> taskPermissions() {
        return taskPermissions;
    }

    /** {@inheritDoc} */
    @Override public Map<String, EnumSet<SecurityPermission>> servicePermissions() {
        return srvcPermissions;
    }

    /** {@inheritDoc} */
    @Nullable @Override public EnumSet<SecurityPermission> systemPermissions() {
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

    /** */
    private void writeObject(ObjectOutputStream out) throws IOException {
        ObjectOutputStream.PutField fields = out.putFields();

        fields.put("dfltAllowAll", dfltAllowAll);
        fields.put("cachePermissions", cachePermissions);
        fields.put("sysPermissions", sysPermissions);
        fields.put("taskPermissions", taskPermissions);

        out.writeFields();

        if (serializeVersion() >= 2)
            U.writeMap(out, srvcPermissions);
    }

    /** */
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        ObjectInputStream.GetField fields = in.readFields();

        dfltAllowAll = fields.get("dfltAllowAll", false);
        cachePermissions = readPermissions(fields, "cachePermissions");
        taskPermissions = readPermissions(fields, "taskPermissions");

        Collection<SecurityPermission> sysPerms = (Collection<SecurityPermission>)fields.get("sysPermissions", null);

        sysPermissions = sysPerms == null ? null : toEnumSet(sysPerms);

        Map<String, ? extends Collection<SecurityPermission>> srvcPerms = serializeVersion() >= 2 ? U.readMap(in) : null;

        if (srvcPerms == null) {
            // Allow all for compatibility mode
            srvcPerms = serializeVersion() < 2 ? compatibleServicePermissions() : Collections.emptyMap();
        }

        srvcPermissions = normalizeResourcePermissions(srvcPerms);
    }

    /**
     * @param fields Fields of the Java-serialized form.
     * @param name Field to read.
     * @return Permissions per resource name, empty if the field is absent from the stream.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, EnumSet<SecurityPermission>> readPermissions(ObjectInputStream.GetField fields,
        String name
    ) throws IOException {
        return normalizeResourcePermissions((Map<String, ? extends Collection<SecurityPermission>>)fields.get(name, null));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecurityBasicPermissionSet.class, this);
    }

    /** */
    private static Map<String, EnumSet<SecurityPermission>> checkPermissions(
        Map<String, EnumSet<SecurityPermission>> perms,
        String name
    ) {
        A.notNull(perms, name);
        A.ensure(perms.values().stream().noneMatch(Objects::isNull), name + " must not contain a null permission set");

        return perms;
    }
}
