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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * Provides a convenient way to create a permission set.
 * <p>
 * Here is example:
 * <pre>
 *      SecurityPermissionSet permsSet = new SecurityPermissionSetBuilder()
 *          .appendCachePermissions("cache1", CACHE_PUT, CACHE_REMOVE)
 *          .appendCachePermissions("cache2", CACHE_READ)
 *          .appendTaskPermissions("task1", TASK_CANCEL)
 *          .appendTaskPermissions("task2", TASK_EXECUTE)
 *          .appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE)
 *          .build();
 * </pre>
 * <p>
 * The builder also does additional validation. For example, if you try to
 * append {@code EVENTS_ENABLE} permission for a cache, exception will be thrown:
 * <pre>
 *      SecurityPermissionSet permsSet = new SecurityPermissionSetBuilder()
 *          .appendCachePermissions("cache1", EVENTS_ENABLE)
 *          .build();
 * </pre>
 */
public class SecurityPermissionSetBuilder {
    /** Cache permissions.*/
    private Map<String, Collection<SecurityPermission>> cachePerms = new HashMap<>();

    /** Task permissions.*/
    private Map<String, Collection<SecurityPermission>> taskPerms = new HashMap<>();

    /** Service permissions.*/
    private Map<String, Collection<SecurityPermission>> srvcPerms = new HashMap<>();

    /** System permissions.*/
    private Set<SecurityPermission> sysPerms = new HashSet<>();

    /** Default allow all.*/
    private boolean dfltAllowAll;

    /** */
    public static final SecurityPermissionSet ALLOW_ALL = create().build();

    /**
     * Static factory method for create new permission builder.
     *
     * @return SecurityPermissionSetBuilder
     */
    public static SecurityPermissionSetBuilder create() {
        return new SecurityPermissionSetBuilder().defaultAllowAll(true);
    }

    /**
     * Append default all flag.
     *
     * @param dfltAllowAll Default allow all.
     * @return SecurityPermissionSetBuilder refer to same permission builder.
     */
    public SecurityPermissionSetBuilder defaultAllowAll(boolean dfltAllowAll) {
        this.dfltAllowAll = dfltAllowAll;

        return this;
    }

    /**
     * Append permission set form {@link org.apache.ignite.IgniteCompute task} with {@code name}.
     *
     * @param name  String for map some task to permission set.
     * @param perms Permissions.
     * @return SecurityPermissionSetBuilder refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendTaskPermissions(String name, SecurityPermission... perms) {
        validate(toCollection("TASK_"), perms);

        append(taskPerms, name, toCollection(perms));

        return this;
    }

    /**
     * Append permission set form {@link org.apache.ignite.IgniteServices service} with {@code name}.
     *
     * @param name  String for map some service to permission set.
     * @param perms Permissions.
     * @return SecurityPermissionSetBuilder refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendServicePermissions(String name, SecurityPermission... perms) {
        validate(toCollection("SERVICE_"), perms);

        append(srvcPerms, name, toCollection(perms));

        return this;
    }

    /**
     * Append permission set form {@link org.apache.ignite.IgniteCache cache} with {@code name}.
     *
     * @param name  String for map some cache to permission set.
     * @param perms Permissions.
     * @return {@link SecurityPermissionSetBuilder} refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendCachePermissions(String name, SecurityPermission... perms) {
        validate(toCollection("CACHE_"), perms);

        append(cachePerms, name, toCollection(perms));

        return this;
    }

    /**
     * Append system permission set.
     *
     * @param perms Permission.
     * @return {@link SecurityPermissionSetBuilder} refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendSystemPermissions(SecurityPermission... perms) {
        validate(toCollection("EVENTS_", "ADMIN_", "CACHE_CREATE", "CACHE_DESTROY", "JOIN_AS_SERVER"), perms);

        sysPerms.addAll(toCollection(perms));

        return this;
    }

    /**
     * Validate method use patterns.
     *
     * @param ptrns Pattern.
     * @param perms Permissions.
     */
    private void validate(Collection<String> ptrns, SecurityPermission... perms) {
        assert ptrns != null;
        assert perms != null;

        for (SecurityPermission perm : perms)
            validate(ptrns, perm);
    }

    /**
     * @param ptrns Patterns.
     * @param perm  Permission.
     */
    private void validate(Collection<String> ptrns, SecurityPermission perm) {
        assert ptrns != null;
        assert perm != null;

        boolean ex = true;

        String name = perm.name();

        for (String ptrn : ptrns) {
            if (name.startsWith(ptrn)) {
                ex = false;

                break;
            }
        }

        if (ex)
            throw new IgniteException("you can assign permission only start with " + ptrns + ", but you try " + name);
    }

    /**
     * Convert vararg to {@link Collection}.
     *
     * @param perms Permissions.
     */
    @SafeVarargs
    private final <T> Collection<T> toCollection(T... perms) {
        assert perms != null;

        Collection<T> col = U.newLinkedHashSet(perms.length);

        Collections.addAll(col, perms);

        return col;
    }

    /**
     * @param permsMap Permissions map.
     * @param name Name.
     * @param perms Permission.
     */
    private void append(
        Map<String, Collection<SecurityPermission>> permsMap,
        String name,
        Collection<SecurityPermission> perms
    ) {
        assert permsMap != null;
        assert name != null;
        assert perms != null;

        Collection<SecurityPermission> col = permsMap.get(name);

        if (col == null)
            permsMap.put(name, perms);
        else
            col.addAll(perms);
    }

    /**
     * Builds the {@link SecurityPermissionSet}.
     *
     * @return {@link SecurityPermissionSet} instance.
     */
    public SecurityPermissionSet build() {
        SecurityBasicPermissionSet permSet = new SecurityBasicPermissionSet();

        permSet.setDefaultAllowAll(dfltAllowAll);
        permSet.setCachePermissions(unmodifiableMap(cachePerms));
        permSet.setTaskPermissions(unmodifiableMap(taskPerms));
        permSet.setServicePermissions(unmodifiableMap(srvcPerms));
        permSet.setSystemPermissions(unmodifiableSet(sysPerms));

        return permSet;
    }
}
