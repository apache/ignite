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

package org.apache.ignite.internal.processors.security.permission;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public abstract class ActionPermissionCollection<T extends ActionPermission> extends PermissionCollection {

    /**
     * Key is task name; value is ActionPermission.
     */
    private Map<String, T> perms;

    /**
     * Boolean saying if "*" is in the collection.
     */
    private boolean allAllowed;

    protected ActionPermissionCollection() {
        perms = new HashMap<>(32);

        allAllowed = false;
    }

    protected abstract T newPermission(String name, String actions);

    protected abstract boolean checkPermissionType(Permission permission);

    @Override public final void add(Permission permission) {
        if (!checkPermissionType(permission))
            throw new IllegalArgumentException("Invalid permission: " + permission);

        if (isReadOnly())
            throw new SecurityException("Attempt to add a Permission to a readonly PermissionCollection");

        T tp = (T)permission;

        String name = tp.getName();

        synchronized (this) {
            T existing = perms.get(name);

            if (existing != null) {
                int oldMask = existing.mask();
                int newMask = tp.mask();

                if (oldMask != newMask) {
                    String actions = mergeActions(existing, tp);

                    perms.put(name, newPermission(name, actions));
                }
            }
            else
                perms.put(name, tp);
        }

        if (!allAllowed)
            allAllowed = "*".equals(name);
    }

    private String mergeActions(T a, T b) {
        String aActions = a.getActions();
        String bActions = b.getActions();

        if (aActions.isEmpty())
            return bActions;

        if (bActions.isEmpty())
            return aActions;

        return aActions + ',' + bActions;
    }

    @Override public final boolean implies(Permission permission) {
        if (!checkPermissionType(permission))
            return false;

        T tp = (T)permission;

        int desired = tp.mask();
        int effective = 0;

        T x;
        // short circuit if the "*" Permission was added
        if (allAllowed) {

            synchronized (this) {
                x = perms.get("*");
            }

            if (x != null) {
                effective |= x.mask();

                if ((effective & desired) == desired)
                    return true;
            }
        }

        String name = tp.getName();

        synchronized (this) {
            x = perms.get(name);
        }

        if (x != null) {
            effective |= x.mask();

            if ((effective & desired) == desired)
                return true;
        }

        // work our way up the tree...
        int last, offset;

        offset = name.length() - 1;

        while ((last = name.lastIndexOf('.', offset)) != -1) {

            name = name.substring(0, last + 1) + "*";

            synchronized (this) {
                x = perms.get(name);
            }

            if (x != null) {
                effective |= x.mask();

                if ((effective & desired) == desired)
                    return true;
            }

            offset = last - 1;
        }

        // we don't have to check for "*" as it was already checked
        // at the top (all_allowed), so we just return false
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override public Enumeration<Permission> elements() {
        synchronized (this) {
            /**
             * Casting to rawtype since Enumeration<PropertyPermission>
             * cannot be directly cast to Enumeration<Permission>
             */
            return (Enumeration)Collections.enumeration(perms.values());
        }
    }
}
