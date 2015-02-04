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

package org.apache.ignite.internal.managers.security;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.security.*;

import java.io.*;
import java.util.*;

/**
 * Security context.
 */
public class GridSecurityContext implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Visor tasks prefix. */
    private static final String VISOR_TASK_PREFIX = "org.apache.ignite.internal.visor.";

    /** Cache query task name. */
    public static final String VISOR_CACHE_QUERY_TASK_NAME =
        "org.gridgain.grid.kernal.visor.cmd.tasks.VisorQueryTask";

    /** Cache load task name. */
    public static final String VISOR_CACHE_LOAD_TASK_NAME =
        "org.gridgain.grid.kernal.visor.gui.tasks.VisorCachesLoadTask";

    /** Cache clear task name. */
    public static final String VISOR_CACHE_CLEAR_TASK_NAME =
        "org.gridgain.grid.kernal.visor.cmd.tasks.VisorCachesClearTask";

    /** Security subject. */
    private GridSecuritySubject subj;

    /** String task permissions. */
    private Map<String, Collection<GridSecurityPermission>> strictTaskPermissions = new LinkedHashMap<>();

    /** String task permissions. */
    private Map<String, Collection<GridSecurityPermission>> wildcardTaskPermissions = new LinkedHashMap<>();

    /** String task permissions. */
    private Map<String, Collection<GridSecurityPermission>> strictCachePermissions = new LinkedHashMap<>();

    /** String task permissions. */
    private Map<String, Collection<GridSecurityPermission>> wildcardCachePermissions = new LinkedHashMap<>();

    /** System-wide permissions. */
    private Collection<GridSecurityPermission> sysPermissions;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridSecurityContext() {
        // No-op.
    }

    /**
     * @param subj Subject.
     */
    public GridSecurityContext(GridSecuritySubject subj) {
        this.subj = subj;

        initRules();
    }

    /**
     * @return Security subject.
     */
    public GridSecuritySubject subject() {
        return subj;
    }

    /**
     * Checks whether task operation is allowed.
     *
     * @param taskClsName Task class name.
     * @param perm Permission to check.
     * @return {@code True} if task operation is allowed.
     */
    public boolean taskOperationAllowed(String taskClsName, GridSecurityPermission perm) {
        assert perm == GridSecurityPermission.TASK_EXECUTE || perm == GridSecurityPermission.TASK_CANCEL;

        if (visorTask(taskClsName))
            return visorTaskAllowed(taskClsName);

        Collection<GridSecurityPermission> p = strictTaskPermissions.get(taskClsName);

        if (p != null)
            return p.contains(perm);

        for (Map.Entry<String, Collection<GridSecurityPermission>> entry : wildcardTaskPermissions.entrySet()) {
            if (taskClsName.startsWith(entry.getKey()))
                return entry.getValue().contains(perm);
        }

        return subj.permissions().defaultAllowAll();
    }

    /**
     * Checks whether cache operation is allowed.
     *
     * @param cacheName Cache name.
     * @param perm Permission to check.
     * @return {@code True} if cache operation is allowed.
     */
    public boolean cacheOperationAllowed(String cacheName, GridSecurityPermission perm) {
        assert perm == GridSecurityPermission.CACHE_PUT || perm == GridSecurityPermission.CACHE_READ ||
            perm == GridSecurityPermission.CACHE_REMOVE;

        Collection<GridSecurityPermission> p = strictCachePermissions.get(cacheName);

        if (p != null)
            return p.contains(perm);

        for (Map.Entry<String, Collection<GridSecurityPermission>> entry : wildcardCachePermissions.entrySet()) {
            if (cacheName != null) {
                if (cacheName.startsWith(entry.getKey()))
                    return entry.getValue().contains(perm);
            }
            else {
                // Match null cache to '*'
                if (entry.getKey().isEmpty())
                    return entry.getValue().contains(perm);
            }
        }

        return subj.permissions().defaultAllowAll();
    }

    /**
     * Checks whether system-wide permission is allowed (excluding Visor task operations).
     *
     * @param perm Permission to check.
     * @return {@code True} if system operation is allowed.
     */
    public boolean systemOperationAllowed(GridSecurityPermission perm) {
        if (sysPermissions == null)
            return subj.permissions().defaultAllowAll();

        boolean ret = sysPermissions.contains(perm);

        if (!ret && (perm == GridSecurityPermission.EVENTS_ENABLE || perm == GridSecurityPermission.EVENTS_DISABLE))
            ret = sysPermissions.contains(GridSecurityPermission.ADMIN_VIEW);

        return ret;
    }

    /**
     * Checks if task is Visor task.
     *
     * @param taskCls Task class name.
     * @return {@code True} if task is Visor task.
     */
    private boolean visorTask(String taskCls) {
        return taskCls.startsWith(VISOR_TASK_PREFIX);
    }

    /**
     * Checks if Visor task is allowed for execution.
     *
     * @param taskName Task name.
     * @return {@code True} if execution is allowed.
     */
    private boolean visorTaskAllowed(String taskName) {
        if (sysPermissions == null)
            return subj.permissions().defaultAllowAll();

        switch (taskName) {
            case VISOR_CACHE_QUERY_TASK_NAME:
                return sysPermissions.contains(GridSecurityPermission.ADMIN_QUERY);
            case VISOR_CACHE_LOAD_TASK_NAME:
            case VISOR_CACHE_CLEAR_TASK_NAME:
                return sysPermissions.contains(GridSecurityPermission.ADMIN_CACHE);
            default:
                return sysPermissions.contains(GridSecurityPermission.ADMIN_VIEW);
        }
    }

    /**
     * Init rules.
     */
    private void initRules() {
        GridSecurityPermissionSet permSet = subj.permissions();

        for (Map.Entry<String, Collection<GridSecurityPermission>> entry : permSet.taskPermissions().entrySet()) {
            String ptrn = entry.getKey();

            Collection<GridSecurityPermission> vals = Collections.unmodifiableCollection(entry.getValue());

            if (ptrn.endsWith("*")) {
                String noWildcard = ptrn.substring(0, ptrn.length() - 1);

                wildcardTaskPermissions.put(noWildcard, vals);
            }
            else
                strictTaskPermissions.put(ptrn, vals);
        }

        for (Map.Entry<String, Collection<GridSecurityPermission>> entry : permSet.cachePermissions().entrySet()) {
            String ptrn = entry.getKey();

            Collection<GridSecurityPermission> vals = Collections.unmodifiableCollection(entry.getValue());

            if (ptrn != null && ptrn.endsWith("*")) {
                String noWildcard = ptrn.substring(0, ptrn.length() - 1);

                wildcardCachePermissions.put(noWildcard, vals);
            }
            else
                strictCachePermissions.put(ptrn, vals);
        }

        sysPermissions = permSet.systemPermissions();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(subj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        subj = (GridSecuritySubject)in.readObject();

        initRules();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSecurityContext.class, this);
    }
}
