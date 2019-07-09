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

package org.apache.ignite.internal.processors.security.adapter;

import java.security.Permission;
import java.security.Permissions;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.permission.CachePermission;
import org.apache.ignite.internal.processors.security.SecurityConstants;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.service.permission.ServicePermission;
import org.apache.ignite.internal.processors.task.permission.TaskPermission;
import org.apache.ignite.plugin.security.IgniteSecurityContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;

public class IgniteSecurityContextAdapter implements IgniteSecurityContext {

    private final SecurityContext origin;

    private Permissions perms;

    public IgniteSecurityContextAdapter(SecurityContext origin) {
        this.origin = origin;

        init();
    }

    @Override public SecuritySubject subject() {
        return origin.subject();
    }

    @Override public Permissions permissions() {
        return perms;
    }

    private void init() {
        perms = new Permissions();

        SecurityPermissionSet permSet = origin.subject().permissions();

        addPermissions(CachePermission::new, permSet.cachePermissions());
        addPermissions(TaskPermission::new, permSet.taskPermissions());
        addPermissions(ServicePermission::new, permSet.servicePermissions());

        for (SecurityPermission sp : permSet.systemPermissions())
            perms.add(systemPermission(sp));

        if (permSet.defaultAllowAll()) {
            if (permSet.cachePermissions().isEmpty())
                perms.add(new CachePermission("*", "get,put,remove"));

            if (permSet.taskPermissions().isEmpty())
                perms.add(new TaskPermission("*", "*"));
        }

        perms.setReadOnly();
    }

    private void addPermissions(BiFunction<String, String, Permission> f,
        Map<String, Collection<SecurityPermission>> secPerms) {
        for (Map.Entry<String, Collection<SecurityPermission>> entry : secPerms.entrySet()) {
            String actions = actions(entry.getValue());

            if (!actions.isEmpty())
                perms.add(f.apply(entry.getKey(), actions));
        }
    }

    private String actions(Collection<SecurityPermission> secPerms) {
        StringBuilder sb = new StringBuilder();

        for (SecurityPermission p : secPerms)
            sb.append(securityPermissionToAction(p)).append(',');

        String res = sb.toString();

        return res.isEmpty() ? res : res.substring(0, res.length() - 2);
    }

    private Permission systemPermission(SecurityPermission sp) {
        switch (sp) {
            case EVENTS_DISABLE:
                return SecurityConstants.EVENTS_DISABLE_PERMISSION;

            case EVENTS_ENABLE:
                return SecurityConstants.EVENTS_ENABLE_PERMISSION;

            case ADMIN_VIEW:
                return SecurityConstants.VISOR_ADMIN_VIEW_PERMISSION;

            case ADMIN_CACHE:
                return SecurityConstants.VISOR_ADMIN_CACHE_PERMISSION;

            case ADMIN_QUERY:
                return SecurityConstants.VISOR_ADMIN_QUERY_PERMISSION;

            case ADMIN_OPS:
                return SecurityConstants.VISOR_ADMIN_OPS_PERMISSION;

            case CACHE_CREATE:
                return new CachePermission("*", CachePermission.CREATE);

            case CACHE_DESTROY:
                return new CachePermission("*", CachePermission.DESTROY);

            case JOIN_AS_SERVER:
                return SecurityConstants.JOIN_AS_SERVER_PERMISSION;

            default:
                throw new IllegalStateException("Invalid security permission: " + sp);
        }
    }

    private String securityPermissionToAction(SecurityPermission sp) {
        switch (sp) {
            case CACHE_PUT:
                return CachePermission.PUT;

            case CACHE_READ:
                return CachePermission.GET;

            case CACHE_REMOVE:
                return CachePermission.REMOVE;

            case TASK_CANCEL:
                return TaskPermission.CANCEL;

            case TASK_EXECUTE:
                return TaskPermission.EXECUTE;

            case SERVICE_DEPLOY:
                return ServicePermission.DEPLOY;

            case SERVICE_INVOKE:
                return ServicePermission.INVOKE;

            case SERVICE_CANCEL:
                return ServicePermission.CANCEL;

            default:
                throw new IllegalStateException("Invalid security permission: " + sp);
        }
    }
}