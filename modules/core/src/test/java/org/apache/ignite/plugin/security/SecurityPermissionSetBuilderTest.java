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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_DEPLOY;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_INVOKE;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermission.EVENTS_ENABLE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test for check correct work {@link SecurityPermissionSetBuilder permission builder}
 */
public class SecurityPermissionSetBuilderTest extends GridCommonAbstractTest {
    /**
     *
     */
    @SuppressWarnings({"ThrowableNotThrown", "ArraysAsListWithZeroOrOneArgument"})
    public void testPermissionBuilder() {
        SecurityBasicPermissionSet exp = new SecurityBasicPermissionSet();

        Map<String, Collection<SecurityPermission>> permCache = new HashMap<>();
        permCache.put("cache1", Arrays.asList(CACHE_PUT, CACHE_REMOVE));
        permCache.put("cache2", Arrays.asList(CACHE_READ));

        exp.setCachePermissions(permCache);

        Map<String, Collection<SecurityPermission>> permTask = new HashMap<>();
        permTask.put("task1", Arrays.asList(TASK_CANCEL));
        permTask.put("task2", Arrays.asList(TASK_EXECUTE));

        exp.setTaskPermissions(permTask);

        Map<String, Collection<SecurityPermission>> permSrvc = new HashMap<>();
        permSrvc.put("service1", Arrays.asList(SERVICE_DEPLOY));
        permSrvc.put("service2", Arrays.asList(SERVICE_INVOKE));

        exp.setServicePermissions(permSrvc);

        exp.setSystemPermissions(Arrays.asList(ADMIN_VIEW, EVENTS_ENABLE));

        final SecurityPermissionSetBuilder permsBuilder = new SecurityPermissionSetBuilder();

        assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        permsBuilder.appendCachePermissions("cache", ADMIN_VIEW);
                        return null;
                    }
                }, IgniteException.class,
                "you can assign permission only start with [CACHE_], but you try ADMIN_VIEW"
        );

        assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        permsBuilder.appendTaskPermissions("task", CACHE_READ);
                        return null;
                    }
                }, IgniteException.class,
                "you can assign permission only start with [TASK_], but you try CACHE_READ"
        );

        assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        permsBuilder.appendSystemPermissions(TASK_EXECUTE, CACHE_PUT);
                        return null;
                    }
                }, IgniteException.class,
                "you can assign permission only start with [EVENTS_, ADMIN_], but you try TASK_EXECUTE"
        );

        assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    permsBuilder.appendSystemPermissions(SERVICE_INVOKE, CACHE_REMOVE);
                    return null;
                }
            }, IgniteException.class,
            "you can assign permission only start with [EVENTS_, ADMIN_], but you try SERVICE_INVOKE"
        );

        permsBuilder.appendCachePermissions(
                "cache1", CACHE_PUT, CACHE_REMOVE
        ).appendCachePermissions(
                "cache2", CACHE_READ
        ).appendTaskPermissions(
                "task1", TASK_CANCEL
        ).appendTaskPermissions(
                "task2", TASK_EXECUTE
        ).appendServicePermissions(
            "service1", SERVICE_DEPLOY
        ).appendServicePermissions(
            "service2", SERVICE_INVOKE
        ).appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE);

        SecurityPermissionSet actual = permsBuilder.build();

        assertEquals(exp.cachePermissions(), actual.cachePermissions());
        assertEquals(exp.taskPermissions(), actual.taskPermissions());
        assertEquals(exp.servicePermissions(), actual.servicePermissions());
        assertEquals(exp.systemPermissions(), actual.systemPermissions());
        assertEquals(exp.defaultAllowAll(), actual.defaultAllowAll());
    }
}
