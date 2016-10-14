package org.apache.ignite.plugin.security;

import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
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

        Collection<SecurityPermission> permSys = new ArrayList<>();

        permSys.add(ADMIN_VIEW);
        permSys.add(EVENTS_ENABLE);

        exp.setSysPermissions(permSys);

        final SecurityPermissionSetBuilder permsBuilder = new SecurityPermissionSetBuilder();

        assertThrows(log, new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        permsBuilder.appendCachePermissions("cache", ADMIN_VIEW);
                        return null;
                    }
                }, IgniteException.class,
                "you can assign permission only start with [CACHE_], but you try ADMIN_VIEW"
        );

        assertThrows(log, new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        permsBuilder.appendTaskPermissions("task", CACHE_READ);
                        return null;
                    }
                }, IgniteException.class,
                "you can assign permission only start with [TASK_], but you try CACHE_READ"
        );

        assertThrows(log, new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        permsBuilder.appendSystemPermissions(TASK_EXECUTE, CACHE_PUT);
                        return null;
                    }
                }, IgniteException.class,
                "you can assign permission only start with [EVENTS_, ADMIN_], but you try TASK_EXECUTE"
        );

        permsBuilder.appendCachePermissions(
                "cache1", CACHE_PUT, CACHE_REMOVE
        ).appendCachePermissions(
                "cache2", CACHE_READ
        ).appendTaskPermissions(
                "task1", TASK_CANCEL
        ).appendTaskPermissions(
                "task2", TASK_EXECUTE
        ).appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE);

        SecurityPermissionSet actual = permsBuilder.toSecurityPermissionSet();

        assertEquals(exp.cachePermissions(), actual.cachePermissions());
        assertEquals(exp.taskPermissions(), actual.taskPermissions());
        assertEquals(exp.systemPermissions(), actual.systemPermissions());
        assertEquals(exp.defaultAllowAll(), actual.defaultAllowAll());
    }
}