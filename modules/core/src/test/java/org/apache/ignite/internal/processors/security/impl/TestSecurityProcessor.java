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

package org.apache.ignite.internal.processors.security.impl;

import java.net.InetSocketAddress;
import java.security.Permissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;

/**
 * Security processor for test.
 */
public class TestSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** User data. */
    public static final Map<Object, TestSecurityData> USERS = new ConcurrentHashMap<>();

    /** */
    protected static final Map<UUID, SecurityContext> SECURITY_CONTEXTS = new ConcurrentHashMap<>();

    /** */
    private static final Collection<Class<?>> EXT_SYS_CLASSES = ConcurrentHashMap.newKeySet();

    /** Node security data. */
    private final TestSecurityData nodeSecData;

    /** Users security data. */
    private final Collection<TestSecurityData> predefinedAuthData;

    /** Global authentication. */
    private final boolean globalAuth;

    /**
     * Constructor.
     */
    public TestSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData, boolean globalAuth) {
        super(ctx);

        this.nodeSecData = nodeSecData;
        this.predefinedAuthData = predefinedAuthData.isEmpty()
            ? Collections.emptyList()
            : new ArrayList<>(predefinedAuthData);
        this.globalAuth = globalAuth;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        TestSecurityData data = USERS.get(cred.getLogin());

        if (data == null || !Objects.equals(cred, data.credentials()))
            return null;

        SecurityContext res = new TestSecurityContext(
            new TestSecuritySubject()
                .setType(REMOTE_NODE)
                .setId(node.id())
                .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                .setLogin(cred.getLogin())
                .sandboxPermissions(data.sandboxPermissions())
        );

        SECURITY_CONTEXTS.put(res.subject().id(), res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return globalAuth;
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemType(Class<?> cls) {
        return EXT_SYS_CLASSES.contains(cls);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        TestSecurityData data = USERS.get(ctx.credentials().getLogin());

        if (data == null || !Objects.equals(ctx.credentials(), data.credentials()))
            return null;

        SecurityContext res = new TestSecurityContext(
            new TestSecuritySubject()
                .setType(ctx.subjectType())
                .setId(ctx.subjectId())
                .setAddr(ctx.address())
                .setLogin(ctx.credentials().getLogin())
                .setCerts(ctx.certificates())
                .sandboxPermissions(data.sandboxPermissions())
        );

        SECURITY_CONTEXTS.put(res.subject().id(), res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return SECURITY_CONTEXTS.values().stream().map(SecurityContext::subject).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return securityContext(subjId).subject();
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext(UUID subjId) {
        return SECURITY_CONTEXTS.get(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(
        String name,
        SecurityPermission perm,
        SecurityContext securityCtx
    ) throws SecurityException {
        TestSecurityData userData = USERS.get(securityCtx.subject().login());

        if (userData == null || !contains(userData.permissions(), name, perm)) {
            throw new SecurityException("Authorization failed [perm=" + perm +
                ", name=" + name +
                ", subject=" + securityCtx.subject() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        USERS.put(nodeSecData.credentials().getLogin(), nodeSecData);

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData)
            USERS.put(data.credentials().getLogin(), data);
    }

    /** {@inheritDoc} */
    @Override public boolean sandboxEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void createUser(String login, char[] pwd) {
        TestSecurityData prev = USERS.putIfAbsent(
            login,
            new TestSecurityData(login, new String(pwd), ALL_PERMISSIONS, new Permissions())
        );

        if (prev != null)
            throw new SecurityException("User already exists [login=" + login + ']');
    }

    /** {@inheritDoc} */
    @Override public void alterUser(String login, char[] pwd) {
        TestSecurityData prev = USERS.computeIfPresent(
            login,
            (k, v) -> new TestSecurityData(login, new String(pwd), v.permissions(), v.sandboxPermissions())
        );

        if (prev == null)
            throw new SecurityException("User does not exist [login=" + login + ']');
    }

    /** {@inheritDoc} */
    @Override public void dropUser(String login) {
        USERS.remove(login);
    }

    /** */
    public static void registerExternalSystemTypes(Class<?>... cls) {
        EXT_SYS_CLASSES.addAll(Arrays.asList(cls));
    }

    /** */
    public static boolean contains(SecurityPermissionSet userPerms, String name, SecurityPermission perm) {
        boolean dfltAllowAll = userPerms.defaultAllowAll();

        switch (perm) {
            case CACHE_PUT:
            case CACHE_READ:
            case CACHE_REMOVE:
                return contains(userPerms.cachePermissions(), dfltAllowAll, name, perm);

            case CACHE_CREATE:
            case CACHE_DESTROY:
                return (name != null && contains(userPerms.cachePermissions(), dfltAllowAll, name, perm))
                    || containsSystemPermission(userPerms, perm);

            case TASK_CANCEL:
            case TASK_EXECUTE:
                return contains(userPerms.taskPermissions(), dfltAllowAll, name, perm);

            case SERVICE_DEPLOY:
            case SERVICE_INVOKE:
            case SERVICE_CANCEL:
                return contains(userPerms.servicePermissions(), dfltAllowAll, name, perm);

            default:
                return containsSystemPermission(userPerms, perm);
        }
    }

    /** */
    private static boolean contains(
        Map<String, Collection<SecurityPermission>> userPerms,
        boolean dfltAllowAll,
        String name,
        SecurityPermission perm
    ) {
        Collection<SecurityPermission> perms = userPerms.get(name);

        if (perms == null)
            return dfltAllowAll;

        return perms.stream().anyMatch(perm::equals);
    }

    /** */
    private static boolean containsSystemPermission(
        SecurityPermissionSet userPerms,
        SecurityPermission perm
    ) {
        Collection<SecurityPermission> sysPerms = userPerms.systemPermissions();

        if (F.isEmpty(sysPerms))
            return userPerms.defaultAllowAll();

        return sysPerms.stream().anyMatch(perm::equals);
    }
}
