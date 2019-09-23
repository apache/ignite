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

package org.apache.ignite.internal.processors.security;

import java.security.AccessController;
import java.security.AllPermission;
import java.security.Permissions;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.sandbox.SandboxCallable;
import org.apache.ignite.internal.processors.security.sandbox.SandboxRunnable;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Security utilities.
 */
public class SecurityUtils {
    /** */
    public static final String MSG_SEC_PROC_CLS_IS_INVALID = "Local node's grid security processor class " +
        "is not equal to remote node's grid security processor class " +
        "[locNodeId=%s, rmtNodeId=%s, locCls=%s, rmtCls=%s]";

    /** Default serialization version. */
    private static final int DFLT_SERIALIZE_VERSION = isSecurityCompatibilityMode() ? 1 : 2;

    /** Current serialization version. */
    private static final ThreadLocal<Integer> SERIALIZE_VERSION = new ThreadLocal<Integer>(){
        @Override protected Integer initialValue() {
            return DFLT_SERIALIZE_VERSION;
        }
    };

    /** Permissions that contain {@code AllPermission}. */
    public static final Permissions ALL_PERMISSIONS;

    static {
        ALL_PERMISSIONS = new Permissions();

        ALL_PERMISSIONS.add(new AllPermission());
        ALL_PERMISSIONS.setReadOnly();
    }

    /**
     * Private constructor.
     */
    private SecurityUtils() {
    }

    /**
     * @return Security compatibility mode flag.
     */
    public static boolean isSecurityCompatibilityMode() {
        return IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SECURITY_COMPATIBILITY_MODE, false);
    }

    /**
     * @param ver Serialize version.
     */
    public static void serializeVersion(int ver) {
        SERIALIZE_VERSION.set(ver);
    }

    /**
     * @return Serialize version.
     */
    public static int serializeVersion() {
        return SERIALIZE_VERSION.get();
    }

    /**
     * Sets default serialize version {@link #DFLT_SERIALIZE_VERSION}.
     */
    public static void restoreDefaultSerializeVersion() {
        serializeVersion(DFLT_SERIALIZE_VERSION);
    }

    /**
     * @return Allow all service permissions.
     */
    public static Map<String, Collection<SecurityPermission>> compatibleServicePermissions() {
        Map<String, Collection<SecurityPermission>> srvcPerms = new HashMap<>();

        srvcPerms.put("*", Arrays.asList(
            SecurityPermission.SERVICE_CANCEL,
            SecurityPermission.SERVICE_DEPLOY,
            SecurityPermission.SERVICE_INVOKE));

        return srvcPerms;
    }

    /**
     * Gets the node's security context.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @param node Node.
     * @return Node's security context.
     */
    public static SecurityContext nodeSecurityContext(Marshaller marsh, ClassLoader ldr, ClusterNode node) {
        byte[] subjBytes = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);

        if (subjBytes == null)
            throw new SecurityException("Security context isn't certain.");

        try {
            return U.unmarshal(marsh, subjBytes, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new SecurityException("Failed to get security context.", e);
        }
    }

    /**
     * Computes a result in a privileged action.
     *
     * @param c Instance of SandboxCallable.
     * @param <T> Type of result.
     * @param <E> Type of Exception.
     * @return Computed result.
     * @throws E if unable to compute a result.
     */
    public static <T, E extends Exception> T doPrivileged(SandboxCallable<T, E> c) throws E {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>)c::call);
        }
        catch (PrivilegedActionException e) {
            throw (E)e.getException();
        }
    }

    /**
     * Calls the method <code>run</code> of SandboxRunnable in a privileged action.
     *
     * @param r Instance of SandboxRunnable.
     * @param <E> Type of Exception.
     * @throws E if the run method is failed.
     */
    public static <E extends Exception> void doPrivileged(SandboxRunnable<E> r) throws E {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>)
                () -> {
                    r.run();

                    return null;
                }
            );
        }
        catch (PrivilegedActionException e) {
            throw (E)e.getException();
        }
    }

    /**
     * @return True if sandbox is enabled.
     */
    public static boolean isSandboxEnabled() {
        boolean res;

        try {
            IgniteEx ignite = IgnitionEx.localIgnite();

            res = ignite.context().security().sandbox().enabled();
        }
        catch (IgniteIllegalStateException e) {
            res = System.getSecurityManager() != null;
        }

        return res;
    }
}
