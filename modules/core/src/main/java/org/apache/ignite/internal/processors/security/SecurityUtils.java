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

import java.security.Permission;
import java.security.Permissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.security.sandbox.IgniteInstance;
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
    public static final Permissions EMPTY_PERMISSIONS;

    static {
        EMPTY_PERMISSIONS = new Permissions();

        EMPTY_PERMISSIONS.setReadOnly();
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
     * Checks passed permission by installed SecurityManager.
     *
     * @param perm The requested permission.
     * @see SecurityManager#checkPermission(Permission)
     */
    public static void checkPermission(Permission perm) {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null)
            sm.checkPermission(perm);
    }

    /**
     *
     */
    public static Ignite ignite(Ignite ignite) {
        if (ignite instanceof IgniteEx) {
            IgniteEx ex = (IgniteEx)ignite;

            if (ex.context().security().enabled())
                return new IgniteInstance(ignite);
        }

        return ignite;
    }
}
