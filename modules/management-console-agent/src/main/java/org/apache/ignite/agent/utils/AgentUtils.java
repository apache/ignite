/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.agent.action.Session;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermission;

import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Utility methods.
 */
public final class AgentUtils {
    /** Agents path. */
    private static final String AGENTS_PATH = "/agents";

    /** */
    public static final String[] EMPTY = {};

    /**
     * Default constructor.
     */
    private AgentUtils() {
        // No-op.
    }

    /**
     * @param srvUri Server uri.
     * @param clusterId Cluster ID.
     */
    public static String monitoringUri(String srvUri, UUID clusterId) {
        return srvUri + "/clusters/" + clusterId + "/monitoring-dashboard";
    }

    /**
     * Prepare server uri.
     */
    public static URI toWsUri(String srvUri) {
        URI uri = URI.create(srvUri);

        if (uri.getScheme().startsWith("http")) {
            try {
                uri = new URI("http".equalsIgnoreCase(uri.getScheme()) ? "ws" : "wss",
                        uri.getUserInfo(),
                        uri.getHost(),
                        uri.getPort(),
                        AGENTS_PATH,
                        uri.getQuery(),
                        uri.getFragment()
                );
            }
            catch (URISyntaxException x) {
                throw new IllegalArgumentException(x.getMessage(), x);
            }
        }

        return uri;
    }

    /**
     * Authenticate by session and ignite security.
     *
     * @param security Security.
     * @param ses Session.
     */
    public static SecurityContext authenticate(
        IgniteSecurity security,
        Session ses
    ) throws IgniteAuthenticationException, IgniteCheckedException {
        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(ses.id());
        authCtx.nodeAttributes(Collections.emptyMap());
        authCtx.address(ses.address());
        authCtx.credentials(ses.credentials());

        SecurityContext subjCtx = security.authenticate(authCtx);

        if (subjCtx == null) {
            if (ses.credentials() == null) {
                throw new IgniteAuthenticationException(
                    "Failed to authenticate remote client (secure session SPI not set?): " + ses.id()
                );
            }

            throw new IgniteAuthenticationException(
                "Failed to authenticate remote client (invalid credentials?): " + ses.id()
            );
        }

        return subjCtx;
    }

    /**
     * Authenticate by session and authentication processor.
     *
     * @param authenticationProc Authentication processor.
     * @param ses Session.
     */
    public static AuthorizationContext authenticate(
        IgniteAuthenticationProcessor authenticationProc,
        Session ses
    ) throws IgniteCheckedException {
        SecurityCredentials creds = ses.credentials();

        String login = null;

        if (creds.getLogin() instanceof String)
            login = (String) creds.getLogin();

        String pwd = null;

        if (creds.getPassword() instanceof String)
            pwd = (String) creds.getPassword();

        return authenticationProc.authenticate(login, pwd);
    }



    /**
     * @param security Security.
     * @param perm Permission.
     */
    public static void authorizeIfNeeded(IgniteSecurity security, SecurityPermission perm) {
        authorizeIfNeeded(security, null, perm);
    }

    /**
     * @param security Security.
     * @param name Name.
     * @param perm Permission.
     */
    public static void authorizeIfNeeded(IgniteSecurity security, String name, SecurityPermission perm) {
        if (security.enabled())
            security.authorize(name, perm);
    }

    /**
     * @param ctx Context.
     * @param nodes Nodes.
     *
     * @return Set of supported cluster features.
     */
    public static Set<String> getClusterFeatures(GridKernalContext ctx, Collection<ClusterNode> nodes) {
        IgniteFeatures[] enums = IgniteFeatures.values();

        Set<String> features = U.newHashSet(enums.length);

        for (IgniteFeatures val : enums) {
            if (allNodesSupports(ctx, nodes, val))
                features.add(val.name());
        }

        return features;
    }

    /**
     * @param col Column.
     * @return Empty stream if collection is null else stream of collection elements.
     */
    public static <T> Stream<T> fromNullableCollection(Collection<T> col) {
        return col == null ? Stream.empty() : col.stream();
    }

    /**
     * Quietly closes given processor ignoring possible checked exception.
     *
     * @param proc Process.
     */
    public static void quiteStop(GridProcessor proc) {
        if (proc != null) {
            try {
                proc.stop(true);
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }
}
