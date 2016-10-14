package org.apache.ignite.internal.processors.security;

import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.InetSocketAddress;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.os.GridOsSecurityProcessor;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 */
public class GridSecurityProcessorSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }

    /**
     *
     */
    public void testNotGlobalAuth() throws Exception {
        Map<UUID, List<UUID>> rmAuth = new HashMap<>();

        AtomicInteger selfAuth = new AtomicInteger();

        String name1 = "ignite1";

        Ignite ig1 = startGrid(name1, config(name1, selfAuth, rmAuth, false));

        assertEquals(1, selfAuth.get());

        String name2 = "ignite2";

        Ignite ig2 = startGrid(name2, config(name2, selfAuth, rmAuth, false));

        assertEquals(1, selfAuth.get());

        String name3 = "ignite3";

        Ignite ig3 = startGrid(name3, config(name3, selfAuth, rmAuth, false));

        assertEquals(1, selfAuth.get());

        String name4 = "ignite4";

        Ignite ig4 = startGrid(name4, config(name4, selfAuth, rmAuth, false));

        assertEquals(1, selfAuth.get());

        UUID ig1Id = nodeId(ig1);
        UUID ig2Id = nodeId(ig2);
        UUID ig3Id = nodeId(ig3);
        UUID ig4Id = nodeId(ig4);

        List<UUID> exp1 = Arrays.asList(ig2Id, ig3Id, ig4Id);

        assertEquals(exp1, rmAuth.get(ig1Id));
        assertEquals(null, rmAuth.get(ig2Id));
        assertEquals(null, rmAuth.get(ig3Id));
        assertEquals(null, rmAuth.get(ig4Id));
    }

    /**
     *
     */
    public void testGlobalAuth() throws Exception {
        Map<UUID, List<UUID>> rmAuth = new HashMap<>();

        AtomicInteger selfAuth = new AtomicInteger();

        String name1 = "ignite1";

        Ignite ig1 = startGrid(name1, config(name1, selfAuth, rmAuth, true));

        assertEquals(1, selfAuth.get());

        String name2 = "ignite2";

        Ignite ig2 = startGrid(name2, config(name2, selfAuth, rmAuth, true));

        assertEquals(2, selfAuth.get());

        String name3 = "ignite3";

        Ignite ig3 = startGrid(name3, config(name3, selfAuth, rmAuth, true));

        assertEquals(3, selfAuth.get());

        String name4 = "ignite4";

        Ignite ig4 = startGrid(name4, config(name4, selfAuth, rmAuth, true));

        assertEquals(4, selfAuth.get());

        UUID ig1Id = nodeId(ig1);
        UUID ig2Id = nodeId(ig2);
        UUID ig3Id = nodeId(ig3);
        UUID ig4Id = nodeId(ig4);

        List<UUID> exp1 = Arrays.asList(ig2Id, ig3Id, ig4Id);
        List<UUID> exp2 = Arrays.asList(ig3Id, ig4Id);
        List<UUID> exp3 = Arrays.asList(ig4Id);

        assertEquals(exp1, rmAuth.get(ig1Id));
        assertEquals(exp2, rmAuth.get(ig2Id));
        assertEquals(exp3, rmAuth.get(ig3Id));
        assertEquals(null, rmAuth.get(ig4Id));
    }

    /**
     *
     * @param ig Ignite.
     * @return UUID
     */
    private UUID nodeId(Ignite ig){
        return ig.configuration().getDiscoverySpi().getLocalNode().id();
    }

    /**
     *
     */
    private IgniteConfiguration config(
            String name,
            AtomicInteger authCnt,
            Map<UUID,List<UUID>> authMap,
            Boolean global
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        Map<String, Object> attr = new HashMap<>();

        attr.put("crd", credentials(name));
        attr.put("selfCnt", authCnt);
        attr.put("rmAuth", authMap);
        attr.put("global", global);

        cfg.setUserAttributes(attr);

        return cfg;
    }

    /**
     * Create security credentials.
     *
     * @param name Name.
     */
    private SecurityCredentials credentials(String name){
        SecurityCredentials sc = new SecurityCredentials();

        sc.setLogin("login" + name);
        sc.setPassword("pass" + name);

        return sc;
    }

    /**
     *
     */
     static class GridTestSecurityProcessor extends GridOsSecurityProcessor {
        /** Auth count. */
        private final AtomicInteger selfAuth;

        /** Remote auth. */
        private final Map<UUID,List<UUID>> rmAuth;

        /** Is global. */
        private final boolean global;

        /**
         * @param ctx Kernal context.
         * @param authCnt
         * @param global
         * @param rmAuth
         */
        protected GridTestSecurityProcessor(
                GridKernalContext ctx,
                AtomicInteger authCnt,
                Map<UUID, List<UUID>> rmAuth,
                boolean global
        ) {
            super(ctx);
            this.selfAuth = authCnt;
            this.global = global;
            this.rmAuth = rmAuth;
        }

        /** {@inheritDoc} */
        @Override public boolean isGlobalNodeAuthentication() {
            return global;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException {
            UUID locId = ctx.discovery().localNode().id();
            UUID rmId = node.id();

            if (rmId.equals(locId))
                selfAuth.incrementAndGet();
            else {
                List<UUID> auth = rmAuth.get(locId);
                if (auth == null) {
                    ArrayList<UUID> ls = new ArrayList<>();

                    ls.add(node.id());

                    rmAuth.put(locId, ls);
                } else
                    auth.add(rmId);
            }

            return new TestSecurityContext(new TestSecuritySubject());
        }

        /** {@inheritDoc} */
        @Override public boolean enabled() {
            return true;
        }
    }

    /**
     *
     */
    private static class TestSecurityContext implements SecurityContext, Serializable{

        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Sec subj. */
        private final SecuritySubject secSubj;

        /**
         * @param secSubj Sec subj.
         */
        private TestSecurityContext(SecuritySubject secSubj) {
            this.secSubj = secSubj;
        }

        /** {@inheritDoc} */
        @Override public SecuritySubject subject() {
            return secSubj;
        }

        /** {@inheritDoc} */
        @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean systemOperationAllowed(SecurityPermission perm) {
            return false;
        }
    }

    /**
     *
     */
    private static class TestSecuritySubject implements SecuritySubject{

        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public UUID id() {
            return UUID.fromString("test uuid");
        }

        /** {@inheritDoc} */
        @Override public SecuritySubjectType type() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object login() {
            return "login";
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress address() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet permissions() {
            return new SecurityPermissionSet() {
                private static final long serialVersionUID = 0L;

                @Override public boolean defaultAllowAll() {
                    return true;
                }

                @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
                    return Collections.emptyMap();
                }

                @Override public Map<String, Collection<SecurityPermission>> cachePermissions() {
                    return Collections.emptyMap();
                }

                @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
                    return Collections.emptyList();
                }
            };
        }
    }

}