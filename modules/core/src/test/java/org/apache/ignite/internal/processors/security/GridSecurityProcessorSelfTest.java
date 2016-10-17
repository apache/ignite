package org.apache.ignite.internal.processors.security;

import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.InetSocketAddress;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.os.GridOsSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test for check correct work {@link GridSecurityProcessor}
 */
public class GridSecurityProcessorSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }

    /**
     *
     * @throws Exception If fail.
     */
    public void testNotGlobalAuth() throws Exception {
        Map<UUID, List<UUID>> rmAuth = new HashMap<>();

        AtomicInteger selfAuth = new AtomicInteger();

        Map<SecurityCredentials, TestSecurityPermissionSet> permsMap = new HashMap<>();

        SecurityCredentials cred = credentials("ignite", "best");

        permsMap.put(cred, new TestSecurityPermissionSet());

        String name1 = "ignite1";

        Ignite ig1 = startGrid(name1, config(cred, selfAuth, rmAuth, false, permsMap));

        assertEquals(1, selfAuth.get());

        String name2 = "ignite2";

        Ignite ig2 = startGrid(name2, config(cred, selfAuth, rmAuth, false, permsMap));

        assertEquals(1, selfAuth.get());

        String name3 = "ignite3";

        Ignite ig3 = startGrid(name3, config(cred, selfAuth, rmAuth, false, permsMap));

        assertEquals(1, selfAuth.get());

        String name4 = "ignite4";

        Ignite ig4 = startGrid(name4, config(cred, selfAuth, rmAuth, false, permsMap));

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
     * @throws Exception If fail.
     */
    public void testGlobalAuth() throws Exception {
        Map<UUID, List<UUID>> rmAuth = new HashMap<>();

        AtomicInteger selfAuth = new AtomicInteger();

        Map<SecurityCredentials, TestSecurityPermissionSet> permsMap = new HashMap<>();

        SecurityCredentials cred = credentials("ignite", "best");

        permsMap.put(cred, new TestSecurityPermissionSet());

        String name1 = "ignite1";

        Ignite ig1 = startGrid(name1, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(1, selfAuth.get());

        String name2 = "ignite2";

        Ignite ig2 = startGrid(name2, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(2, selfAuth.get());

        String name3 = "ignite3";

        Ignite ig3 = startGrid(name3, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(3, selfAuth.get());

        String name4 = "ignite4";

        Ignite ig4 = startGrid(name4, config(cred, selfAuth, rmAuth, true, permsMap));

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
     * @throws Exception If fail.
     */
    public void testGlobalAuthFail() throws Exception {
        final Map<UUID, List<UUID>> rmAuth = new HashMap<>();

        final AtomicInteger selfAuth = new AtomicInteger();

        final SecurityCredentials cred = credentials("ignite", "best");

        Map<SecurityCredentials, TestSecurityPermissionSet> permsMap = new HashMap<>();

        TestSecurityPermissionSet permSet = new TestSecurityPermissionSet();

        permSet.sysPerms.add(SecurityPermission.ADMIN_CACHE);

        permsMap.put(cred, permSet);

        String name1 = "ignite1";

        Ignite ig1 = startGrid(name1, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(1, selfAuth.get());

        String name2 = "ignite2";

        Ignite ig2 = startGrid(name2, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(2, selfAuth.get());

        String name3 = "ignite3";

        Ignite ig3 = startGrid(name3, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(3, selfAuth.get());

        final Map<SecurityCredentials, TestSecurityPermissionSet> permsMap2 = new HashMap<>();

        TestSecurityPermissionSet permSet2 = new TestSecurityPermissionSet();

        permSet2.sysPerms.add(SecurityPermission.ADMIN_VIEW);

        permsMap2.put(cred, permSet2);

        final String name4 = "ignite4";

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                startGrid(name4, config(cred, selfAuth, rmAuth, true, permsMap2));

                return null;
            }
        }, IgniteException.class, "Failed to start manager");

        assertEquals(4, selfAuth.get());

        String name5 = "ignite5";

        Ignite ig5 = startGrid(name5, config(cred, selfAuth, rmAuth, true, permsMap));

        assertEquals(5, selfAuth.get());

        UUID ig1Id = nodeId(ig1);
        UUID ig2Id = nodeId(ig2);
        UUID ig3Id = nodeId(ig3);
        UUID ig5Id = nodeId(ig5);

        List<UUID> exp1 = Arrays.asList(ig2Id, ig3Id, ig5Id);
        List<UUID> exp2 = Arrays.asList(ig3Id, ig5Id);

        assertEquals(4, rmAuth.get(ig1Id).size());
        assertEquals(3, rmAuth.get(ig2Id).size());
        assertEquals(2, rmAuth.get(ig3Id).size());

        assertTrue(rmAuth.get(ig1Id).containsAll(exp1));
        assertTrue(rmAuth.get(ig2Id).containsAll(exp2));
    }

    /**
     * @param ig Ignite.
     * @return UUID
     */
    private UUID nodeId(Ignite ig){
        return ig.configuration().getDiscoverySpi().getLocalNode().id();
    }

    /**
     * @param crd Credentials.
     * @param authCnt Authentication counter.
     * @param authMap Authentication map.
     * @param global Is global authentication.
     * @param permsMap Permission map.
     *
     * @throws Exception If fail get configuration.
     */
    private IgniteConfiguration config(
            SecurityCredentials crd,
            AtomicInteger authCnt,
            Map<UUID,List<UUID>> authMap,
            Boolean global,
            Map<SecurityCredentials, TestSecurityPermissionSet> permsMap
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        Map<String, Object> attr = new HashMap<>();

        attr.put("crd", crd);
        attr.put("selfCnt", authCnt);
        attr.put("rmAuth", authMap);
        attr.put("global", global);
        attr.put("permsMap", permsMap);

        cfg.setUserAttributes(attr);

        return cfg;
    }

    /**
     * Create security credentials.
     *
     * @param login Login.
     * @param pass Password.
     */
    private SecurityCredentials credentials(String login, String pass) {
        SecurityCredentials sc = new SecurityCredentials();

        sc.setLogin(login);
        sc.setPassword(pass);

        return sc;
    }

    /**
     * Test security processor.
     */
    public static class GridTestSecurityProcessor extends GridOsSecurityProcessor {
        /** Auth count. */
        private final AtomicInteger selfAuth;

        /** Remote auth. */
        private final Map<UUID,List<UUID>> rmAuth;

        /** Is global. */
        private final boolean global;

        /** Permissions map. */
        private Map<SecurityCredentials, TestSecurityPermissionSet> permsMap;

        /**
         * @param ctx Context.
         * @param selfAuth Local authentication counter.
         * @param rmAuth Map for count remote authentication.
         * @param global Is global authentication.
         * @param permsMap Permission map.
         */
        GridTestSecurityProcessor(
                GridKernalContext ctx,
                AtomicInteger selfAuth,
                Map<UUID, List<UUID>> rmAuth,
                boolean global,
                Map<SecurityCredentials, TestSecurityPermissionSet> permsMap
        ) {
            super(ctx);
            this.selfAuth = selfAuth;
            this.global = global;
            this.rmAuth = rmAuth;
            this.permsMap = permsMap;
        }

        /** {@inheritDoc} */
        @Override public boolean isGlobalNodeAuthentication() {
            return global;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException {
            checkAuth(node);

            TestSecurityPermissionSet permsSet = permsMap.get(cred);

            return new TestSecurityContext(new TestSecuritySubject((String) cred.getLogin(), permsSet));
        }

        /** {@inheritDoc} */
        @Override public boolean enabled() {
            return true;
        }

        /**
         * @param node Node.
         */
        private void checkAuth(ClusterNode node){
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
        }
    }

    /**
     * Test security context.
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
     * Test security subject.
     */
    private static class TestSecuritySubject implements SecuritySubject{

        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Login. */
        private String login;

        /** Permissions set. */
        private SecurityPermissionSet permsSet;

        /**
         * @param login Login.
         * @param permsSet Permissions set.
         */
        private TestSecuritySubject(String login, SecurityPermissionSet permsSet) {
            this.login = login;
            this.permsSet = permsSet;
        }

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
            return login;
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress address() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet permissions() {
            return permsSet;
        }
    }

    /**
     * Test permission set.
     */
    public static class TestSecurityPermissionSet implements SecurityPermissionSet{
        /** */
        private boolean defaultAllowAll;

        /** Task permissions. */
        private final Map<String, Collection<SecurityPermission>> taskPerms = new HashMap<>();

        /** Cache permissions. */
        private final Map<String, Collection<SecurityPermission>> cachePerms = new HashMap<>();

        /** System permissions. */
        private final List<SecurityPermission> sysPerms = new ArrayList<>();

        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean defaultAllowAll() {
            return defaultAllowAll;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
            return taskPerms;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> cachePermissions() {
            return cachePerms;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
            return sysPerms;
        }
    }
}