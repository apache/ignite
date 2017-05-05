package org.apache.ignite.cache.database.standbycluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistenceConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class GridChangeGlobalStateAbstractTest extends GridCommonAbstractTest {
    /** Primary suffix. */
    private static final String primarySuffix = "-primary";

    /** BackUp suffix. */
    private static final String backUpSuffix = "-backUp";

    /** BackUp suffix. */
    private static final String clientSuffix = "-client";

    /** Primary ip finder. */
    protected final TcpDiscoveryIpFinder primaryIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Back up ip finder. */
    protected final TcpDiscoveryIpFinder backUpIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Consistent id count. */
    private int consistentIdCnt;

    /** Nodes. */
    protected Map<String, Ignite> nodes = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        nodes.clear();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName(), true));

        startPrimaryNodes(primaryNodes());

        startPrimaryClientNodes(primaryClientNodes());

        startBackUpNodes(backUpNodes());

        startBackUpClientNodes(backUpClientNodes());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAll(clientSuffix);

        stopAll(primarySuffix);

        stopAll(backUpSuffix);

        nodes.clear();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName(), true));
    }

    /**
     *
     */
    protected int primaryNodes() {
        return 3;
    }

    /**
     *
     */
    protected int primaryClientNodes() {
        return 3;
    }

    /**
     *
     */
    protected int backUpNodes() {
        return 3;
    }

    /**
     *
     */
    protected int backUpClientNodes() {
        return 3;
    }

    /**
     * @param idx idx.
     */
    protected Ignite primary(int idx) {
        return nodes.get("node" + idx + primarySuffix);
    }

    /**
     * @param idx idx.
     */
    protected Ignite primaryClient(int idx) {
        return nodes.get("node" + idx + primarySuffix + clientSuffix);
    }

    /**
     * @param idx idx.
     */
    protected Ignite backUp(int idx) {
        return nodes.get("node" + idx + backUpSuffix);
    }

    /**
     * @param idx idx.
     */
    protected Ignite backUpClient(int idx) {
        return nodes.get("node" + idx + backUpSuffix + clientSuffix);
    }

    /**
     * @param cnt Count.
     */
    protected void startPrimaryNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            startPrimary(i);
    }

    /**
     * @param idx Index.
     */
    protected void startPrimary(int idx) throws Exception {
        String node = "node" + idx;

        String name = node + primarySuffix;

        IgniteConfiguration cfg = getConfiguration(name);
        cfg.setConsistentId(node);
        cfg.setActiveOnStart(true);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(primaryIpFinder);

        Ignite ig = startGrid(name, cfg);

        nodes.put(name, ig);
    }

    /**
     * @param cnt Count.
     */
    protected void startBackUpNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            startBackUp(i);
    }

    /**
     * @param idx Index.
     */
    protected void startBackUp(int idx) throws Exception {
        String node = "node" + idx;

        String name = node + backUpSuffix;

        IgniteConfiguration cfg = getConfiguration(name);
        cfg.setConsistentId(node);
        cfg.setActiveOnStart(false);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(backUpIpFinder);

        Ignite ig = startGrid(name, cfg);

        nodes.put(name, ig);
    }

    /**
     * @param cnt Count.
     */
    protected void startPrimaryClientNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            String node = "node" + i;

            String name = node + primarySuffix + clientSuffix;

            IgniteConfiguration cfg = getConfiguration(name);
            cfg.setConsistentId(node);
            cfg.setClientMode(true);
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(primaryIpFinder);

            Ignite ig = startGrid(name, cfg);

            nodes.put(name, ig);
        }
    }

    /**
     * @param cnt Count.
     */
    protected void startBackUpClientNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            String node = "node" + i;

            String name = node + backUpSuffix + clientSuffix;

            IgniteConfiguration cfg = getConfiguration(name);
            cfg.setConsistentId(node);
            cfg.setActiveOnStart(false);
            cfg.setClientMode(true);
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(backUpIpFinder);

            Ignite ig = startGrid(name, cfg);

            nodes.put(name, ig);
        }
    }

    /**
     *
     */
    protected Iterable<Ignite> allBackUpNodes(){
        List<Ignite> r = new ArrayList<>();

        for (String name : this.nodes.keySet())
            if (name.contains(backUpSuffix))
                r.add(nodes.get(name));

        return r;
    }

    /**
     *
     */
    protected Ignite randomBackUp(boolean includeClient) {
        int nodes = 0;

        List<Ignite> igs = new ArrayList<>();

        for (String name : this.nodes.keySet())
            if (name.contains(backUpSuffix)){
                if (includeClient)
                   igs.add(this.nodes.get(name));
                else {
                    if (name.contains(clientSuffix))
                        continue;

                    igs.add(this.nodes.get(name));
                }
            }

        int idx = ThreadLocalRandom.current().nextInt(0, igs.size());

        return igs.get(idx);
    }


    /**
     * @param i Idx.
     */
    protected void stopPrimary(int i) {
        String name = "node" + i + primarySuffix;

        nodes.get(name).close();

        nodes.remove(name);
    }

    /**
     *
     */
    protected void stopAllPrimary() {
        stopAll(primarySuffix);
    }

    /**
     *
     */
    protected void stopAllBackUp() {
        stopAll(backUpSuffix);
    }

    /**
     *
     */
    protected void stopAllClient() {
        stopAll(clientSuffix);
    }

    /**
     * @param suffix Suffix.
     */
    private void stopAll(String suffix) {
        for (String name : nodes.keySet())
            if (name.contains(suffix)) {
                Ignite ig = nodes.get(name);

                stopGrid(ig.name());

                nodes.remove(name);
            }
    }

    /**
     * @param gridName Grid name.
     */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        PersistenceConfiguration pCfg = new PersistenceConfiguration();

        pCfg.setPersistenceStorePath(testName() + "/db");
        pCfg.setWalArchivePath(testName() + "/db/wal/archive");
        pCfg.setWalStorePath(testName() + "/db/wal");

        cfg.setPersistenceConfiguration(pCfg);

        final MemoryConfiguration memCfg = new MemoryConfiguration();

        memCfg.setPageSize(1024);
        memCfg.setConcurrencyLevel(64);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();
        memPlcCfg.setSize(5 * 1024 * 1024);
        memPlcCfg.setName("dfltMemPlc");

        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(memCfg);

        return cfg;
    }

    /**
     *
     */
    protected String testName() {
        return getClass().getSimpleName();
    }

}
