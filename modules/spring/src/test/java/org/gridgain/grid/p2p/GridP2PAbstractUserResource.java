/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.logger.*;
import org.springframework.context.*;
import javax.management.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests task resource injection.
 */
@SuppressWarnings({"UnusedDeclaration"})
abstract class GridP2PAbstractUserResource {
    /** */
    protected static final Map<Class<?>, Integer> createClss = new HashMap<>();

    /** */
    protected static final Map<Class<?>, Integer> deployClss = new HashMap<>();

    /** */
    protected static final Map<Class<?>, Integer> undeployClss = new HashMap<>();

    /** */
    @IgniteLoggerResource
    private GridLogger log;

    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @IgniteLocalNodeIdResource
    private UUID nodeId;

    /** */
    @IgniteMBeanServerResource
    private MBeanServer mbeanSrv;

    /** */
    @IgniteExecutorServiceResource
    private ExecutorService exec;

    /** */
    @IgniteHomeResource
    private String ggHome;

    /** */
    @IgniteSpringApplicationContextResource
    private ApplicationContext springCtx;

    /** */
    GridP2PAbstractUserResource() {
        addUsage(createClss);
    }

    /**
     * Resets counters.
     */
    public static void resetResourceCounters() {
        clearMap(createClss);
        clearMap(deployClss);
        clearMap(undeployClss);
    }

    /**
     * @param cls Class.
     * @param cnt Expected usage count.
     */
    public static void checkCreateCount(Class<?> cls, int cnt) {
        checkUsageCount(createClss, cls, cnt);
    }

    /**
     * @param cls Class.
     * @param cnt Expected usage count.
     */
    public static void checkDeployCount(Class<?> cls, int cnt) {
        checkUsageCount(deployClss, cls, cnt);
    }

    /**
     * @param cls Class.
     * @param cnt Expected usage count.
     */
    public static void checkUndeployCount(Class<?> cls, int cnt) {
        checkUsageCount(undeployClss, cls, cnt);
    }

    /**
     * @param usage Usage map.
     * @param cls Class.
     * @param cnt Expected usage count.
     */
    public static void checkUsageCount(Map<Class<?>, Integer> usage, Class<?> cls, int cnt) {
        Integer used;

        synchronized (usage) {
            used = usage.get(cls);
        }

        if (used == null)
            used = 0;

        assert used == cnt : "Invalid count [expected=" + cnt + ", actual=" + used + ", usageMap=" + usage + ']';
    }

    /**
     * @param map Map to clear.
     */
    private static void clearMap(final Map<Class<?>, Integer> map) {
        synchronized (map) {
            map.clear();
        }
    }

    /** */
    @SuppressWarnings("unused")
    @IgniteUserResourceOnDeployed
    private void deploy() {
        addUsage(deployClss);

        assert log != null;
        assert ignite != null;
        assert nodeId != null;
        assert mbeanSrv != null;
        assert exec != null;
        assert ggHome != null;
        assert springCtx != null;

        log.info("Deploying resource: " + this);
    }

    /** */
    @SuppressWarnings("unused")
    @IgniteUserResourceOnUndeployed
    private void undeploy() {
        addUsage(undeployClss);

        assert log != null;
        assert ignite != null;
        assert nodeId != null;
        assert mbeanSrv != null;
        assert exec != null;
        assert ggHome != null;
        assert springCtx != null;

        log.info("Undeploying resource: " + this);
    }

    /** */
    @SuppressWarnings("unused")
    private void neverCalled() {
        assert false;
    }

    /**
     * @param map Usage map to check.
     */
    protected void addUsage(final Map<Class<?>, Integer> map) {
        synchronized (map) {
            Integer cnt = map.get(getClass());

            map.put(getClass(), cnt == null ? 1 : cnt + 1);
        }
    }
}
