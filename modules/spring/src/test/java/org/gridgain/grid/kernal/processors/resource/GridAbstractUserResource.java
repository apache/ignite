/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

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
abstract class GridAbstractUserResource {
    /** */
    public static final Map<Class<?>, Integer> createClss = new HashMap<>();

    /** */
    public static final Map<Class<?>, Integer> deployClss = new HashMap<>();

    /** */
    public static final Map<Class<?>, Integer> undeployClss = new HashMap<>();

    /** */
    @GridLoggerResource private GridLogger log;

    /** */
    @GridInstanceResource private Ignite ignite;

    /** */
    @GridLocalNodeIdResource private UUID nodeId;

    /** */
    @GridMBeanServerResource private MBeanServer mbeanSrv;

    /** */
    @GridExecutorServiceResource
    private ExecutorService exec;

    /** */
    @GridHomeResource private String ggHome;

    /** */
    @GridNameResource private String gridName;

    /** */
    @GridSpringApplicationContextResource private ApplicationContext springCtx;

    /** */
    GridAbstractUserResource() {
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
     * @param map Map to clear.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    private static void clearMap(Map<Class<?>, Integer> map) {
        synchronized (map) {
            map.clear();
        }
    }

    /** */
    @SuppressWarnings("unused")
    @GridUserResourceOnDeployed private void deploy() {
        addUsage(deployClss);

        assert log != null;
        assert ignite != null;
        assert nodeId != null;
        assert mbeanSrv != null;
        assert exec != null;
        assert ggHome != null;
        assert gridName != null;
        assert springCtx != null;

        log.info("Deploying resource: " + this);
    }

    /** */
    @SuppressWarnings("unused")
    @GridUserResourceOnUndeployed private void undeploy() {
        addUsage(undeployClss);

        assert log != null;
        assert ignite != null;
        assert nodeId != null;
        assert mbeanSrv != null;
        assert exec != null;
        assert ggHome != null;
        assert gridName != null;
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
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    protected void addUsage(Map<Class<?>, Integer> map) {
        synchronized (map) {
            Integer cnt = map.get(getClass());

            map.put(getClass(), cnt == null ? 1 : cnt + 1);
        }
    }
}
