/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.external.resource;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.springframework.context.*;
import javax.management.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests task resource injection.
 */
@SuppressWarnings({"UnusedDeclaration"})
public abstract class GridAbstractUserExternalResource {
    /** */
    static final Map<Class<?>, Integer> createClss = new HashMap<>();

    /** */
    static final Map<Class<?>, Integer> deployClss = new HashMap<>();

    /** */
    static final Map<Class<?>, Integer> undeployClss = new HashMap<>();

    /** */
    @GridLoggerResource private GridLogger log;

    /** */
    @GridInstanceResource private Grid grid;

    /** */
    @GridLocalNodeIdResource private UUID nodeId;

    /** */
    @GridMBeanServerResource private MBeanServer mbeanSrv;

    /** */
    @GridExecutorServiceResource private ExecutorService exec;

    /** */
    @GridHomeResource private String ggHome;

    /** */
    @GridNameResource private String ggName;

    /** */
    @GridSpringApplicationContextResource private ApplicationContext springCtx;

    /** */
    GridAbstractUserExternalResource() {
        addUsage(createClss);
    }

    /** */
    @SuppressWarnings("unused")
    @GridUserResourceOnDeployed private void deploy() {
        addUsage(deployClss);

        assert log != null;
        assert grid != null;
        assert nodeId != null;
        assert mbeanSrv != null;
        assert exec != null;
        assert ggHome != null;
        assert ggName != null;
        assert springCtx != null;

        log.info("Deploying resource: " + this);
    }

    /** */
    @SuppressWarnings("unused")
    @GridUserResourceOnUndeployed private void undeploy() {
        addUsage(undeployClss);

        assert log != null;
        assert grid != null;
        assert nodeId != null;
        assert mbeanSrv != null;
        assert exec != null;
        assert ggHome != null;
        assert ggName != null;
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
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void addUsage(Map<Class<?>, Integer> map) {
        synchronized (map) {
            Integer cnt = map.get(getClass());

            map.put(getClass(), cnt == null ? 1 : cnt + 1);
        }
    }

    /**
     * @param usage Map of classes to their usages.
     * @param cls Used class.
     * @param cnt Expected number of usages.
     */
    public static void checkUsageCount(Map<Class<?>, Integer> usage, Class<?> cls, int cnt) {
        Integer used = usage.get(cls);

        if (used == null) {
            used = 0;
        }

        assert used == cnt : "Invalid count [expected=" + cnt + ", actual=" + used + ", usageMap=" + usage + ']';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getSimpleName()).append(" [");
        buf.append(']');

        return buf.toString();
    }
}
