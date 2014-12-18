/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.thread.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.logger.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.io.*;
import java.lang.management.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Test resources for injection.
 */
public class GridTestResources {
    /** */
    private static final IgniteLogger rootLog = new GridTestLog4jLogger(false);

    /** */
    private final IgniteLogger log;

    /** Local host. */
    private final String locHost;

    /** */
    private final UUID nodeId;

    /** */
    private IgniteMarshaller marshaller;

    /** */
    private final MBeanServer jmx;

    /** */
    private final String home;

    /** */
    private ThreadPoolExecutor execSvc;

    /** */
    private GridResourceProcessor rsrcProc;

    /** */
    public GridTestResources() {
        log = rootLog.getLogger(getClass());
        nodeId = UUID.randomUUID();
        jmx = ManagementFactory.getPlatformMBeanServer();
        home = U.getGridGainHome();
        locHost = localHost();

        GridTestKernalContext ctx = new GridTestKernalContext();

        ctx.config().setGridLogger(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param jmx JMX server.
     */
    public GridTestResources(MBeanServer jmx) {
        assert jmx != null;

        this.jmx = jmx;

        log = rootLog.getLogger(getClass());

        nodeId = UUID.randomUUID();
        home = U.getGridGainHome();
        locHost = localHost();

        GridTestKernalContext ctx = new GridTestKernalContext();

        ctx.config().setGridLogger(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param log Logger.
     */
    public GridTestResources(IgniteLogger log) {
        assert log != null;

        this.log = log.getLogger(getClass());

        nodeId = UUID.randomUUID();
        jmx = ManagementFactory.getPlatformMBeanServer();
        home = U.getGridGainHome();
        locHost = localHost();

        GridTestKernalContext ctx = new GridTestKernalContext();

        ctx.config().setGridLogger(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @return Local host.
     */
    @Nullable private String localHost() {
        try {
            return U.getLocalHost().getHostAddress();
        }
        catch (IOException e) {
            System.err.println("Failed to detect local host address.");

            e.printStackTrace();

            return null;
        }
    }

    /**
     * @param prestart Prestart flag.
     */
    public void startThreads(boolean prestart) {
        execSvc = new IgniteThreadPoolExecutor(nodeId.toString(), 40, 40, Long.MAX_VALUE,
            new LinkedBlockingQueue<Runnable>());

        // Improve concurrency for testing.
        if (prestart)
            execSvc.prestartAllCoreThreads();
    }

    /** */
    public void stopThreads() {
        if (execSvc != null) {
            U.shutdownNow(getClass(), execSvc, log);

            execSvc = null;
        }
    }

    /**
     * @param target Target.
     * @throws IgniteCheckedException If failed.
     */
    public void inject(Object target) throws IgniteCheckedException {
        assert target != null;
        assert getLogger() != null;

        rsrcProc.injectBasicResource(target, IgniteLoggerResource.class, getLogger().getLogger(target.getClass()));
        rsrcProc.injectBasicResource(target, IgniteInstanceResource.class,
            new GridTestIgnite(null, locHost, nodeId, marshaller, jmx, home));
    }

    /**
     * @return Executor service.
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * @return GridGain home.
     */
    public String getGridgainHome() {
        return home;
    }

    /**
     * @return MBean server.
     */
    public MBeanServer getMBeanServer() {
        return jmx;
    }

    /**
     * @return Logger for specified class.
     */
    public static IgniteLogger getLogger(Class<?> cls) {
        return rootLog.getLogger(cls);
    }

    /**
     * @return Logger.
     */
    public IgniteLogger getLogger() {
        return log;
    }

    /**
     * @return Node ID.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * @return Local host.
     */
    public String getLocalHost() {
        return locHost;
    }

    /**
     * @return Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public synchronized IgniteMarshaller getMarshaller() throws IgniteCheckedException {
        if (marshaller == null) {
            String marshallerName = GridTestProperties.getProperty("marshaller.class");

            if (marshallerName == null)
                marshaller = new IgniteOptimizedMarshaller();
            else {
                try {
                    Class<? extends IgniteMarshaller> cls = (Class<? extends IgniteMarshaller>)Class.forName(marshallerName);

                    marshaller = cls.newInstance();
                }
                catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                    throw new IgniteCheckedException("Failed to create test marshaller [marshaller=" + marshallerName + ']', e);
                }
            }
        }

        return marshaller;
    }
}
