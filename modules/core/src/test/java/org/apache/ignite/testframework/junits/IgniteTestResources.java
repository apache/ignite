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

package org.apache.ignite.testframework.junits;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import javax.management.MBeanServer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * Test resources for injection.
 */
public class IgniteTestResources {
    /** */
    private static final IgniteLogger rootLog = new GridTestLog4jLogger(false);

    /** */
    private final IgniteLogger log;

    /** Local host. */
    private final String locHost;

    /** */
    private final UUID nodeId;

    /** */
    private final MBeanServer jmx;

    /** */
    private final String home;

    /** */
    private ThreadPoolExecutor execSvc;

    /** */
    private GridResourceProcessor rsrcProc;

    /** */
    public IgniteTestResources() throws IgniteCheckedException {
        log = rootLog.getLogger(getClass());
        nodeId = UUID.randomUUID();
        jmx = ManagementFactory.getPlatformMBeanServer();
        home = U.getIgniteHome();
        locHost = localHost();

        U.setWorkDirectory(null, null);

        GridTestKernalContext ctx = new GridTestKernalContext(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param jmx JMX server.
     */
    public IgniteTestResources(MBeanServer jmx) throws IgniteCheckedException {
        assert jmx != null;

        this.jmx = jmx;

        log = rootLog.getLogger(getClass());

        nodeId = UUID.randomUUID();
        home = U.getIgniteHome();
        locHost = localHost();

        GridTestKernalContext ctx = new GridTestKernalContext(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param log Logger.
     */
    public IgniteTestResources(IgniteLogger log) throws IgniteCheckedException {
        assert log != null;

        this.log = log.getLogger(getClass());

        nodeId = UUID.randomUUID();
        jmx = ManagementFactory.getPlatformMBeanServer();
        home = U.getIgniteHome();
        locHost = localHost();

        GridTestKernalContext ctx = new GridTestKernalContext(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @return Resource processor.
     */
    public GridResourceProcessor resources() {
        return rsrcProc;
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

        rsrcProc.injectBasicResource(target, LoggerResource.class, getLogger().getLogger(target.getClass()));
        rsrcProc.injectBasicResource(target, IgniteInstanceResource.class,
            new IgniteMock(null, locHost, nodeId, getMarshaller(), jmx, home));
    }

    /**
     * @return Executor service.
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * @return Ignite home.
     */
    public String getIgniteHome() {
        return home;
    }

    /**
     * @return MBean server.
     */
    public MBeanServer getMBeanServer() {
        return jmx;
    }

    /**
     * @param cls Class.
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
    public synchronized Marshaller getMarshaller() throws IgniteCheckedException {
        String marshallerName = GridTestProperties.getProperty(GridTestProperties.MARSH_CLASS_NAME);

        Marshaller marsh;

        if (marshallerName == null)
            marsh = new OptimizedMarshaller();
        else {
            try {
                Class<? extends Marshaller> cls = (Class<? extends Marshaller>)Class.forName(marshallerName);

                marsh = cls.newInstance();
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new IgniteCheckedException("Failed to create test marshaller [marshaller=" +
                    marshallerName + ']', e);
            }
        }

        if (marsh instanceof OptimizedMarshaller)
            ((OptimizedMarshaller)marsh).setRequireSerializable(false);

        marsh.setContext(new MarshallerContextTestImpl());

        return marsh;
    }
}