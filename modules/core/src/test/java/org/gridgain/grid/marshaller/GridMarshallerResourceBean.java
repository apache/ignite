/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.logger.java.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.loadbalancer.*;
import org.gridgain.grid.thread.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import javax.management.*;
import java.io.*;
import java.lang.management.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Marshaller resource bean.
 */
class GridMarshallerResourceBean implements Serializable {
    /** Logger. */
    private IgniteLogger log;

    /** Marshaller. */
    private IgniteMarshaller marshaller;

    /** Load balancer. */
    private ComputeLoadBalancer balancer;

    /** MBean server. */
    private MBeanServer mbeanSrv;

    /** Session. */
    private ComputeTaskSession ses;

    /** Executor service. */
    private ExecutorService execSvc;

    /** Application context. */
    private ApplicationContext appCtx;

    /** Job context. */
    private ComputeJobContext jobCtx;

    /**
     * Initialization.
     */
    GridMarshallerResourceBean() {
        log = new IgniteJavaLogger();
        marshaller = new IgniteJdkMarshaller();
        mbeanSrv = ManagementFactory.getPlatformMBeanServer();
        ses = new GridTestTaskSession();
        execSvc = new GridThreadPoolExecutor(1, 1, 0, new LinkedBlockingQueue<Runnable>());
        appCtx = new GenericApplicationContext();
        jobCtx = new GridTestJobContext();
        balancer = new LoadBalancer();
    }

    /**
     * Checks that all resources are null.
     */
    void checkNullResources() {
        assert log == null;
        assert marshaller == null;
        assert balancer == null;
        assert mbeanSrv == null;
        assert ses == null;
        assert execSvc == null;
        assert appCtx == null;
        assert jobCtx == null;
    }

    /** */
    private static class LoadBalancer extends GridLoadBalancerAdapter {
        /** */
        public LoadBalancer() {
        }

        /** {@inheritDoc} */
        @Override public ClusterNode getBalancedNode(ComputeJob job, Collection<ClusterNode> exclNodes) {
            return null;
        }
    }
}
