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

package org.apache.ignite.marshaller;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.managers.loadbalancer.*;
import org.apache.ignite.logger.java.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.thread.*;
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
    private Marshaller marshaller;

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
        log = new JavaLogger();
        marshaller = new JdkMarshaller();
        mbeanSrv = ManagementFactory.getPlatformMBeanServer();
        ses = new GridTestTaskSession();
        execSvc = new IgniteThreadPoolExecutor(1, 1, 0, new LinkedBlockingQueue<Runnable>());
        appCtx = new GenericApplicationContext();
        jobCtx = new GridTestJobContext();
        balancer = new LoadBalancer();
    }

    /**
     * Checks that all resources are null.
     */
    public void checkNullResources() {
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
