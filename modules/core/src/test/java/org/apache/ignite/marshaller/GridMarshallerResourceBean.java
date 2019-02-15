/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.marshaller;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.management.MBeanServer;
import org.apache.ignite.GridTestJobContext;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.managers.loadbalancer.GridLoadBalancerAdapter;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

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
        execSvc = Executors.newSingleThreadExecutor();
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
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ClusterNode getBalancedNode(ComputeJob job, Collection<ClusterNode> exclNodes) {
            return null;
        }
    }
}