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

package org.apache.ignite.p2p;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PRemoteMultiJVMClassLoadersSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** Timeout for task execution in milliseconds. */
    private static final long TIMEOUT = 1000;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(
            GridP2PRemoteTestTask.class.getName(),
            GridP2PRemoteTestTask1.class.getName(),
            JobA.class.getName(),
            JobB.class.getName(),
            JobB2.class.getName(),
            GridP2PRemoteMultiJVMClassLoadersSelfTest.class.getName()
        );

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestDifferentRemoteClassLoader(DeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite = startGrids(3);

            ClassLoader tstClsLdr1 = new GridTestClassLoader(
                    Collections.EMPTY_MAP, getClass().getClassLoader(),
                    GridP2PRemoteTestTask.class.getName(), JobA.class.getName());

            ClassLoader tstClsLdr2 = new GridTestClassLoader(
                    Collections.EMPTY_MAP, getClass().getClassLoader(),
                    GridP2PRemoteTestTask1.class.getName(), JobB.class.getName(), JobB2.class.getName());

            Class<? extends IgniteCallable> jobA = (Class<? extends IgniteCallable>) tstClsLdr1.loadClass(JobA.class
                    .getName());
            Class<? extends IgniteCallable> jobB = (Class<? extends IgniteCallable>) tstClsLdr2.loadClass(JobB.class
                    .getName());
            Class<? extends IgniteCallable> jobB2 = (Class<? extends IgniteCallable>) tstClsLdr2.loadClass(JobB2.class
                    .getName());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr1.loadClass(GridP2PRemoteTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> task2 =
                    (Class<? extends ComputeTask<?, ?>>) tstClsLdr2.loadClass(GridP2PRemoteTestTask.class.getName());

            ComputeTask compTaskA = task1.getDeclaredConstructor(Class.class, ClassLoader.class, Collection.class)
                    .newInstance(jobA, tstClsLdr1, ignite.cluster().nodes());
            ComputeTask compTaskB = task2.getDeclaredConstructor(Class.class, ClassLoader.class, Collection.class)
                    .newInstance(jobB, tstClsLdr2, ignite.cluster().nodes());
            ComputeTask compTaksB2 = task2.getDeclaredConstructor(Class.class, ClassLoader.class, Collection.class)
                    .newInstance(jobB2, tstClsLdr2, ignite.cluster().nodes());

            IgniteEx igniteRem1 = grid(1);
            IgniteProcessProxy proxy1 = (IgniteProcessProxy)igniteRem1;
            proxy1.remoteCompute().execute(compTaskA, null);

            IgniteEx igniteRem2 = grid(2);
            IgniteProcessProxy proxy2 = (IgniteProcessProxy)igniteRem2;
            proxy2.remoteCompute().execute(compTaskB, null);
            Object res = proxy2.remoteCompute().withTimeout(TIMEOUT).execute(compTaksB2, null);
            assert res == null;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testDifferentClassLoaderPrivateMode() throws Exception {
        processTestDifferentRemoteClassLoader(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testDifferentClassLoaderIsolatedMode() throws Exception {
        processTestDifferentRemoteClassLoader(DeploymentMode.SHARED);
    }

    /**
     * P2P test task.
     */
    public static class GridP2PRemoteTestTask extends ComputeTaskAdapter<Serializable, Object> implements
            GridPeerDeployAware {
        /** */
        private Class<? extends IgniteCallable> innerCallableClass;
        private Collection<ClusterNode> nodes;
        private ClassLoader cl;

        public GridP2PRemoteTestTask(Class<? extends IgniteCallable> innerCallableClass,
                                     ClassLoader cl, Collection<ClusterNode> nodes) {
            this.innerCallableClass = innerCallableClass;
            this.nodes = nodes;
            this.cl = cl;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Serializable arg) {
            Map<ComputeJob, ClusterNode> jobList = new HashMap<ComputeJob, ClusterNode>();
            try {
                Constructor<? extends IgniteCallable> constructor = innerCallableClass.getDeclaredConstructor();

                IgniteCallable igniteCallable = constructor.newInstance();
                for (ClusterNode clusterNode : nodes)
                    jobList.put(new CallableJobAdapter(igniteCallable), clusterNode);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return jobList;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }

        @Override public Class<?> deployClass() {
            return innerCallableClass;
        }

        @Override public ClassLoader classLoader() {
            return new IgniteLoader(innerCallableClass.getClassLoader(), cl);
        }
    }

    public static class CallableJobAdapter implements ComputeJob{
        IgniteCallable callable;

        public CallableJobAdapter(IgniteCallable callable) {
            this.callable = callable;
        }

        public void cancel() {
        }

        public Object execute() throws IgniteException {
            try {
                return callable.call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    /**
     * P2p test task.
     */
    public static class GridP2PRemoteTestTask1 extends GridP2PRemoteTestTask {
        public GridP2PRemoteTestTask1(Class<? extends IgniteCallable> innerCallableClass, ClassLoader cl,
                                      Collection<ClusterNode> nodes) {
            super(innerCallableClass, cl, nodes);
        }
        // No-op.
    }

    public static class JobA implements IgniteCallable<Integer> {
        public Integer call() throws Exception {
            System.out.println("I'm " + this.getClass().getName());
            return 42;
        }
    }

    public static class JobB implements IgniteCallable<Integer> {
        public Integer call() throws Exception {
            System.out.println("I'm " + this.getClass().getName());
            return 42;
        }
    }

    public static class JobB2 implements IgniteCallable<Integer> {
        public Integer call() throws Exception {
            System.out.println("I'm " + this.getClass().getName());
            return 42;
        }
    }

    public static class IgniteLoader extends ClassLoader {
        private final ClassLoader[] innerLoaders;

        public IgniteLoader(ClassLoader parentLoader, ClassLoader... innerLoaders) {
            super(parentLoader);
            this.innerLoaders = innerLoaders;
        }

        @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
            try {
                return super.findClass(name);
            } catch (ClassNotFoundException e) {
            }

            for (ClassLoader innerLoader : innerLoaders) {
                try {
                    return innerLoader.loadClass(name);
                } catch (ClassNotFoundException e) {
                }
            }

            throw new ClassNotFoundException(name);
        }
    }
}
