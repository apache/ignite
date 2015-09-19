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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class IgniteExplicitImplicitDeploymentSelfTest extends GridCommonAbstractTest {
    /** */
    public IgniteExplicitImplicitDeploymentSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridDeploymentResourceTestTask.class.getName(),
            GridDeploymentResourceTestJob.class.getName());

        cfg.setDeploymentMode(DeploymentMode.ISOLATED);

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testImplicitDeployLocally() throws Exception {
        execImplicitDeployLocally(true, true, true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testImplicitDeployP2P() throws Exception {
        execImplicitDeployP2P(true, true, true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testExplicitDeployLocally() throws Exception {
        execExplicitDeployLocally(true, true, true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testExplicitDeployP2P() throws Exception {
        execExplicitDeployP2P(true, true, true);
    }

    /**
     * @param ignite Grid.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void stopGrid(Ignite ignite) {
        try {
            if (ignite != null)
                G.stop(ignite.name(), true);
        }
        catch (Throwable e) {
            error("Got error when stopping grid.", e);
        }
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    private void execExplicitDeployLocally(boolean byCls, boolean byTask, boolean byName) throws Exception {
        Ignite ignite = null;

        try {
            ignite = startGrid();

            // Explicit Deployment. Task execution should return 0.
            // Say resource class loader - different to task one.
            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader());

            // Assume that users task and job were loaded with this class loader
            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            info("Loader1: " + ldr1);
            info("Loader2: " + ldr2);

            Class<? extends ComputeTask<String, Integer>> taskCls = (Class<? extends ComputeTask<String, Integer>>)
                ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

            // Check auto-deploy. It should pick up resource class loader.
            if (byCls) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                Integer res = ignite.compute().execute(taskCls, null);

                assert res != null;
                assert res == 2 : "Invalid response: " + res;
            }

            if (byTask) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                Integer res = ignite.compute().execute(taskCls.newInstance(), null);

                assert res != null;
                assert res == 2 : "Invalid response: " + res;
            }

            if (byName) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                Integer res = (Integer) ignite.compute().execute(taskCls.getName(), null);

                assert res != null;
                assert res == 1 : "Invalid response: " + res;
            }
        }
        finally {
            stopGrid(ignite);
        }
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
   @SuppressWarnings("unchecked")
   private void execImplicitDeployLocally(boolean byCls, boolean byTask, boolean byName) throws Exception {
       Ignite ignite = null;

       try {
           ignite = startGrid();

           // First task class loader.
           ClassLoader ldr1 = new GridTestClassLoader(
               Collections.singletonMap("testResource", "1"),
               getClass().getClassLoader(),
               GridDeploymentResourceTestTask.class.getName(),
               GridDeploymentResourceTestJob.class.getName()
           );

           // Second task class loader
           ClassLoader ldr2 = new GridTestClassLoader(
               Collections.singletonMap("testResource", "2"),
               getClass().getClassLoader(),
               GridDeploymentResourceTestTask.class.getName(),
               GridDeploymentResourceTestJob.class.getName()
           );

           // The same name but different classes/ class loaders.
           Class<? extends ComputeTask<String, Integer>> taskCls1 = (Class<? extends ComputeTask<String, Integer>>)
               ldr1.loadClass(GridDeploymentResourceTestTask.class.getName());

           Class<? extends ComputeTask<String, Integer>> taskCls2 = (Class<? extends ComputeTask<String, Integer>>)
               ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

           if (byCls) {
               Integer res1 = ignite.compute().execute(taskCls1, null);
               Integer res2 = ignite.compute().execute(taskCls2, null);

               assert res1 != null;
               assert res2 != null;

               assert res1 == 1 : "Invalid res1: " + res1;
               assert res2 == 2 : "Invalid res2: " + res2;
           }

           if (byTask) {
               Integer res1 = ignite.compute().execute(taskCls1.newInstance(), null);
               Integer res2 = ignite.compute().execute(taskCls2.newInstance(), null);

               assert res1 != null;
               assert res2 != null;

               assert res1 == 1 : "Invalid res1: " + res1;
               assert res2 == 2 : "Invalid res2: " + res2;
           }

           if (byName) {
               ignite.compute().localDeployTask(taskCls1, ldr1);

               Integer res1 = (Integer) ignite.compute().execute(taskCls1.getName(), null);

               ignite.compute().localDeployTask(taskCls2, ldr2);

               Integer res2 = (Integer) ignite.compute().execute(taskCls2.getName(), null);

               assert res1 != null;
               assert res2 != null;

               assert res1 == 1 : "Invalid res1: " + res1;
               assert res2 == 2 : "Invalid res2: " + res2;
           }
       }
       finally {
           stopGrid(ignite);
       }
   }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    private void execExplicitDeployP2P(boolean byCls, boolean byTask, boolean byName) throws Exception {
       Ignite ignite1 = null;
       Ignite ignite2 = null;

       try {
           ignite1 = startGrid(1);
           ignite2 = startGrid(2);

           ClassLoader ldr1 = new GridTestClassLoader(
               Collections.singletonMap("testResource", "1"),
               getClass().getClassLoader(),
               GridDeploymentResourceTestTask.class.getName(),
               GridDeploymentResourceTestJob.class.getName()
           );

           ClassLoader ldr2 = new GridTestClassLoader(
               Collections.singletonMap("testResource", "2"),
               getClass().getClassLoader(),
               GridDeploymentResourceTestTask.class.getName(),
               GridDeploymentResourceTestJob.class.getName()
           );

           Class<? extends ComputeTask<String, Integer>> taskCls = (Class<? extends ComputeTask<String, Integer>>)
               ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

           if (byCls) {
               ignite1.compute().localDeployTask(taskCls, ldr1);

               // Even though the task is deployed with resource class loader,
               // when we execute it, it will be redeployed with task class-loader.
               Integer res = ignite1.compute().execute(taskCls, null);

               assert res != null;
               assert res == 2 : "Invalid response: " + res;
           }


           if (byTask) {
               ignite1.compute().localDeployTask(taskCls, ldr1);

               // Even though the task is deployed with resource class loader,
               // when we execute it, it will be redeployed with task class-loader.
               Integer res = ignite1.compute().execute(taskCls.newInstance(), null);

               assert res != null;
               assert res == 2 : "Invalid response: " + res;
           }

           if (byName) {
               ignite1.compute().localDeployTask(taskCls, ldr1);

               // Even though the task is deployed with resource class loader,
               // when we execute it, it will be redeployed with task class-loader.
               Integer res = (Integer) ignite1.compute().execute(taskCls.getName(), null);

               assert res != null;
               assert res == 1 : "Invalid response: " + res;
           }
       }
       finally {
           stopGrid(ignite2);
           stopGrid(ignite1);
       }
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
   @SuppressWarnings("unchecked")
   private void execImplicitDeployP2P(boolean byCls, boolean byTask, boolean byName) throws Exception {
      Ignite ignite1 = null;
      Ignite ignite2 = null;

      try {
          ignite1 = startGrid(1);
          ignite2 = startGrid(2);

          ClassLoader ldr1 = new GridTestClassLoader(
              Collections.singletonMap("testResource", "1"),
              getClass().getClassLoader(),
              GridDeploymentResourceTestTask.class.getName(),
              GridDeploymentResourceTestJob.class.getName()
          );

          ClassLoader ldr2 = new GridTestClassLoader(
              Collections.singletonMap("testResource", "2"),
              getClass().getClassLoader(),
              GridDeploymentResourceTestTask.class.getName(),
              GridDeploymentResourceTestJob.class.getName()
          );

          Class<? extends ComputeTask<String, Integer>> taskCls1 = (Class<? extends ComputeTask<String, Integer>>)
              ldr1.loadClass(GridDeploymentResourceTestTask.class.getName());

          Class<? extends ComputeTask<String, Integer>> taskCls2 = (Class<? extends ComputeTask<String, Integer>>)
              ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

          if (byCls) {
              Integer res1 = ignite1.compute().execute(taskCls1, null);
              Integer res2 = ignite1.compute().execute(taskCls2, null);

              assert res1 != null;
              assert res2 != null;

              assert res1 == 1 : "Invalid res1: " + res1;
              assert res2 == 2 : "Invalid res2: " + res2;
          }

          if (byTask) {
              Integer res1 = ignite1.compute().execute(taskCls1.newInstance(), null);
              Integer res2 = ignite1.compute().execute(taskCls2.newInstance(), null);

              assert res1 != null;
              assert res2 != null;

              assert res1 == 1 : "Invalid res1: " + res1;
              assert res2 == 2 : "Invalid res2: " + res2;
          }

          if (byName) {
              ignite1.compute().localDeployTask(taskCls1, ldr1);

              Integer res1 = (Integer) ignite1.compute().execute(taskCls1.getName(), null);

              ignite1.compute().localDeployTask(taskCls2, ldr2);

              Integer res2 = (Integer) ignite1.compute().execute(taskCls2.getName(), null);

              assert res1 != null;
              assert res2 != null;

              assert res1 == 1 : "Invalid res1: " + res1;
              assert res2 == 2 : "Invalid res2: " + res2;
          }
      }
      finally {
          stopGrid(ignite1);
          stopGrid(ignite2);
      }
   }

    /**
     * We use custom name to avoid auto-deployment in the same VM.
     */
    @SuppressWarnings({"PublicInnerClass"})
    @ComputeTaskName("GridDeploymentResourceTestTask")
    public static class GridDeploymentResourceTestTask extends ComputeTaskAdapter<String, Integer> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            boolean ignoreLocNode = false;

            UUID locId = ignite.configuration().getNodeId();

            if (subgrid.size() == 1)
                assert subgrid.get(0).id().equals(locId) : "Wrong node id.";
            else
                ignoreLocNode = true;

            for (ClusterNode node : subgrid) {
                // Ignore local node.
                if (ignoreLocNode && node.id().equals(locId))
                    continue;

                map.put(new GridDeploymentResourceTestJob(), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /**
     * Simple job for this test.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class GridDeploymentResourceTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            if (log.isInfoEnabled())
                log.info("Executing grid job: " + this);

            try {
                ClassLoader ldr = Thread.currentThread().getContextClassLoader();

                if (log.isInfoEnabled())
                    log.info("Loader (inside job): " + ldr);

                InputStream in = ldr.getResourceAsStream("testResource");

                if (in != null) {
                    Reader reader = new InputStreamReader(in);

                    try {
                        char res = (char)reader.read();

                        return Integer.parseInt(Character.toString(res));
                    }
                    finally {
                        U.close(in, null);
                    }
                }

                return null;
            }
            catch (IOException e) {
                throw new IgniteException("Failed to execute job.", e);
            }
        }
    }
}