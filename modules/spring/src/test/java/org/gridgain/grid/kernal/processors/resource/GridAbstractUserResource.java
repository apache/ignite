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

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
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
    @IgniteLoggerResource
    private IgniteLogger log;

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
    @IgniteNameResource
    private String gridName;

    /** */
    @IgniteSpringApplicationContextResource
    private ApplicationContext springCtx;

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
    @IgniteUserResourceOnDeployed
    private void deploy() {
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
    @IgniteUserResourceOnUndeployed
    private void undeploy() {
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
