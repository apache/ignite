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

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.resources.*;

/**
 * User resource, that increases node-local counters
 * on deploy and undeploy.
 */
public class GridP2PAwareTestUserResource {
    /** Deploy counter key. */
    private static final String DEPLOY_CNT_KEY = "deployCnt";

    /** Undeploy counter key. */
    private static final String UNDEPLOY_CNT_KEY = "undeployCnt";

    /** Grid instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /**
     * Invoked on resource deploy. Increments deploy counter
     * in node-local store.
     */
    @SuppressWarnings("ConstantConditions")
    @IgniteUserResourceOnDeployed
    public void onDeployed() {
        concurrentIncrement(DEPLOY_CNT_KEY);
    }

    /**
     * Invoked on resource undeploy. Increments undeploy counter
     * in node-local store.
     */
    @SuppressWarnings("ConstantConditions")
    @IgniteUserResourceOnUndeployed
    public void onUndeployed() {
        concurrentIncrement(UNDEPLOY_CNT_KEY);
    }

    /**
     * Concurrently increments numeric cache value.
     *
     * @param key Key for the value to be incremented.
     */
    private <T> void concurrentIncrement(T key) {
        ClusterNodeLocalMap<T, Integer> nodeLoc = ignite.cluster().nodeLocalMap();

        Integer cntr = nodeLoc.get(key);

        if (cntr == null)
            cntr = nodeLoc.putIfAbsent(key, 1);

        if (cntr != null) {
            while (!nodeLoc.replace(key, cntr, cntr + 1)) {
                cntr = nodeLoc.get(key);

                assert cntr != null;
            }
        }
    }
}
