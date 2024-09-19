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
package org.apache.ignite.internal.processors.cache.distributed;

import java.util.LinkedList;
import java.util.List;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTask;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskResult;
import org.apache.ignite.internal.management.cache.CacheDistributionCommandArg;
import org.apache.ignite.internal.processors.cache.persistence.CleanCacheStoresMaintenanceAction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/** */
public class CacheWithNodeFilterTest extends GridCommonAbstractTest {
    /** */
    public static final int NODES = 2;

    /** */
    @Test
    public void test() throws Exception {
        LinkedList<Ignite> nodes = new LinkedList<>();

        for (int i = 0; i < NODES; i++)
            nodes.add(Ignition.start(getConfiguration("server_" + i)));

        nodes.getFirst().cluster().state(ClusterState.ACTIVE);

        Ignite client = Ignition.start(getConfiguration("client"));

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setNodeFilter(node -> node.consistentId().toString().endsWith("0"))
            .setStatisticsEnabled(true));

        CacheDistributionCommandArg arg = new CacheDistributionCommandArg();
        arg.caches(new String[] {DEFAULT_CACHE_NAME});

        VisorTaskArgument<CacheDistributionCommandArg> vis =
            new VisorTaskArgument<>(F.nodeIds(client.cluster().forServers().nodes()), arg, false);

        client.compute().execute(CacheDistributionTask.class, vis);
        VisorTaskResult<CacheDistributionTaskResult> res = client.compute().execute(CacheDistributionTask.class, vis);
        CacheDistributionTaskResult result = res.result();

        assertEquals(0, result.exceptions().size());
    }

    /** */
    @After
    public void cleanup() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
