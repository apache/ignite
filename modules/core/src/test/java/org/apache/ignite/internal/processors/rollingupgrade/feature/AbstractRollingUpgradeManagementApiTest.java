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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;

/** */
public abstract class AbstractRollingUpgradeManagementApiTest extends AbstractRollingUpgradeTest {
    /** */
    protected <A, R> R executeCommandFromClient(
        int connNodeIdx,
        int jobNodeIdx,
        String cliVer,
        Class<? extends VisorMultiNodeTask<A, R, ?>> taskCls,
        A arg
    ) throws Exception {
        return runWithVersion(cliVer, () -> {
            try (IgniteClient cli = startClient(connNodeIdx)) {
                return CommandUtils.execute(cli, null, taskCls, arg, F.asList(grid(jobNodeIdx).localNode()));
            }
        });
    }

    /** */
    private static <R> R runWithVersion(String ver, Callable<R> action) throws Exception {
        IgniteCoreFeatureSet prev = IgniteCoreFeatureSet.INSTANCE;
        IgniteCoreFeatureSet.INSTANCE = createCoreFeatureSet(ver);

        try {
            return action.call();
        }
        finally {
            IgniteCoreFeatureSet.INSTANCE = prev;
        }
    }

    /** */
    private IgniteClient startClient(int connNodeIdx) {
        String addr = "127.0.0.1:" + grid(connNodeIdx).context().clientListener().port();

        return Ignition.startClient(new ClientConfiguration().setAddresses(addr).setClusterDiscoveryEnabled(false));
    }
}
