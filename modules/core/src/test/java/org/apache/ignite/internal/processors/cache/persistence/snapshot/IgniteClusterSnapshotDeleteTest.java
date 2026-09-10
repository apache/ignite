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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
@RunWith(JUnit4.class)
public class IgniteClusterSnapshotDeleteTest extends AbstractSnapshotSelfTest {
    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        super.afterTestSnapshot();

        G.allGrids();
    }

    /** */
    @Test
    public void testNodeNotSupportingSnapshotDeleteFeature() throws Exception {
        // Creates empty feature set unsupporting the snapshot deletion if required.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "Test Ignite features provider";
            }

            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (!cls.equals(DiscoveryNodeValidationProcessor.class))
                    return null;

                boolean doNotSupport = ctx.igniteConfiguration().getIgniteInstanceName().equals(getTestIgniteInstanceName(1));

                return (T)new RollingUpgradeProcessor(
                    ((IgniteEx)ctx.grid()).context(),
                    doNotSupport ? new IgniteCoreFeatureSet(IgniteVersionUtils.VER, new IgniteFeatureSet()) : IgniteCoreFeatureSet.local()
                ) {
                    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode joiningNode) {
                        // Simulates started rolling updrade allowing node with other features join cluster.
                        return null;
                    }
                };
            }
        };

        startGridsMultiThreaded(3);

        assertThrowsAnyCause(
            null,
            () -> snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout()),
            IgniteIllegalStateException.class,
            "Node " + grid(1).localNode().id() + " doesn't support snapshot deletion"
        );
    }
}
