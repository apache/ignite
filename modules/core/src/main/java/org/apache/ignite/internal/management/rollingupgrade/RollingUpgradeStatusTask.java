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

package org.apache.ignite.internal.management.rollingupgrade;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;

/** Task to obtain rolling upgrade status. */
@GridInternal
public class RollingUpgradeStatusTask extends VisorOneNodeTask<NoArg, RollingUpgradeTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, RollingUpgradeTaskResult> job(NoArg arg) {
        return new RollingUpgradeStatusJob(arg, debug);
    }

    /** */
    private static class RollingUpgradeStatusJob extends VisorJob<NoArg, RollingUpgradeTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected RollingUpgradeStatusJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected RollingUpgradeTaskResult run(NoArg arg) throws IgniteException {
            IgnitePair<IgniteProductVersion> vers = ignite.context().rollingUpgrade().versions();

            List<RollingUpgradeStatusNode> nodes = null;

            if (vers != null && vers.get2() != null)
                nodes = ignite.context().discovery().allNodes().stream()
                    .map(node ->
                        new RollingUpgradeStatusNode(node.consistentId(),
                            F.first(node.addresses()),
                            IgniteProductVersion.fromString(node.attribute(ATTR_BUILD_VER))))
                    .collect(Collectors.toList());

            RollingUpgradeTaskResult res = new RollingUpgradeTaskResult(
                vers == null ? null : vers.get1(),
                vers == null ? null : vers.get2(),
                null,
                nodes
            );

            return res;
        }
    }
}
