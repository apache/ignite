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

import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteNodeFeatureSet;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor.OP_FEATURES_ATTR;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.C;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.D;
import static org.junit.Assert.assertEquals;

/** */
public class TestCommandTask extends VisorOneNodeTask<TestCommandArgument, TestCommandResponse> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private transient IgniteNodeFeatureSet cmdInitiatorFeatures;

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(
        List<ClusterNode> subgrid,
        VisorTaskArgument<TestCommandArgument> arg
    ) {
        cmdInitiatorFeatures = arg.initiatorFeatures();

        assertEquals(cmdInitiatorFeatures, OperationContext.get(OP_FEATURES_ATTR));

        return super.map0(subgrid, arg);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        assertEquals(cmdInitiatorFeatures, OperationContext.get(OP_FEATURES_ATTR));

        return super.result(res, rcvd);
    }

    /** {@inheritDoc} */
    @Override protected CommandJob job(TestCommandArgument arg) {
        return new CommandJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected TestCommandResponse reduce0(List<ComputeJobResult> results) {
        TestCommandResponse res = super.reduce0(results);

        res.taskFeatures = OperationContext.get(OP_FEATURES_ATTR);

        return res;
    }

    /** */
    public static class CommandJob extends VisorJob<TestCommandArgument, TestCommandResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected CommandJob(TestCommandArgument arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected TestCommandResponse run(TestCommandArgument arg) {
            return new TestCommandResponse(initiatorFeatures(), arg, C, D);
        }
    }
}
