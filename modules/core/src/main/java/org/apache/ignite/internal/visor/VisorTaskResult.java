/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteNodeFeatureSet;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor.OP_FEATURES_ATTR;

/**
 * Management task result.
 */
public class VisorTaskResult<R> extends IgniteDataTransferObject {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** */
    private transient IgniteNodeFeatureSet cmdInitiatorFeatures;

    /** Task result. */
    @Order(0)
    @Nullable R res;

    /** Error. */
    @Order(1)
    @Nullable Exception err;

    /** */
    public VisorTaskResult() {
        // No-op.
    }

    /**
     * @param res Task result.
     * @param err Error.
     * @param cmdInitiatorFeatures Feature set of the command initiator.
     */
    public VisorTaskResult(@Nullable R res, @Nullable Exception err, IgniteNodeFeatureSet cmdInitiatorFeatures) {
        assert cmdInitiatorFeatures != null;

        this.res = res;
        this.err = err;
        this.cmdInitiatorFeatures = cmdInitiatorFeatures;
    }

    /** {@inheritDoc} */
    @Override protected void writeIgniteDataTransferObject(ObjectOutput out) throws IOException {
        try (Scope ignored = OperationContext.set(OP_FEATURES_ATTR, cmdInitiatorFeatures)) {
            super.writeIgniteDataTransferObject(out);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readIgniteDataTransferObject(ObjectInput in) throws IOException, ClassNotFoundException {
        try (Scope ignored = OperationContext.set(OP_FEATURES_ATTR, new IgniteNodeFeatureSet(IgniteCoreFeatureSet.local()))) {
            super.readIgniteDataTransferObject(in);
        }
    }

    /**
     * @return Task result.
     * @throws Exception if the task was completed with an error.
     */
    public @Nullable R result() throws Exception {
        if (err != null)
            throw err;

        return res;
    }
}
