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

package org.apache.ignite.internal.visor.encryption;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * View/change cache group re-encryption rate limit .
 */
@GridInternal
public class VisorReencryptionRateTask extends VisorMultiNodeTask<VisorReencryptionRateTaskArg,
    VisorCacheGroupEncryptionTaskResult<Double>, VisorReencryptionRateTask.ReencryptionRateJobResult>
{
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorReencryptionRateTaskArg, ReencryptionRateJobResult> job(
        VisorReencryptionRateTaskArg arg) {
        return new VisorReencryptionRateJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorCacheGroupEncryptionTaskResult<Double> reduce0(List<ComputeJobResult> results) {
        Map<UUID, Double> jobResults = new HashMap<>();
        Map<UUID, IgniteException> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            UUID nodeId = res.getNode().id();

            if (res.getException() != null) {
                exceptions.put(nodeId, res.getException());

                continue;
            }

            ReencryptionRateJobResult dtoRes = res.getData();

            jobResults.put(nodeId, dtoRes.limit());
        }

        return new VisorCacheGroupEncryptionTaskResult<>(jobResults, exceptions);
    }

    /** The job for view/change cache group re-encryption rate limit. */
    private static class VisorReencryptionRateJob
        extends VisorJob<VisorReencryptionRateTaskArg, ReencryptionRateJobResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorReencryptionRateJob(VisorReencryptionRateTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ReencryptionRateJobResult run(VisorReencryptionRateTaskArg arg) throws IgniteException {
            double prevRate = ignite.context().encryption().getReencryptionRate();

            if (arg.rate() != null)
                ignite.context().encryption().setReencryptionRate(arg.rate());

            return new ReencryptionRateJobResult(prevRate);
        }
    }

    /** */
    protected static class ReencryptionRateJobResult extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Re-encryption rate limit. */
        private Double limit;

        /** */
        public ReencryptionRateJobResult() {
            // No-op.
        }

        /** */
        public ReencryptionRateJobResult(Double limit) {
            this.limit = limit;
        }

        /**
         * @return Re-encryption rate limit.
         */
        public Double limit() {
            return limit;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            out.writeDouble(limit);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
            limit = in.readDouble();
        }
    }
}
